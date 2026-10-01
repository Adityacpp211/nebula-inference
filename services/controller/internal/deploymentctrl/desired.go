// Package deploymentctrl reconciles NEBULA deployments into Kubernetes objects.
//
// Desired state lives in PostgreSQL; this package turns one deployment row into
// the objects that should exist (desired.go, pure), compares what Kubernetes
// reports with what the row wants to decide the next lifecycle state (decide.go,
// pure), and applies both (reconciler.go). Keeping the first two pure is what lets
// every case — mock versus llama.cpp, stop, scale to zero, a failed artifact pull —
// be tested without a cluster.
//
// Objects per deployment, all labelled app.kubernetes.io/managed-by=nebula and
// owned by one root ConfigMap, so deleting the root garbage-collects the set
// (docs/deployment-architecture.md §2.2):
//
//	ConfigMap  <name>-root    identity, spec hash, artifact coordinates
//	Deployment <name>         worker pods: artifact-puller init container + worker
//	Service    <name>         ClusterIP, the address routes point at
//	PDB        <name>         only when replicas > 1
package deploymentctrl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/packages/scheduler"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Settings are the controller-wide inputs to object construction.
type Settings struct {
	Namespace          string
	Env                string
	WorkerImage        string
	MockWorkerImage    string
	PullerImage        string
	ImagePullPolicy    corev1.PullPolicy
	CacheHostPath      string
	ArtifactSecretName string
	StartingTimeoutSec int32
	MaxArtifactBytes   int64
	// NATSURL is where workers publish heartbeats; empty for none.
	NATSURL string
}

// Ports and paths inside a worker pod.
const (
	WorkerPort      = 8090
	modelsMount     = "/models"
	containerWorker = "worker"
	containerPuller = "artifact-puller"
)

// Objects is the desired set for one deployment.
type Objects struct {
	Root       *corev1.ConfigMap
	Deployment *appsv1.Deployment
	Service    *corev1.Service
	// PDB is nil when the deployment runs fewer than two replicas: a PDB with
	// minAvailable 1 on a single replica would block every node drain.
	PDB *policyv1.PodDisruptionBudget
	// SpecHash identifies the pod template; a change rolls the pods.
	SpecHash string
}

// Replicas is how many pods the deployment should run in its current state.
func Replicas(d *store.Deployment) int32 {
	switch d.State {
	case lifecycle.Stopping, lifecycle.Stopped:
		return 0
	}
	return d.DesiredReplicas
}

// Build constructs the desired objects. Owner references to the root are filled
// in by the reconciler once the root exists and has a UID.
func Build(d *store.Deployment, s Settings) (*Objects, error) {
	name := k8s.ObjectName(d.OrgSlug, d.Name)
	labels := map[string]string{
		k8s.LabelName:         k8s.WorkerName,
		k8s.LabelManagedBy:    k8s.ManagedBy,
		k8s.LabelOrg:          truncLabel(d.OrgSlug),
		k8s.LabelOrgID:        d.OrgID.String(),
		k8s.LabelDeployment:   truncLabel(d.Name),
		k8s.LabelDeploymentID: d.ID.String(),
		k8s.LabelVersionID:    d.Version.ID.String(),
		k8s.LabelRuntime:      string(d.Version.Runtime),
		k8s.LabelRevision:     strconv.Itoa(int(d.CurrentRevision)),
	}
	selector := map[string]string{k8s.LabelDeploymentID: d.ID.String()}

	profile, err := scheduler.ParseProfile(d.Version.HardwareProfile)
	if err != nil {
		return nil, err
	}
	res, err := scheduler.ParseResources(d.Resources)
	if err != nil {
		return nil, err
	}
	constraints := scheduler.Derive(scheduler.Request{Profile: profile, Resources: res, Replicas: int(d.DesiredReplicas)})

	pod, err := podSpec(d, s, constraints, selector)
	if err != nil {
		return nil, err
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: copyMap(labels)},
		Spec:       pod,
	}
	// The hash covers the pod template only, so a replica change does not roll
	// pods and a spec change always does.
	raw, _ := json.Marshal(template)
	sum := sha256.Sum256(raw)
	specHash := "sha256:" + hex.EncodeToString(sum[:])[:16]
	template.Annotations = map[string]string{k8s.AnnotationSpecHash: specHash}

	annotations := map[string]string{
		k8s.AnnotationSpecHash:   specHash,
		k8s.AnnotationGeneration: strconv.FormatInt(d.Generation, 10),
	}
	meta := func(n string, extra map[string]string) metav1.ObjectMeta {
		l := copyMap(labels)
		for k, v := range extra {
			l[k] = v
		}
		return metav1.ObjectMeta{Name: n, Namespace: s.Namespace, Labels: l, Annotations: copyMap(annotations)}
	}

	replicas := Replicas(d)
	zero := intstr.FromInt32(0)
	one := intstr.FromInt32(1)
	objs := &Objects{
		SpecHash: specHash,
		Root: &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: meta(k8s.RootName(name), map[string]string{k8s.LabelComponent: k8s.ComponentRoot}),
			Data: map[string]string{
				"deployment_id":   d.ID.String(),
				"org":             d.OrgSlug,
				"deployment":      d.Name,
				"model_version":   d.Version.Ref(),
				"artifact_uri":    d.Version.ArtifactURI,
				"artifact_sha256": d.Version.ChecksumHex,
				"generation":      strconv.FormatInt(d.Generation, 10),
				"spec_hash":       specHash,
			},
		},
		Deployment: &appsv1.Deployment{
			TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
			ObjectMeta: meta(name, nil),
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: selector},
				Strategy: appsv1.DeploymentStrategy{
					Type: appsv1.RollingUpdateDeploymentStrategyType,
					// maxUnavailable 0: a new version's pods must be ready before an old
					// one is removed, so a version that cannot load never takes capacity
					// away (Phase 11 builds rollback on this).
					RollingUpdate: &appsv1.RollingUpdateDeployment{MaxUnavailable: &zero, MaxSurge: &one},
				},
				ProgressDeadlineSeconds: &s.StartingTimeoutSec,
				RevisionHistoryLimit:    ptr(int32(3)),
				Template:                template,
			},
		},
		Service: &corev1.Service{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
			ObjectMeta: meta(name, nil),
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeClusterIP,
				Selector: selector,
				Ports: []corev1.ServicePort{{
					Name: "http", Port: WorkerPort, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP,
				}},
			},
		},
	}
	if replicas > 1 {
		objs.PDB = &policyv1.PodDisruptionBudget{
			TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
			ObjectMeta: meta(name, nil),
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &one,
				Selector:     &metav1.LabelSelector{MatchLabels: selector},
			},
		}
	}
	return objs, nil
}

// needsArtifact reports whether the worker loads weights from disk. The mock
// runtime is a declared stub that generates placeholder tokens from nothing, so it
// gets no puller and no model mount.
func needsArtifact(d *store.Deployment) bool { return d.Version.Runtime != models.RuntimeMock }

func podSpec(d *store.Deployment, s Settings, c scheduler.Constraints, selector map[string]string) (corev1.PodSpec, error) {
	requests, err := quantities(c.Requests)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	limits, err := quantities(c.Limits)
	if err != nil {
		return corev1.PodSpec{}, err
	}

	image := s.WorkerImage
	if d.Version.Runtime == models.RuntimeMock {
		image = s.MockWorkerImage
	}

	modelPath := modelsMount + "/sha256/" + d.Version.ChecksumHex
	env := []corev1.EnvVar{
		{Name: "NEBULA_ENV", Value: s.Env},
		{Name: "NEBULA_LOG_FORMAT", Value: "json"},
		{Name: "NEBULA_WORKER_HOST", Value: "0.0.0.0"},
		{Name: "NEBULA_WORKER_PORT", Value: strconv.Itoa(WorkerPort)},
		{Name: "NEBULA_WORKER_RUNTIME", Value: string(d.Version.Runtime)},
		{Name: "NEBULA_WORKER_MODEL_VERSION", Value: d.Version.Ref()},
		{Name: "NEBULA_WORKER_MODEL_FORMAT", Value: string(d.Version.Format)},
		{Name: "NEBULA_WORKER_CONTEXT_WINDOW", Value: strconv.Itoa(int(d.Version.ContextWindow))},
		// Every request reaching a worker in a cluster comes from the gateway, which
		// always sends the full header set; a request without one is refused.
		{Name: "NEBULA_WORKER_STRICT_HEADERS", Value: "true"},
		{Name: "NEBULA_WORKER_DRAIN_DELAY_S", Value: "5"},
		// Heartbeat identity: the router matches a heartbeat to the EndpointSlice
		// entry by pod name (docs/events.md §3.1).
		{Name: "NEBULA_WORKER_POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "NEBULA_WORKER_NODE_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
		{Name: "NEBULA_WORKER_DEPLOYMENT_ID", Value: d.ID.String()},
		{Name: "NEBULA_WORKER_MODEL_VERSION_ID", Value: d.Version.ID.String()},
	}
	if needsArtifact(d) {
		env = append(env,
			corev1.EnvVar{Name: "NEBULA_WORKER_MODEL_PATH", Value: modelPath},
			corev1.EnvVar{Name: "NEBULA_WORKER_MODEL_SHA256", Value: d.Version.ChecksumHex},
		)
	}
	runtimeConfig, err := mergeObjects(d.Version.RuntimeConfig, d.RuntimeOverrides)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	if s.NATSURL != "" {
		env = append(env, corev1.EnvVar{Name: "NEBULA_NATS_URL", Value: s.NATSURL})
	}
	if runtimeConfig != "{}" {
		env = append(env, corev1.EnvVar{Name: "NEBULA_WORKER_RUNTIME_CONFIG", Value: runtimeConfig})
	}
	var queue struct {
		MaxDepth         *int `json:"max_depth"`
		DefaultTimeoutMS *int `json:"default_timeout_ms"`
	}
	_ = json.Unmarshal(d.QueueConfig, &queue)
	if queue.MaxDepth != nil {
		env = append(env, corev1.EnvVar{Name: "NEBULA_WORKER_MAX_QUEUE_DEPTH", Value: strconv.Itoa(*queue.MaxDepth)})
	}
	if queue.DefaultTimeoutMS != nil {
		env = append(env, corev1.EnvVar{Name: "NEBULA_WORKER_DEFAULT_TIMEOUT_S",
			Value: strconv.FormatFloat(float64(*queue.DefaultTimeoutMS)/1000, 'f', 3, 64)})
	}

	// Containers run as non-root with nothing writable but their scratch volume.
	sc := &corev1.SecurityContext{
		RunAsNonRoot:             ptr(true),
		AllowPrivilegeEscalation: ptr(false),
		ReadOnlyRootFilesystem:   ptr(true),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	httpGet := func(path string) corev1.ProbeHandler {
		return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("http")}}
	}

	worker := corev1.Container{
		Name:            containerWorker,
		Image:           image,
		ImagePullPolicy: s.ImagePullPolicy,
		Env:             env,
		Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: WorkerPort, Protocol: corev1.ProtocolTCP}},
		Resources:       corev1.ResourceRequirements{Requests: requests, Limits: limits},
		// Three probes, three questions (docs/deployment-architecture.md §2.2): the
		// startup probe tolerates a slow model load, readiness controls traffic, and
		// liveness only asks whether the process is wedged — never about the model.
		StartupProbe:   &corev1.Probe{ProbeHandler: httpGet("/readyz"), PeriodSeconds: 5, FailureThreshold: 60, TimeoutSeconds: 3},
		ReadinessProbe: &corev1.Probe{ProbeHandler: httpGet("/readyz"), PeriodSeconds: 5, FailureThreshold: 3, TimeoutSeconds: 3},
		LivenessProbe:  &corev1.Probe{ProbeHandler: httpGet("/livez"), PeriodSeconds: 10, FailureThreshold: 3, TimeoutSeconds: 3},
		Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{
			// Stop accepting work, then give EndpointSlice propagation time before
			// SIGTERM, so in-flight requests finish instead of being cut.
			"python", "-c",
			"import urllib.request,time;" +
				"urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:" + strconv.Itoa(WorkerPort) +
				"/internal/v1/drain',method='POST'),timeout=2);time.sleep(5)",
		}}}},
		SecurityContext:          sc,
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		VolumeMounts:             []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
	}

	spec := corev1.PodSpec{
		// The worker holds no credentials and talks to nothing that needs one
		// (docs/security-boundaries.md §2, B3), so it gets no API token either.
		AutomountServiceAccountToken:  ptr(false),
		EnableServiceLinks:            ptr(false),
		TerminationGracePeriodSeconds: ptr(int64(60)),
		NodeSelector:                  c.NodeSelector,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: ptr(true), RunAsUser: ptr(int64(65532)), RunAsGroup: ptr(int64(65532)),
			FSGroup:        ptr(int64(65532)),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Volumes: []corev1.Volume{{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}
	if c.RuntimeClassName != "" {
		spec.RuntimeClassName = &c.RuntimeClassName
	}
	for _, t := range c.Tolerations {
		spec.Tolerations = append(spec.Tolerations, corev1.Toleration{
			Key: t.Key, Operator: corev1.TolerationOperator(t.Operator), Effect: corev1.TaintEffect(t.Effect),
		})
	}
	if c.SpreadByHostname {
		spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: "kubernetes.io/hostname",
			// Prefer spread, never block a single-node cluster.
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: selector},
		}}
	}

	if needsArtifact(d) {
		hostPathType := corev1.HostPathDirectoryOrCreate
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "models", VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: s.CacheHostPath, Type: &hostPathType},
		}})
		worker.VolumeMounts = append(worker.VolumeMounts, corev1.VolumeMount{Name: "models", MountPath: modelsMount, ReadOnly: true})

		scheme, _, _, err := artifact.ParseURI(d.Version.ArtifactURI)
		if err != nil {
			return corev1.PodSpec{}, fmt.Errorf("model version %s: %w", d.Version.Ref(), err)
		}
		puller := corev1.Container{
			Name:            containerPuller,
			Image:           s.PullerImage,
			ImagePullPolicy: s.ImagePullPolicy,
			Env: []corev1.EnvVar{
				{Name: "NEBULA_ARTIFACT_URI", Value: d.Version.ArtifactURI},
				{Name: "NEBULA_ARTIFACT_SHA256", Value: d.Version.ChecksumHex},
				{Name: "NEBULA_ARTIFACT_SIZE_BYTES", Value: strconv.FormatInt(d.Version.SizeBytes, 10)},
				{Name: "NEBULA_ARTIFACT_MAX_BYTES", Value: strconv.FormatInt(s.MaxArtifactBytes, 10)},
				{Name: "NEBULA_ARTIFACT_CACHE_DIR", Value: modelsMount},
			},
			// The puller writes its FetchResult as the termination message, so a
			// checksum failure surfaces in the pod status as a named reason that the
			// controller turns into a deployment condition.
			TerminationMessagePath:   corev1.TerminationMessagePathDefault,
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
			},
			SecurityContext: sc,
			VolumeMounts:    []corev1.VolumeMount{{Name: "models", MountPath: modelsMount}, {Name: "tmp", MountPath: "/tmp"}},
		}
		if scheme == "s3" {
			// Credentials by reference: the controller never reads the Secret, and has
			// no permission to (docs/deployment-architecture.md §3).
			puller.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: s.ArtifactSecretName},
			}}}
		}
		spec.InitContainers = []corev1.Container{puller}
	}
	spec.Containers = []corev1.Container{worker}
	return spec, nil
}

func quantities(in map[string]string) (corev1.ResourceList, error) {
	out := corev1.ResourceList{}
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		q, err := resource.ParseQuantity(in[k])
		if err != nil {
			return nil, fmt.Errorf("resource %s=%q: %w", k, in[k], err)
		}
		out[corev1.ResourceName(k)] = q
	}
	return out, nil
}

// mergeObjects overlays the deployment's runtime_overrides on the version's
// runtime_config, returning compact JSON.
func mergeObjects(base, overlay json.RawMessage) (string, error) {
	m := map[string]any{}
	for _, raw := range []json.RawMessage{base, overlay} {
		if len(raw) == 0 {
			continue
		}
		var part map[string]any
		if err := json.Unmarshal(raw, &part); err != nil {
			return "", fmt.Errorf("runtime config: %w", err)
		}
		for k, v := range part {
			m[k] = v
		}
	}
	b, err := json.Marshal(m)
	return string(b), err
}

func truncLabel(v string) string {
	v = strings.ToLower(v)
	if len(v) > 63 {
		v = strings.TrimRight(v[:63], "-_.")
	}
	return v
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func ptr[T any](v T) *T { return &v }
