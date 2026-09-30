package deploymentctrl_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/deploymentctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

const ns = "nebula-workloads"

// memStore is an in-memory store that enforces the lifecycle graph the way the
// database trigger does, and records every transition.
type memStore struct {
	mu          sync.Mutex
	rows        map[uuid.UUID]*store.Deployment
	transitions []string
	statuses    []store.Status
}

func (m *memStore) Get(_ context.Context, id uuid.UUID) (*store.Deployment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.rows[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (m *memStore) Transition(_ context.Context, id uuid.UUID, from, to models.DeploymentState, reason, _ string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.rows[id]
	if d == nil || d.State != from {
		return false, nil
	}
	if !lifecycle.CanTransition(from, to) {
		panic("illegal transition " + string(from) + " -> " + string(to)) // the database would refuse it
	}
	d.State = to
	d.StateEnteredAt = time.Now()
	m.transitions = append(m.transitions, string(from)+">"+string(to)+":"+reason)
	return true, nil
}

func (m *memStore) WriteStatus(_ context.Context, id uuid.UUID, st store.Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d := m.rows[id]; d != nil {
		d.ObservedGeneration = max(d.ObservedGeneration, st.ObservedGeneration)
		d.ReadyReplicas, d.UpdatedReplicas, d.Conditions = st.ReadyReplicas, st.UpdatedReplicas, st.Conditions
	}
	m.statuses = append(m.statuses, st)
	return nil
}

func (m *memStore) state(id uuid.UUID) models.DeploymentState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows[id].State
}

func settings() deploymentctrl.Settings {
	return deploymentctrl.Settings{
		Namespace: ns, Env: "dev", WorkerImage: "nebula/worker-llamacpp:t", MockWorkerImage: "nebula/worker-mock:t",
		PullerImage: "nebula/artifact-puller:t", ImagePullPolicy: corev1.PullIfNotPresent,
		CacheHostPath: "/var/lib/nebula/models", ArtifactSecretName: "nebula-artifact-store",
		StartingTimeoutSec: 600, MaxArtifactBytes: 1 << 30,
	}
}

const sum = "0000000000000000000000000000000000000000000000000000000000000abc"

func deployment(runtime models.Runtime, replicas int32) *store.Deployment {
	return &store.Deployment{
		ID: uuid.New(), OrgID: uuid.New(), OrgSlug: "acme", Name: "qwen-prod",
		State: lifecycle.Pending, StateEnteredAt: time.Now(), Generation: 1, CurrentRevision: 1,
		DesiredReplicas:  replicas,
		Resources:        json.RawMessage(`{"cpu_milli":1000,"memory_mib":2048}`),
		RuntimeOverrides: json.RawMessage(`{"n_parallel":4}`), QueueConfig: json.RawMessage(`{"max_depth":16}`),
		Version: store.Version{
			ID: uuid.New(), Model: "qwen2.5", Version: "0.5b-q4", Format: models.FormatGGUF, Runtime: runtime,
			ChecksumHex: sum, SizeBytes: 1234, ArtifactURI: "s3://nebula-models/sha256/" + sum, ContextWindow: 4096,
			HardwareProfile: json.RawMessage(`{"min_ram_mib":1024}`), RuntimeConfig: json.RawMessage(`{"n_ctx":4096}`),
		},
	}
}

type env struct {
	kube *fake.Clientset
	st   *memStore
	r    *deploymentctrl.Reconciler
	now  time.Time
}

func newEnv(ds ...*store.Deployment) *env {
	st := &memStore{rows: map[uuid.UUID]*store.Deployment{}}
	for _, d := range ds {
		st.rows[d.ID] = d
	}
	e := &env{kube: fake.NewClientset(), st: st, now: time.Now()}
	e.r = &deploymentctrl.Reconciler{
		Kube: e.kube, Store: st, Settings: settings(), StartingTimeout: 10 * time.Minute,
		Logger: telemetry.Discard(), Now: func() time.Time { return e.now },
	}
	return e
}

func (e *env) reconcile(t *testing.T, id uuid.UUID) deploymentctrl.Result {
	t.Helper()
	res, err := e.r.Reconcile(context.Background(), id)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (e *env) dep(t *testing.T) *appsv1.Deployment {
	t.Helper()
	d, err := e.kube.AppsV1().Deployments(ns).Get(context.Background(), "nebula-acme-qwen-prod", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("worker deployment: %v", err)
	}
	return d
}

// setReady simulates the kube controller-manager reporting ready replicas.
func (e *env) setReady(t *testing.T, ready int32) {
	t.Helper()
	d := e.dep(t)
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: *d.Spec.Replicas,
		ReadyReplicas: ready, UpdatedReplicas: ready, AvailableReplicas: ready}
	if _, err := e.kube.AppsV1().Deployments(ns).UpdateStatus(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleToReady(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 2)
	e := newEnv(d)

	res := e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Starting {
		t.Fatalf("after first pass: %s (transitions %v)", got, e.st.transitions)
	}
	if res.RequeueAfter == 0 {
		t.Error("a starting deployment must be looked at again")
	}
	if strings.Join(e.st.transitions, ",") != "pending>provisioning:Claimed,provisioning>starting:ObjectsApplied" {
		t.Errorf("transitions: %v", e.st.transitions)
	}

	e.setReady(t, 1)
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Degraded {
		t.Fatalf("1/2 ready: %s", got)
	}
	e.setReady(t, 2)
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Ready {
		t.Fatalf("2/2 ready: %s", got)
	}
	last := e.st.statuses[len(e.st.statuses)-1]
	if last.ObservedGeneration != 1 || last.ReadyReplicas != 2 {
		t.Errorf("status: %+v", last)
	}
	var avail store.Condition
	for _, c := range last.Conditions {
		if c.Type == "Available" {
			avail = c
		}
	}
	if avail.Status != "True" {
		t.Errorf("available condition: %+v", last.Conditions)
	}

	// At rest: another pass changes nothing.
	n := len(e.st.transitions)
	if res := e.reconcile(t, d.ID); res.RequeueAfter != 0 || len(e.st.transitions) != n {
		t.Errorf("a ready deployment must be at rest: %+v %v", res, e.st.transitions)
	}
}

// The object set: labels, owner references, the root, the PDB, and a pod spec
// that carries every documented safety property.
func TestObjectsAreComplete(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 2)
	e := newEnv(d)
	e.reconcile(t, d.ID)
	ctx := context.Background()

	root, err := e.kube.CoreV1().ConfigMaps(ns).Get(ctx, "nebula-acme-qwen-prod-root", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if root.Labels[k8s.LabelComponent] != "root" || root.Data["artifact_sha256"] != sum ||
		root.Data["model_version"] != "qwen2.5:0.5b-q4" {
		t.Errorf("root: %+v %+v", root.Labels, root.Data)
	}

	dep := e.dep(t)
	if len(dep.OwnerReferences) != 1 || dep.OwnerReferences[0].Name != root.Name || !*dep.OwnerReferences[0].Controller {
		t.Errorf("owner refs: %+v", dep.OwnerReferences)
	}
	for _, l := range []string{k8s.LabelManagedBy, k8s.LabelDeploymentID, k8s.LabelVersionID, k8s.LabelRuntime, k8s.LabelRevision, k8s.LabelOrg} {
		if dep.Labels[l] == "" || dep.Spec.Template.Labels[l] == "" {
			t.Errorf("label %s missing", l)
		}
	}
	if dep.Annotations[k8s.AnnotationSpecHash] == "" || dep.Spec.Template.Annotations[k8s.AnnotationSpecHash] == "" {
		t.Error("spec hash annotation missing")
	}
	if *dep.Spec.Replicas != 2 || dep.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 {
		t.Errorf("replicas/strategy: %d %+v", *dep.Spec.Replicas, dep.Spec.Strategy)
	}

	pod := dep.Spec.Template.Spec
	if *pod.AutomountServiceAccountToken || pod.SecurityContext.RunAsNonRoot == nil || !*pod.SecurityContext.RunAsNonRoot {
		t.Error("pod must run non-root with no API token")
	}
	if pod.NodeSelector["nebula.dev/accelerator"] != "cpu" || len(pod.TopologySpreadConstraints) != 1 {
		t.Errorf("placement: %+v", pod.NodeSelector)
	}
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Name != "artifact-puller" {
		t.Fatalf("a llama.cpp worker needs the artifact puller: %+v", pod.InitContainers)
	}
	puller := pod.InitContainers[0]
	if envOf(puller, "NEBULA_ARTIFACT_SHA256") != sum || puller.EnvFrom[0].SecretRef.Name != "nebula-artifact-store" ||
		puller.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Errorf("puller: %+v", puller)
	}
	w := pod.Containers[0]
	if w.StartupProbe == nil || w.ReadinessProbe == nil || w.LivenessProbe == nil ||
		w.LivenessProbe.HTTPGet.Path != "/livez" || w.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Error("all three probes, asking three different questions")
	}
	if !*w.SecurityContext.ReadOnlyRootFilesystem || *w.SecurityContext.AllowPrivilegeEscalation ||
		len(w.SecurityContext.Capabilities.Drop) != 1 {
		t.Errorf("container security: %+v", w.SecurityContext)
	}
	if _, ok := w.Resources.Limits[corev1.ResourceCPU]; ok {
		t.Error("no CPU limit on CPU inference")
	}
	if w.Resources.Requests.Memory().String() != "2Gi" || w.Resources.Limits.Memory().String() != "2560Mi" {
		t.Errorf("memory: %s / %s", w.Resources.Requests.Memory(), w.Resources.Limits.Memory())
	}
	if envOf(w, "NEBULA_WORKER_MODEL_VERSION") != "qwen2.5:0.5b-q4" || envOf(w, "NEBULA_WORKER_STRICT_HEADERS") != "true" ||
		envOf(w, "NEBULA_WORKER_MODEL_PATH") != "/models/sha256/"+sum || envOf(w, "NEBULA_WORKER_MAX_QUEUE_DEPTH") != "16" {
		t.Errorf("worker env: %+v", w.Env)
	}
	var rc map[string]any
	_ = json.Unmarshal([]byte(envOf(w, "NEBULA_WORKER_RUNTIME_CONFIG")), &rc)
	if rc["n_ctx"] != 4096.0 || rc["n_parallel"] != 4.0 {
		t.Errorf("runtime config must merge the version's config and the deployment's overrides: %v", rc)
	}

	if _, err := e.kube.CoreV1().Services(ns).Get(ctx, "nebula-acme-qwen-prod", metav1.GetOptions{}); err != nil {
		t.Errorf("service: %v", err)
	}
	if _, err := e.kube.PolicyV1().PodDisruptionBudgets(ns).Get(ctx, "nebula-acme-qwen-prod", metav1.GetOptions{}); err != nil {
		t.Errorf("a two-replica deployment needs a PDB: %v", err)
	}
}

func envOf(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// The mock runtime is a declared stub: no artifact, no puller, the mock image.
func TestMockRuntimeHasNoPuller(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeMock, 1)
	d.Version.Format = models.FormatMock
	e := newEnv(d)
	e.reconcile(t, d.ID)
	pod := e.dep(t).Spec.Template.Spec
	if len(pod.InitContainers) != 0 || pod.Containers[0].Image != "nebula/worker-mock:t" ||
		envOf(pod.Containers[0], "NEBULA_WORKER_MODEL_PATH") != "" {
		t.Errorf("mock pod: %+v", pod)
	}
	if _, err := e.kube.PolicyV1().PodDisruptionBudgets(ns).Get(context.Background(), "nebula-acme-qwen-prod", metav1.GetOptions{}); err == nil {
		t.Error("a single replica must not get a PDB: it would block every drain")
	}
}

// The deliberate test: delete the Deployment out from under NEBULA and it comes
// back; edit it by hand and the edit is reverted.
func TestDriftIsCorrected(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 2)
	e := newEnv(d)
	e.reconcile(t, d.ID)
	e.setReady(t, 2)
	e.reconcile(t, d.ID)
	ctx := context.Background()

	if err := e.kube.AppsV1().Deployments(ns).Delete(ctx, "nebula-acme-qwen-prod", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	e.reconcile(t, d.ID)
	if _, err := e.kube.AppsV1().Deployments(ns).Get(ctx, "nebula-acme-qwen-prod", metav1.GetOptions{}); err != nil {
		t.Fatalf("a deleted Deployment must be recreated: %v", err)
	}

	dep := e.dep(t)
	three := int32(3)
	dep.Spec.Replicas = &three
	dep.Spec.Template.Spec.Containers[0].Image = "attacker/image:1"
	if _, err := e.kube.AppsV1().Deployments(ns).Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.reconcile(t, d.ID)
	dep = e.dep(t)
	if *dep.Spec.Replicas != 2 || dep.Spec.Template.Spec.Containers[0].Image != "nebula/worker-llamacpp:t" {
		t.Errorf("drift must be reverted: replicas %d image %s", *dep.Spec.Replicas, dep.Spec.Template.Spec.Containers[0].Image)
	}
}

// A spec change goes back through provisioning and rolls the pods; a scale does
// not change the pod template.
func TestSpecChangeRollsAndScaleDoesNot(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 2)
	e := newEnv(d)
	e.reconcile(t, d.ID)
	e.setReady(t, 2)
	e.reconcile(t, d.ID)
	hash := e.dep(t).Spec.Template.Annotations[k8s.AnnotationSpecHash]

	e.st.mu.Lock()
	row := e.st.rows[d.ID]
	row.Generation, row.DesiredReplicas = 2, 3
	e.st.mu.Unlock()
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Starting {
		t.Fatalf("after a scale: %s", got)
	}
	if dep := e.dep(t); *dep.Spec.Replicas != 3 || dep.Spec.Template.Annotations[k8s.AnnotationSpecHash] != hash {
		t.Errorf("a scale must change replicas and not the template")
	}

	e.st.mu.Lock()
	row.Generation, row.Resources = 3, json.RawMessage(`{"cpu_milli":2000,"memory_mib":4096}`)
	e.st.mu.Unlock()
	e.reconcile(t, d.ID)
	if e.dep(t).Spec.Template.Annotations[k8s.AnnotationSpecHash] == hash {
		t.Error("a resource change must roll the pods")
	}
	joined := strings.Join(e.st.transitions, ",")
	if !strings.Contains(joined, "ready>provisioning:SpecChanged") {
		t.Errorf("transitions: %s", joined)
	}
}

func TestStopScalesToZeroThenStopped(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 2)
	d.State = lifecycle.Stopping
	d.ObservedGeneration = 1
	e := newEnv(d)
	e.reconcile(t, d.ID)
	if *e.dep(t).Spec.Replicas != 0 {
		t.Fatal("stopping must scale to zero")
	}
	e.setReady(t, 0)
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Stopped {
		t.Fatalf("with no pods left: %s", got)
	}
}

// A puller that reports a checksum mismatch fails the deployment at once, with
// the reason, rather than after the start timeout.
func TestArtifactFailureFailsWithTheReason(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 1)
	e := newEnv(d)
	e.reconcile(t, d.ID)

	msg, _ := json.Marshal(artifact.FetchResult{Status: artifact.FetchFailed, ErrorCode: "checksum_mismatch",
		Error: "artifact checksum does not match: declared abc, computed def"})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: ns, Labels: map[string]string{k8s.LabelDeploymentID: d.ID.String()}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name:  "artifact-puller",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 3, Message: string(msg)}},
		}}},
	}
	if _, err := e.kube.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Failed {
		t.Fatalf("state %s", got)
	}
	if !strings.Contains(strings.Join(e.st.transitions, ","), "starting>failed:ArtifactChecksumMismatch") {
		t.Errorf("transitions: %v", e.st.transitions)
	}
}

// A deployment that never becomes ready fails after the start timeout, and the
// failure names what the pods said.
func TestStartTimeout(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 1)
	e := newEnv(d)
	e.reconcile(t, d.ID)
	e.setReady(t, 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: ns, Labels: map[string]string{k8s.LabelDeploymentID: d.ID.String()}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			Message: "0/3 nodes are available: 3 Insufficient memory.",
		}}},
	}
	_, _ = e.kube.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	e.now = e.now.Add(11 * time.Minute)
	e.reconcile(t, d.ID)
	if got := e.st.state(d.ID); got != lifecycle.Failed {
		t.Fatalf("state %s", got)
	}
	last := e.st.statuses[len(e.st.statuses)-1]
	if !strings.Contains(last.LastError, "Insufficient memory") {
		t.Errorf("the failure must carry the pod's explanation: %q", last.LastError)
	}
}

// Deleted in PostgreSQL → objects removed; orphans (no row at all) collected only
// against a successfully read live set.
func TestDeletionAndOrphans(t *testing.T) {
	t.Parallel()
	d := deployment(models.RuntimeLlamaCPP, 1)
	e := newEnv(d)
	e.reconcile(t, d.ID)
	ctx := context.Background()

	e.st.mu.Lock()
	delete(e.st.rows, d.ID)
	e.st.mu.Unlock()
	e.reconcile(t, d.ID)
	cms, _ := e.kube.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{})
	deps, _ := e.kube.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if len(cms.Items) != 0 || len(deps.Items) != 0 {
		t.Errorf("a deleted deployment's objects must go: %d configmaps, %d deployments", len(cms.Items), len(deps.Items))
	}

	keep := deployment(models.RuntimeLlamaCPP, 1)
	keep.Name = "keep"
	orphan := deployment(models.RuntimeLlamaCPP, 1)
	orphan.Name = "orphan"
	e2 := newEnv(keep, orphan)
	e2.reconcile(t, keep.ID)
	e2.reconcile(t, orphan.ID)
	n, err := e2.r.CollectOrphans(ctx, map[uuid.UUID]bool{keep.ID: true})
	if err != nil || n != 1 {
		t.Fatalf("collected %d, %v", n, err)
	}
	left, _ := e2.kube.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if len(left.Items) != 1 || left.Items[0].Labels[k8s.LabelDeploymentID] != keep.ID.String() {
		t.Errorf("only the orphan may be collected: %d left", len(left.Items))
	}
}
