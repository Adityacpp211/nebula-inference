package deploymentctrl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Store is the database surface the reconciler uses. Satisfied by *store.Store;
// an interface so the reconciler is testable with an in-memory fake.
type Store interface {
	Get(ctx context.Context, id uuid.UUID) (*store.Deployment, error)
	Transition(ctx context.Context, id uuid.UUID, from, to models.DeploymentState, reason, message string) (bool, error)
	WriteStatus(ctx context.Context, id uuid.UUID, st store.Status) error
}

// Reconciler drives one deployment toward its desired state per call.
type Reconciler struct {
	Kube            kubernetes.Interface
	Store           Store
	Settings        Settings
	StartingTimeout time.Duration
	Logger          *slog.Logger
	Now             func() time.Time
}

// Result tells the caller when to look again.
type Result struct {
	// RequeueAfter asks for another pass even without an event; zero means the
	// deployment is at rest.
	RequeueAfter time.Duration
}

// Reconcile is idempotent: calling it twice in a row changes nothing the second
// time. That property is what makes an informer event, a poll and a resync all
// safe triggers for the same work.
func (r *Reconciler) Reconcile(ctx context.Context, id uuid.UUID) (Result, error) {
	now := r.now()
	d, err := r.Store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		// Deleted in PostgreSQL: remove its objects. Deleting the root garbage-
		// collects the rest through owner references.
		return Result{}, r.deleteObjects(ctx, id)
	}
	if err != nil {
		return Result{}, fmt.Errorf("reading deployment: %w", err)
	}
	log := r.Logger.With(slog.String("deployment_id", id.String()), slog.String("org", d.OrgSlug),
		slog.String("deployment", d.Name))

	// Transitions that precede an apply.
	switch {
	case d.State == lifecycle.Pending:
		if d, err = r.move(ctx, d, lifecycle.Provisioning, ReasonClaimed,
			fmt.Sprintf("applying generation %d", d.Generation)); err != nil || d == nil {
			return Result{RequeueAfter: time.Second}, err
		}
	case d.Generation > d.ObservedGeneration &&
		(d.State == lifecycle.Starting || d.State == lifecycle.Ready || d.State == lifecycle.Degraded):
		if d, err = r.move(ctx, d, lifecycle.Provisioning, ReasonSpecChanged,
			fmt.Sprintf("reconciling generation %d", d.Generation)); err != nil || d == nil {
			return Result{RequeueAfter: time.Second}, err
		}
	case d.Generation > d.ObservedGeneration && d.State == lifecycle.Failed:
		if d, err = r.move(ctx, d, lifecycle.Provisioning, ReasonNewRevision,
			fmt.Sprintf("a new revision (generation %d) was applied after the failure", d.Generation)); err != nil || d == nil {
			return Result{RequeueAfter: time.Second}, err
		}
	}

	objs, err := Build(d, r.Settings)
	if err != nil {
		// The spec cannot be turned into objects. Waiting will not change that.
		log.WarnContext(ctx, "deployment spec cannot be built", slog.String("cause", err.Error()))
		if d.State == lifecycle.Provisioning {
			_, _ = r.move(ctx, d, lifecycle.Failed, ReasonInvalidSpec, err.Error())
		}
		return Result{}, r.Store.WriteStatus(ctx, d.ID, store.Status{
			ObservedGeneration: d.ObservedGeneration, LastError: err.Error(),
			ReadyReplicas: d.ReadyReplicas, UpdatedReplicas: d.UpdatedReplicas,
		})
	}

	applied, err := r.apply(ctx, objs)
	if err != nil {
		if permanent(err) {
			log.ErrorContext(ctx, "applying objects was refused", slog.String("cause", err.Error()))
			if d.State == lifecycle.Provisioning {
				_, _ = r.move(ctx, d, lifecycle.Failed, ReasonApplyFailed, err.Error())
			}
			return Result{}, r.Store.WriteStatus(ctx, d.ID, store.Status{
				ObservedGeneration: d.ObservedGeneration, LastError: err.Error(),
				ReadyReplicas: d.ReadyReplicas, UpdatedReplicas: d.UpdatedReplicas,
			})
		}
		return Result{}, fmt.Errorf("applying objects: %w", err)
	}
	d.ObservedGeneration = d.Generation

	obs, err := r.observe(ctx, applied, objs)
	if err != nil {
		return Result{}, fmt.Errorf("observing objects: %w", err)
	}

	dec := Decide(d, obs, now, r.StartingTimeout)
	if dec.To != "" && dec.To != d.State {
		next, err := r.move(ctx, d, dec.To, dec.Reason, dec.Message)
		if err != nil {
			return Result{}, err
		}
		if next == nil {
			return Result{RequeueAfter: time.Second}, nil
		}
		d = next
		log.InfoContext(ctx, "deployment state changed", slog.String("to", string(dec.To)),
			slog.String("reason", dec.Reason))
	}

	status := store.Status{
		ObservedGeneration: d.ObservedGeneration,
		ReadyReplicas:      obs.Ready,
		UpdatedReplicas:    obs.Updated,
		Conditions:         Conditions(d, obs, d.Conditions, now),
	}
	if obs.ArtifactFailure != nil {
		status.LastError = obs.ArtifactFailure.Message
	} else if len(obs.Problems) > 0 && obs.Ready < Replicas(d) {
		status.LastError = strings.Join(obs.Problems, "; ")
	}
	if err := r.Store.WriteStatus(ctx, d.ID, status); err != nil {
		return Result{}, fmt.Errorf("writing status: %w", err)
	}

	switch d.State {
	case lifecycle.Provisioning, lifecycle.Starting, lifecycle.Stopping:
		return Result{RequeueAfter: 2 * time.Second}, nil
	case lifecycle.Degraded:
		return Result{RequeueAfter: 5 * time.Second}, nil
	}
	return Result{}, nil
}

// move performs a compare-and-set transition and re-reads the row. A nil
// deployment with a nil error means someone else moved it first: the caller
// requeues and starts over from what the database says now.
func (r *Reconciler) move(ctx context.Context, d *store.Deployment, to models.DeploymentState, reason, msg string) (*store.Deployment, error) {
	if !lifecycle.CanTransition(d.State, to) {
		return d, nil
	}
	ok, err := r.Store.Transition(ctx, d.ID, d.State, to, reason, msg)
	if err != nil || !ok {
		return nil, err
	}
	next, err := r.Store.Get(ctx, d.ID)
	if err != nil {
		return nil, err
	}
	// The row's observed_generation is written later in the pass; keep the value
	// this pass is working with.
	next.ObservedGeneration = max(next.ObservedGeneration, d.ObservedGeneration)
	return next, nil
}

// apply server-side-applies the set. The root first, because the others carry
// owner references to its UID. Server-side apply rather than create-or-update: the
// same call creates a missing object, corrects drift in a changed one, and leaves
// fields other managers own (the HPA's replicas, later) alone.
func (r *Reconciler) apply(ctx context.Context, o *Objects) (*appsv1.Deployment, error) {
	ns := r.Settings.Namespace
	force := true
	opts := metav1.PatchOptions{FieldManager: k8s.FieldManager, Force: &force}

	body, err := json.Marshal(o.Root)
	if err != nil {
		return nil, err
	}
	root, err := r.Kube.CoreV1().ConfigMaps(ns).Patch(ctx, o.Root.Name, types.ApplyPatchType, body, opts)
	if err != nil {
		return nil, fmt.Errorf("root configmap: %w", err)
	}
	owner := []metav1.OwnerReference{{
		APIVersion: "v1", Kind: "ConfigMap", Name: root.Name, UID: root.UID,
		BlockOwnerDeletion: ptr(true), Controller: ptr(true),
	}}
	o.Deployment.OwnerReferences = owner
	o.Service.OwnerReferences = owner

	if body, err = json.Marshal(o.Deployment); err != nil {
		return nil, err
	}
	dep, err := r.Kube.AppsV1().Deployments(ns).Patch(ctx, o.Deployment.Name, types.ApplyPatchType, body, opts)
	if err != nil {
		return nil, fmt.Errorf("deployment: %w", err)
	}
	if body, err = json.Marshal(o.Service); err != nil {
		return nil, err
	}
	if _, err := r.Kube.CoreV1().Services(ns).Patch(ctx, o.Service.Name, types.ApplyPatchType, body, opts); err != nil {
		return nil, fmt.Errorf("service: %w", err)
	}
	if o.PDB != nil {
		o.PDB.OwnerReferences = owner
		if body, err = json.Marshal(o.PDB); err != nil {
			return nil, err
		}
		if _, err := r.Kube.PolicyV1().PodDisruptionBudgets(ns).Patch(ctx, o.PDB.Name, types.ApplyPatchType, body, opts); err != nil {
			return nil, fmt.Errorf("pod disruption budget: %w", err)
		}
	} else {
		err := r.Kube.PolicyV1().PodDisruptionBudgets(ns).Delete(ctx, o.Deployment.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("removing pod disruption budget: %w", err)
		}
	}
	return dep, nil
}

// observe reads what Kubernetes reports about the applied deployment.
func (r *Reconciler) observe(ctx context.Context, dep *appsv1.Deployment, o *Objects) (Observed, error) {
	obs := Observed{Exists: dep != nil}
	if dep == nil {
		return obs, nil
	}
	obs.Current = dep.Status.ObservedGeneration >= dep.Generation
	obs.Replicas = dep.Status.Replicas
	obs.Ready = dep.Status.ReadyReplicas
	obs.Updated = dep.Status.UpdatedReplicas
	obs.Available = dep.Status.AvailableReplicas

	pods, err := r.Kube.CoreV1().Pods(r.Settings.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: k8s.LabelDeploymentID + "=" + o.Deployment.Labels[k8s.LabelDeploymentID],
	})
	if err != nil {
		return obs, err
	}
	obs.Pods = len(pods.Items)
	seen := map[string]bool{}
	add := func(p string) {
		if !seen[p] {
			seen[p] = true
			obs.Problems = append(obs.Problems, p)
		}
	}
	for i := range pods.Items {
		inspectPod(&pods.Items[i], &obs, add)
	}
	return obs, nil
}

// inspectPod extracts the explanations a human would otherwise dig out of
// `kubectl describe`.
func inspectPod(p *corev1.Pod, obs *Observed, add func(string)) {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			add("unschedulable: " + c.Message)
		}
	}
	for _, cs := range p.Status.InitContainerStatuses {
		if cs.Name != containerPuller {
			continue
		}
		for _, t := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
			if t == nil || t.ExitCode == 0 {
				continue
			}
			var res artifact.FetchResult
			if err := json.Unmarshal([]byte(t.Message), &res); err == nil && res.Status == artifact.FetchFailed {
				if obs.ArtifactFailure == nil {
					obs.ArtifactFailure = &ArtifactFailure{Code: res.ErrorCode, Message: "artifact pull failed: " + res.Error}
				}
			} else {
				add(fmt.Sprintf("artifact-puller exited %d: %s", t.ExitCode, firstLine(t.Message)))
			}
		}
		if w := cs.State.Waiting; w != nil && w.Reason != "" && w.Reason != "PodInitializing" {
			add("artifact-puller " + w.Reason + ": " + firstLine(w.Message))
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil && w.Reason != "" && w.Reason != "ContainerCreating" {
			msg := w.Reason
			if w.Message != "" {
				msg += ": " + firstLine(w.Message)
			}
			if t := cs.LastTerminationState.Terminated; t != nil {
				msg += fmt.Sprintf(" (last exit %d %s)", t.ExitCode, t.Reason)
			}
			add(cs.Name + " " + msg)
		}
		if t := cs.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" {
			add(cs.Name + " was OOMKilled: raise resources.memory_mib")
		}
	}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

// deleteObjects removes a deleted deployment's objects.
func (r *Reconciler) deleteObjects(ctx context.Context, id uuid.UUID) error {
	sel := k8s.LabelDeploymentID + "=" + id.String()
	bg := metav1.DeletePropagationBackground
	opts := metav1.DeleteOptions{PropagationPolicy: &bg}
	ns := r.Settings.Namespace
	cms, err := r.Kube.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{LabelSelector: sel + "," + k8s.ManagedSelector()})
	if err != nil {
		return err
	}
	for _, cm := range cms.Items {
		if err := r.Kube.CoreV1().ConfigMaps(ns).Delete(ctx, cm.Name, opts); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	// Objects that lost their owner reference (created by hand, or before the root
	// existed) are not garbage-collected with it; remove them directly.
	deps, err := r.Kube.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{LabelSelector: sel + "," + k8s.ManagedSelector()})
	if err != nil {
		return err
	}
	for _, dep := range deps.Items {
		if err := r.Kube.AppsV1().Deployments(ns).Delete(ctx, dep.Name, opts); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	svcs, err := r.Kube.CoreV1().Services(ns).List(ctx, metav1.ListOptions{LabelSelector: sel + "," + k8s.ManagedSelector()})
	if err != nil {
		return err
	}
	for _, svc := range svcs.Items {
		if err := r.Kube.CoreV1().Services(ns).Delete(ctx, svc.Name, opts); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// CollectOrphans deletes managed objects whose deployment no longer exists. It is
// called only with a live set read successfully from PostgreSQL: when the database
// cannot be read, nothing is deleted, because an object the controller cannot
// verify is an orphan is an object someone may be serving from.
func (r *Reconciler) CollectOrphans(ctx context.Context, live map[uuid.UUID]bool) (int, error) {
	ns := r.Settings.Namespace
	seen := map[string]bool{}
	cms, err := r.Kube.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{LabelSelector: k8s.ManagedSelector()})
	if err != nil {
		return 0, err
	}
	deps, err := r.Kube.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{LabelSelector: k8s.ManagedSelector()})
	if err != nil {
		return 0, err
	}
	ids := []string{}
	for _, cm := range cms.Items {
		ids = append(ids, cm.Labels[k8s.LabelDeploymentID])
	}
	for _, dep := range deps.Items {
		ids = append(ids, dep.Labels[k8s.LabelDeploymentID])
	}
	n := 0
	for _, raw := range ids {
		if seen[raw] {
			continue
		}
		seen[raw] = true
		id, err := uuid.Parse(raw)
		if err != nil || live[id] {
			continue
		}
		if err := r.deleteObjects(ctx, id); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// permanent reports errors retrying cannot fix: the API server refused the object
// itself, not the attempt.
func permanent(err error) bool {
	return apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsBadRequest(err)
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
