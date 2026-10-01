// Package inventoryctrl keeps the node cache and the worker event history.
//
// Two jobs, both observational (axiom A1 — Kubernetes is the truth, these are
// read conveniences):
//
//   - Nodes: allocatable capacity and the sum of pod requests per node, written
//     to the `nodes` table so the control plane can answer capacity admission
//     without a Kubernetes client (it has none, by design).
//   - Worker events: pod lifecycle changes of NEBULA workers, appended to
//     `worker_events`, because Kubernetes Events expire in about an hour and "why
//     was this deployment degraded yesterday?" must still have an answer.
package inventoryctrl

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	listerscorev1 "k8s.io/client-go/listers/core/v1"

	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/scheduler"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// NodeStore is the node-cache surface.
type NodeStore interface {
	SyncNodes(ctx context.Context, nodes []store.Node) error
}

// EventStore records worker events.
type EventStore interface {
	RecordWorkerEvent(ctx context.Context, e store.WorkerEvent) error
}

// Snapshot builds the node rows from listers. Pure given its inputs.
func Snapshot(nodes []*corev1.Node, pods []*corev1.Pod) []store.Node {
	requested := map[string]*store.Amounts{}
	for _, p := range pods {
		if p.Spec.NodeName == "" || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue // unscheduled, or finished and no longer holding resources
		}
		a := requested[p.Spec.NodeName]
		if a == nil {
			a = &store.Amounts{}
			requested[p.Spec.NodeName] = a
		}
		r := podRequests(p)
		a.CPUMilli += r.CPUMilli
		a.MemoryMiB += r.MemoryMiB
		a.GPU += r.GPU
	}

	out := make([]store.Node, 0, len(nodes))
	for _, n := range nodes {
		row := store.Node{
			Name:           n.Name,
			ProviderID:     n.Spec.ProviderID,
			Labels:         n.Labels,
			Capacity:       amounts(n.Status.Capacity),
			Allocatable:    amounts(n.Status.Allocatable),
			Schedulable:    !n.Spec.Unschedulable && ready(n),
			KubeletVersion: n.Status.NodeInfo.KubeletVersion,
		}
		if row.Labels == nil {
			row.Labels = map[string]string{}
		}
		if r := requested[n.Name]; r != nil {
			row.Requested = *r
		}
		for _, t := range n.Spec.Taints {
			row.Taints = append(row.Taints, store.Taint{Key: t.Key, Effect: string(t.Effect)})
		}
		for _, c := range n.Status.Conditions {
			row.Conditions = append(row.Conditions, store.Condition{
				Type: string(c.Type), Status: string(c.Status), Reason: c.Reason,
				Message: c.Message, LastTransitionTime: c.LastTransitionTime.Time,
			})
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// podRequests is the effective request of a pod as the scheduler computes it: the
// larger of the sum of its containers and its largest init container.
func podRequests(p *corev1.Pod) store.Amounts {
	var sum, initMax store.Amounts
	for _, c := range p.Spec.Containers {
		a := amounts(c.Resources.Requests)
		sum.CPUMilli += a.CPUMilli
		sum.MemoryMiB += a.MemoryMiB
		sum.GPU += a.GPU
	}
	for _, c := range p.Spec.InitContainers {
		a := amounts(c.Resources.Requests)
		initMax.CPUMilli = max(initMax.CPUMilli, a.CPUMilli)
		initMax.MemoryMiB = max(initMax.MemoryMiB, a.MemoryMiB)
		initMax.GPU = max(initMax.GPU, a.GPU)
	}
	return store.Amounts{
		CPUMilli: max(sum.CPUMilli, initMax.CPUMilli), MemoryMiB: max(sum.MemoryMiB, initMax.MemoryMiB),
		GPU: max(sum.GPU, initMax.GPU),
	}
}

func amounts(rl corev1.ResourceList) store.Amounts {
	var a store.Amounts
	if q, ok := rl[corev1.ResourceCPU]; ok {
		a.CPUMilli = k8s.CPUMilli(q)
	}
	if q, ok := rl[corev1.ResourceMemory]; ok {
		a.MemoryMiB = k8s.MemoryMiB(q)
	}
	if q, ok := rl[corev1.ResourceName(scheduler.ResourceGPU)]; ok {
		a.GPU = q.Value()
	}
	return a
}

func ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Nodes periodically syncs the node cache.
type Nodes struct {
	Nodes    listerscorev1.NodeLister
	Pods     listerscorev1.PodLister
	Store    NodeStore
	Interval time.Duration
	Logger   *slog.Logger
}

// Run syncs until ctx ends. A failed sync is logged and retried next interval;
// the cache simply ages, and admission stops trusting it after five minutes.
func (n *Nodes) Run(ctx context.Context) {
	t := time.NewTicker(n.Interval)
	defer t.Stop()
	for {
		if err := n.Sync(ctx); err != nil {
			n.Logger.WarnContext(ctx, "node inventory sync failed", slog.String("cause", err.Error()))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Sync writes one snapshot.
func (n *Nodes) Sync(ctx context.Context) error {
	nodes, err := n.Nodes.List(labels.Everything())
	if err != nil {
		return err
	}
	pods, err := n.Pods.List(labels.Everything())
	if err != nil {
		return err
	}
	return n.Store.SyncNodes(ctx, Snapshot(nodes, pods))
}

// ---------------------------------------------------------------------------
// worker events
// ---------------------------------------------------------------------------

// Events turns pod status changes into worker_events rows.
type Events struct {
	Store  EventStore
	Logger *slog.Logger

	mu   sync.Mutex
	seen map[string]podMemo
}

type podMemo struct {
	scheduled bool
	ready     bool
	restarts  int32
	pulled    bool
}

// Observe compares a pod with what was last recorded for it and appends the
// events that happened in between. Called from informer add/update handlers.
func (e *Events) Observe(ctx context.Context, p *corev1.Pod) {
	depID, err := uuid.Parse(p.Labels[k8s.LabelDeploymentID])
	if err != nil {
		return
	}
	e.mu.Lock()
	if e.seen == nil {
		e.seen = map[string]podMemo{}
	}
	prev := e.seen[p.Name]
	cur := memoOf(p)
	e.seen[p.Name] = cur
	e.mu.Unlock()

	record := func(typ string, detail map[string]any) {
		if err := e.Store.RecordWorkerEvent(ctx, store.WorkerEvent{
			OrgID: orgOf(p), DeploymentID: depID, Pod: p.Name, Node: p.Spec.NodeName,
			Type: typ, Detail: detail, OccurredAt: time.Now().UTC(),
		}); err != nil {
			e.Logger.WarnContext(ctx, "recording a worker event failed", slog.String("type", typ),
				slog.String("cause", err.Error()))
		}
	}
	if cur.scheduled && !prev.scheduled {
		record("scheduled", map[string]any{"node": p.Spec.NodeName})
	}
	if cur.pulled && !prev.pulled {
		record("artifact_cached", nil)
	}
	if cur.ready && !prev.ready {
		record("ready", nil)
	}
	if !cur.ready && prev.ready {
		record("unhealthy", nil)
	}
	if cur.restarts > prev.restarts {
		typ, detail := "crashed", map[string]any{"restarts": cur.restarts}
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.LastTerminationState.Terminated; t != nil {
				detail["exit_code"], detail["reason"] = t.ExitCode, t.Reason
				if t.Reason == "OOMKilled" {
					typ = "oom_killed"
				}
			}
		}
		record(typ, detail)
	}
	for _, cs := range p.Status.InitContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 && !prev.pulled && prev.restarts == cur.restarts {
			record("model_load_failed", map[string]any{"container": cs.Name, "exit_code": t.ExitCode, "message": t.Message})
		}
	}
	if p.Status.Reason == "Evicted" {
		record("evicted", map[string]any{"message": p.Status.Message})
	}
}

// Forget records a pod's deletion and drops its memo.
func (e *Events) Forget(ctx context.Context, p *corev1.Pod) {
	e.mu.Lock()
	delete(e.seen, p.Name)
	e.mu.Unlock()
	if depID, err := uuid.Parse(p.Labels[k8s.LabelDeploymentID]); err == nil {
		_ = e.Store.RecordWorkerEvent(ctx, store.WorkerEvent{
			OrgID: orgOf(p), DeploymentID: depID, Pod: p.Name, Node: p.Spec.NodeName,
			Type: "terminated", OccurredAt: time.Now().UTC(),
		})
	}
}

func orgOf(p *corev1.Pod) uuid.UUID {
	id, _ := uuid.Parse(p.Labels[k8s.LabelOrgID])
	return id
}

func memoOf(p *corev1.Pod) podMemo {
	m := podMemo{scheduled: p.Spec.NodeName != ""}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			m.ready = true
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		m.restarts += cs.RestartCount
	}
	for _, cs := range p.Status.InitContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode == 0 {
			m.pulled = true
		}
	}
	return m
}
