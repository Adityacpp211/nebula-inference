package inventoryctrl_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/inventoryctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

func node(name, cpu, mem string, unschedulable bool) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"nebula.dev/accelerator": "cpu"}},
		Spec:       corev1.NodeSpec{Unschedulable: unschedulable, Taints: []corev1.Taint{{Key: "k", Effect: corev1.TaintEffectNoSchedule}}},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)},
			Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)},
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func pod(nodeName string, phase corev1.PodPhase, cpu, mem, initCPU string) *corev1.Pod {
	p := &corev1.Pod{
		Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)},
		}}}},
		Status: corev1.PodStatus{Phase: phase},
	}
	if initCPU != "" {
		p.Spec.InitContainers = []corev1.Container{{Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(initCPU)},
		}}}
	}
	return p
}

func TestSnapshot(t *testing.T) {
	t.Parallel()
	rows := inventoryctrl.Snapshot(
		[]*corev1.Node{node("w2", "4", "8Gi", false), node("w1", "2", "4Gi", true)},
		[]*corev1.Pod{
			pod("w2", corev1.PodRunning, "500m", "1Gi", ""),
			// An init container larger than the main one sets the pod's effective CPU.
			pod("w2", corev1.PodRunning, "100m", "512Mi", "1"),
			// Finished pods hold nothing; unscheduled pods are on no node.
			pod("w2", corev1.PodSucceeded, "2", "2Gi", ""),
			pod("", corev1.PodPending, "2", "2Gi", ""),
		})
	if len(rows) != 2 || rows[0].Name != "w1" {
		t.Fatalf("rows must be sorted by name: %+v", rows)
	}
	w1, w2 := rows[0], rows[1]
	if w1.Schedulable {
		t.Error("a cordoned node is not schedulable")
	}
	if w2.Allocatable != (store.Amounts{CPUMilli: 4000, MemoryMiB: 8192}) {
		t.Errorf("allocatable %+v", w2.Allocatable)
	}
	if w2.Requested != (store.Amounts{CPUMilli: 1500, MemoryMiB: 1536}) {
		t.Errorf("requested %+v, want 1500m (500m + max(100m, 1)) and 1536Mi", w2.Requested)
	}
	if len(w2.Taints) != 1 || w2.Taints[0].Effect != "NoSchedule" {
		t.Errorf("taints %+v", w2.Taints)
	}
}

type memEvents struct {
	mu     sync.Mutex
	events []string
}

func (m *memEvents) RecordWorkerEvent(_ context.Context, e store.WorkerEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e.Type)
	return nil
}

func TestEventsRecordTransitionsOnce(t *testing.T) {
	t.Parallel()
	st := &memEvents{}
	ev := &inventoryctrl.Events{Store: st, Logger: telemetry.Discard()}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Labels: map[string]string{
		k8s.LabelDeploymentID: uuid.NewString(), k8s.LabelOrgID: uuid.NewString(),
	}}}
	ctx := context.Background()

	ev.Observe(ctx, p) // pending, unscheduled: nothing
	p.Spec.NodeName = "w1"
	ev.Observe(ctx, p)
	ev.Observe(ctx, p) // unchanged: nothing new
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	ev.Observe(ctx, p)
	p.Status.Conditions = nil
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{RestartCount: 1, LastTerminationState: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}
	ev.Observe(ctx, p)
	ev.Forget(ctx, p)

	want := []string{"scheduled", "ready", "unhealthy", "oom_killed", "terminated"}
	if len(st.events) != len(want) {
		t.Fatalf("events %v, want %v", st.events, want)
	}
	for i := range want {
		if st.events[i] != want[i] {
			t.Fatalf("events %v, want %v", st.events, want)
		}
	}
}
