// Package scheduler turns a model version's hardware profile and a deployment's
// resources into Kubernetes placement constraints, and admits or rejects a
// deployment against observed cluster capacity before anything is created.
//
// NEBULA does not bind pods: kube-scheduler places them (ADR-0005). What this
// package contributes is the translation Kubernetes cannot do — "this GGUF needs
// 6 GiB and a CPU node" — and an answer to "could this ever fit?" that arrives as
// a 422 with an explanation instead of a Pending pod nobody explains.
//
// It is pure: no Kubernetes client, no database, no clock. The node inventory is
// passed in, which is what makes admission exhaustively testable with table-driven
// fixtures (docs/repository-structure.md §4, rule 5).
package scheduler

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Labels NEBULA places on, and selects, nodes.
const (
	LabelAccelerator = "nebula.dev/accelerator"
	LabelPool        = "nebula.dev/pool"
	LabelGPUProduct  = "nvidia.com/gpu.product"
	ResourceGPU      = "nvidia.com/gpu"
)

// Accelerators.
const (
	AcceleratorCPU = "cpu"
	AcceleratorGPU = "gpu"
)

// HardwareProfile is a model version's requirement, from model_versions.hardware_profile
// (docs/data-model.md). Every field is optional; zero means "no requirement".
type HardwareProfile struct {
	RequiresGPU             bool     `json:"requires_gpu"`
	MinVRAMMiB              int64    `json:"min_vram_mib"`
	MinRAMMiB               int64    `json:"min_ram_mib"`
	MinCPUMilli             int64    `json:"min_cpu_milli"`
	RecommendedCPUMilli     int64    `json:"recommended_cpu_milli"`
	SupportedAccelerators   []string `json:"supported_accelerators"`
	ParallelSlots           int      `json:"parallel_slots"`
	GPUProduct              string   `json:"gpu_product,omitempty"`
	GPUComputeCapabilityMin *string  `json:"gpu_compute_capability_min,omitempty"`
}

// Resources is a deployment's per-replica request, from deployments.resources.
type Resources struct {
	CPUMilli  int64  `json:"cpu_milli"`
	MemoryMiB int64  `json:"memory_mib"`
	GPUCount  int64  `json:"gpu_count"`
	GPUType   string `json:"gpu_type,omitempty"`
}

// ParseProfile decodes a hardware profile, tolerating unknown fields (the profile
// is extended by later phases and an old controller must not refuse a new row).
func ParseProfile(raw []byte) (HardwareProfile, error) {
	var p HardwareProfile
	if len(raw) == 0 {
		return p, nil
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("hardware_profile: %w", err)
	}
	return p, nil
}

// ParseResources decodes deployment resources.
func ParseResources(raw []byte) (Resources, error) {
	var r Resources
	if len(raw) == 0 {
		return r, nil
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("resources: %w", err)
	}
	return r, nil
}

// Constraints is what the controller puts on a worker pod.
type Constraints struct {
	NodeSelector map[string]string `json:"nodeSelector"`
	// Requests and Limits use Kubernetes quantity strings.
	Requests    map[string]string `json:"requests"`
	Limits      map[string]string `json:"limits"`
	Tolerations []Toleration      `json:"tolerations,omitempty"`
	// RuntimeClassName is set for GPU pods.
	RuntimeClassName string `json:"runtimeClassName,omitempty"`
	// SpreadByHostname asks for replicas on different nodes when possible. Always
	// "ScheduleAnyway": prefer spread, never block a single-node cluster.
	SpreadByHostname bool `json:"spreadByHostname"`
}

// Toleration mirrors the Kubernetes type without importing it.
type Toleration struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Effect   string `json:"effect"`
}

// MemoryHeadroom is the ratio of the memory limit to the request. The difference
// is room for the KV cache to grow under concurrent long contexts; a limit equal to
// the request turns the first long prompt into an OOM kill.
const MemoryHeadroom = 1.25

// Request is one placement question.
type Request struct {
	Profile   HardwareProfile
	Resources Resources
	Replicas  int
}

// Effective returns the per-replica demand: the deployment's request, raised to
// the model's minimums. A deployment cannot ask for less than the model needs to
// load; it is corrected here rather than failing later with an OOM.
func (r Request) Effective() Resources {
	e := r.Resources
	if e.CPUMilli < r.Profile.MinCPUMilli {
		e.CPUMilli = r.Profile.MinCPUMilli
	}
	if e.CPUMilli == 0 {
		e.CPUMilli = r.Profile.RecommendedCPUMilli
	}
	if e.MemoryMiB < r.Profile.MinRAMMiB {
		e.MemoryMiB = r.Profile.MinRAMMiB
	}
	if r.Profile.RequiresGPU && e.GPUCount == 0 {
		e.GPUCount = 1
	}
	return e
}

// Validate refuses requests that are malformed regardless of the cluster.
func (r Request) Validate() error {
	e := r.Effective()
	var problems []string
	if e.CPUMilli <= 0 {
		problems = append(problems, "resources.cpu_milli must be positive (or the model must declare min_cpu_milli)")
	}
	if e.MemoryMiB <= 0 {
		problems = append(problems, "resources.memory_mib must be positive (or the model must declare min_ram_mib)")
	}
	if e.GPUCount < 0 {
		problems = append(problems, "resources.gpu_count must not be negative")
	}
	if r.Profile.RequiresGPU && r.Resources.GPUCount == 0 && len(r.Profile.SupportedAccelerators) > 0 &&
		!contains(r.Profile.SupportedAccelerators, AcceleratorGPU) {
		problems = append(problems, "hardware_profile requires a GPU but lists no gpu accelerator")
	}
	if r.Replicas < 0 {
		problems = append(problems, "replicas must not be negative")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return nil
}

// Derive computes the placement constraints for a request.
func Derive(r Request) Constraints {
	e := r.Effective()
	c := Constraints{
		NodeSelector:     map[string]string{},
		Requests:         map[string]string{"cpu": milli(e.CPUMilli), "memory": mebi(e.MemoryMiB)},
		Limits:           map[string]string{"memory": mebi(int64(float64(e.MemoryMiB) * MemoryHeadroom))},
		SpreadByHostname: true,
	}
	if e.GPUCount > 0 {
		c.NodeSelector[LabelAccelerator] = AcceleratorGPU
		gpu := fmt.Sprintf("%d", e.GPUCount)
		c.Requests[ResourceGPU] = gpu
		c.Limits[ResourceGPU] = gpu
		// GPU pods get CPU limits too: a GPU node is expensive and a CPU-hungry
		// neighbour starving the feeding thread wastes it.
		c.Limits["cpu"] = milli(e.CPUMilli)
		c.RuntimeClassName = "nvidia"
		c.Tolerations = []Toleration{{Key: ResourceGPU, Operator: "Exists", Effect: "NoSchedule"}}
		product := r.Resources.GPUType
		if product == "" {
			product = r.Profile.GPUProduct
		}
		if product != "" {
			c.NodeSelector[LabelGPUProduct] = product
		}
	} else {
		// No CPU limit on CPU inference pods: throttling shows up as tail latency, and
		// the memory limit is the protection that matters
		// (docs/deployment-architecture.md §2.1).
		c.NodeSelector[LabelAccelerator] = AcceleratorCPU
	}
	return c
}

// ---------------------------------------------------------------------------
// capacity admission
// ---------------------------------------------------------------------------

// Node is one node's capacity as the inventory observed it.
type Node struct {
	Name        string
	Labels      map[string]string
	Schedulable bool
	// Allocatable is the node's allocatable capacity; Requested is the sum of
	// requests already placed there. Both in the units below.
	AllocatableCPUMilli  int64
	AllocatableMemoryMiB int64
	AllocatableGPU       int64
	RequestedCPUMilli    int64
	RequestedMemoryMiB   int64
	RequestedGPU         int64
	// NoScheduleTaints are the keys of NoSchedule/NoExecute taints on the node.
	NoScheduleTaints []string
}

// Free returns the headroom left on a node.
func (n Node) Free() (cpuMilli, memMiB, gpu int64) {
	return n.AllocatableCPUMilli - n.RequestedCPUMilli,
		n.AllocatableMemoryMiB - n.RequestedMemoryMiB,
		n.AllocatableGPU - n.RequestedGPU
}

// Decision is the admission answer.
type Decision struct {
	Admitted    bool        `json:"admitted"`
	Constraints Constraints `json:"constraints"`
	// FittingNodes is how many nodes could host at least one replica.
	FittingNodes int `json:"fitting_nodes"`
	// Placeable is how many replicas fit, packing greedily into current headroom.
	Placeable int `json:"placeable_replicas"`
	// Reason names the constraint that failed, when not admitted.
	Reason string `json:"reason,omitempty"`
	// CapacityNote is a human sentence for the response body.
	CapacityNote string `json:"capacity_note"`
	// Inventory says whether the answer is backed by observed nodes at all.
	Inventory string `json:"inventory"`
}

// Inventory states.
const (
	InventoryObserved = "observed"
	// InventoryUnavailable means no node has been observed. Admission cannot say
	// "no" without evidence, so it admits and says it could not check — the pods
	// will be Pending if it was wrong, and the deployment condition will say why.
	InventoryUnavailable = "unavailable"
)

// Admit decides whether a request can ever be placed. "Ever" is the operative
// word: a request no node could host even when empty is rejected, because it would
// sit Pending forever. A request that fits the nodes' size but not their current
// headroom is admitted with a note, because headroom changes: pods finish, the
// autoscaler scales things down, an operator adds a node.
func Admit(r Request, nodes []Node) Decision {
	c := Derive(r)
	e := r.Effective()
	d := Decision{Constraints: c, Inventory: InventoryObserved}

	if len(nodes) == 0 {
		d.Admitted = true
		d.Inventory = InventoryUnavailable
		d.CapacityNote = "no nodes have been observed yet, so capacity could not be checked; " +
			"if the cluster cannot host this deployment its pods will stay Pending and the deployment will say so"
		return d
	}

	var eligible []Node
	selectorMisses, taintMisses, unschedulable := 0, 0, 0
	for _, n := range nodes {
		switch {
		case !n.Schedulable:
			unschedulable++
		case !matches(n.Labels, c.NodeSelector):
			selectorMisses++
		case !tolerated(n.NoScheduleTaints, c.Tolerations):
			taintMisses++
		default:
			eligible = append(eligible, n)
		}
	}
	if len(eligible) == 0 {
		d.Reason = "node_selector"
		d.CapacityNote = fmt.Sprintf("no schedulable node matches %s (%d nodes observed: %d with other labels, %d tainted, %d cordoned)",
			selectorString(c.NodeSelector), len(nodes), selectorMisses, taintMisses, unschedulable)
		return d
	}

	// Could one replica fit on an EMPTY eligible node? If not, never.
	largest := false
	for _, n := range eligible {
		if n.AllocatableCPUMilli >= e.CPUMilli && n.AllocatableMemoryMiB >= e.MemoryMiB && n.AllocatableGPU >= e.GPUCount {
			largest = true
			break
		}
	}
	if !largest {
		maxCPU, maxMem, maxGPU := int64(0), int64(0), int64(0)
		for _, n := range eligible {
			maxCPU, maxMem, maxGPU = max(maxCPU, n.AllocatableCPUMilli), max(maxMem, n.AllocatableMemoryMiB), max(maxGPU, n.AllocatableGPU)
		}
		switch {
		case e.MemoryMiB > maxMem:
			d.Reason = "memory"
		case e.CPUMilli > maxCPU:
			d.Reason = "cpu"
		default:
			d.Reason = "gpu"
		}
		d.CapacityNote = fmt.Sprintf("one replica needs %s CPU, %s memory and %d GPU, but the largest eligible node offers %s CPU, %s and %d GPU in total",
			milli(e.CPUMilli), mebi(e.MemoryMiB), e.GPUCount, milli(maxCPU), mebi(maxMem), maxGPU)
		return d
	}

	// Greedy packing into current headroom, largest free node first, for the note.
	type slot struct{ cpu, mem, gpu int64 }
	free := make([]slot, 0, len(eligible))
	var totalCPU, totalMem int64
	for _, n := range eligible {
		fc, fm, fg := n.Free()
		free = append(free, slot{fc, fm, fg})
		if fc > 0 {
			totalCPU += fc
		}
		if fm > 0 {
			totalMem += fm
		}
		if fc >= e.CPUMilli && fm >= e.MemoryMiB && fg >= e.GPUCount {
			d.FittingNodes++
		}
	}
	for placed := 0; placed < r.Replicas; placed++ {
		sort.Slice(free, func(i, j int) bool { return free[i].mem > free[j].mem })
		s := &free[0]
		if s.cpu < e.CPUMilli || s.mem < e.MemoryMiB || s.gpu < e.GPUCount {
			break
		}
		s.cpu -= e.CPUMilli
		s.mem -= e.MemoryMiB
		s.gpu -= e.GPUCount
		d.Placeable++
	}
	d.Admitted = true
	d.CapacityNote = fmt.Sprintf("%d of %d eligible nodes can host a replica now; %d of %d replicas fit current headroom (%s CPU, %s free)",
		d.FittingNodes, len(eligible), d.Placeable, r.Replicas, milli(totalCPU), mebi(totalMem))
	if d.Placeable < r.Replicas {
		d.CapacityNote += "; the rest will be Pending until capacity frees up"
	}
	return d
}

func matches(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func tolerated(taints []string, tols []Toleration) bool {
	for _, t := range taints {
		ok := false
		for _, tol := range tols {
			if tol.Key == t && tol.Operator == "Exists" {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func selectorString(sel map[string]string) string {
	keys := make([]string, 0, len(sel))
	for k := range sel {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+sel[k])
	}
	return strings.Join(parts, ",")
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// milli renders CPU as a Kubernetes quantity: whole cores when exact, else millis.
func milli(m int64) string {
	if m%1000 == 0 {
		return fmt.Sprintf("%d", m/1000)
	}
	return fmt.Sprintf("%dm", m)
}

// mebi renders memory as a Kubernetes quantity.
func mebi(m int64) string {
	if m%1024 == 0 {
		return fmt.Sprintf("%dGi", m/1024)
	}
	return fmt.Sprintf("%dMi", m)
}
