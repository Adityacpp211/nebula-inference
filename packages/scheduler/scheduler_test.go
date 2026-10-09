package scheduler_test

import (
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/scheduler"
)

func cpuNode(name string, cpu, memMiB int64) scheduler.Node {
	return scheduler.Node{
		Name: name, Schedulable: true,
		Labels:              map[string]string{scheduler.LabelAccelerator: "cpu"},
		AllocatableCPUMilli: cpu, AllocatableMemoryMiB: memMiB,
	}
}

func TestDeriveCPU(t *testing.T) {
	t.Parallel()
	c := scheduler.Derive(scheduler.Request{
		Profile:   scheduler.HardwareProfile{MinRAMMiB: 2048, MinCPUMilli: 1000},
		Resources: scheduler.Resources{CPUMilli: 4000, MemoryMiB: 8192},
	})
	if c.NodeSelector[scheduler.LabelAccelerator] != "cpu" || c.Requests["cpu"] != "4" ||
		c.Requests["memory"] != "8Gi" || c.Limits["memory"] != "10Gi" {
		t.Errorf("constraints: %+v", c)
	}
	if _, ok := c.Limits["cpu"]; ok {
		t.Error("CPU inference pods must not get a CPU limit: throttling is tail latency")
	}
	if c.RuntimeClassName != "" || len(c.Tolerations) != 0 || !c.SpreadByHostname {
		t.Errorf("cpu extras: %+v", c)
	}
}

// A deployment cannot ask for less than the model needs to load.
func TestEffectiveRaisesToTheModelMinimum(t *testing.T) {
	t.Parallel()
	c := scheduler.Derive(scheduler.Request{
		Profile:   scheduler.HardwareProfile{MinRAMMiB: 6144, MinCPUMilli: 2000},
		Resources: scheduler.Resources{CPUMilli: 500, MemoryMiB: 1024},
	})
	if c.Requests["cpu"] != "2" || c.Requests["memory"] != "6Gi" {
		t.Errorf("requests must be raised to the model minimum: %+v", c.Requests)
	}
}

func TestDeriveGPU(t *testing.T) {
	t.Parallel()
	c := scheduler.Derive(scheduler.Request{
		Profile:   scheduler.HardwareProfile{RequiresGPU: true, GPUProduct: "NVIDIA-A100-SXM4-40GB"},
		Resources: scheduler.Resources{CPUMilli: 8000, MemoryMiB: 32768},
	})
	if c.NodeSelector[scheduler.LabelAccelerator] != "gpu" || c.NodeSelector[scheduler.LabelGPUProduct] != "NVIDIA-A100-SXM4-40GB" ||
		c.Requests[scheduler.ResourceGPU] != "1" || c.Limits[scheduler.ResourceGPU] != "1" || c.Limits["cpu"] != "8" ||
		c.RuntimeClassName != "nvidia" || len(c.Tolerations) != 1 {
		t.Errorf("gpu constraints: %+v", c)
	}
}

func TestAdmit(t *testing.T) {
	t.Parallel()
	two := []scheduler.Node{cpuNode("w1", 4000, 8192), cpuNode("w2", 4000, 8192)}
	cases := []struct {
		name      string
		req       scheduler.Request
		nodes     []scheduler.Node
		admitted  bool
		reason    string
		placeable int
		note      string
	}{
		{"fits both", scheduler.Request{Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 2048}, Replicas: 2},
			two, true, "", 2, "2 of 2 eligible nodes"},
		{"fits size, not headroom", scheduler.Request{Resources: scheduler.Resources{CPUMilli: 3000, MemoryMiB: 6144}, Replicas: 3},
			two, true, "", 2, "the rest will be Pending"},
		{"too much memory, ever", scheduler.Request{Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 16384}, Replicas: 1},
			two, false, "memory", 0, "largest eligible node"},
		{"too much cpu, ever", scheduler.Request{Resources: scheduler.Resources{CPUMilli: 8000, MemoryMiB: 1024}, Replicas: 1},
			two, false, "cpu", 0, "largest eligible node"},
		{"gpu on a cpu cluster", scheduler.Request{Profile: scheduler.HardwareProfile{RequiresGPU: true},
			Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 1024}, Replicas: 1},
			two, false, "node_selector", 0, "nebula.dev/accelerator=gpu"},
		{"no inventory yet", scheduler.Request{Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 1024}, Replicas: 1},
			nil, true, "", 0, "could not be checked"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := scheduler.Admit(tc.req, tc.nodes)
			if d.Admitted != tc.admitted || d.Reason != tc.reason || d.Placeable != tc.placeable ||
				!strings.Contains(d.CapacityNote, tc.note) {
				t.Errorf("got %+v", d)
			}
		})
	}
}

// Cordoned and tainted nodes do not count, and the refusal says so.
func TestAdmitIgnoresCordonedAndTaintedNodes(t *testing.T) {
	t.Parallel()
	cordoned := cpuNode("w1", 8000, 16384)
	cordoned.Schedulable = false
	tainted := cpuNode("w2", 8000, 16384)
	tainted.NoScheduleTaints = []string{"dedicated"}
	d := scheduler.Admit(scheduler.Request{Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 1024}, Replicas: 1},
		[]scheduler.Node{cordoned, tainted})
	if d.Admitted || !strings.Contains(d.CapacityNote, "1 tainted, 1 cordoned") {
		t.Errorf("got %+v", d)
	}
}

// Current usage reduces headroom but never turns "fits" into "never fits".
func TestAdmitCountsExistingRequests(t *testing.T) {
	t.Parallel()
	busy := cpuNode("w1", 4000, 8192)
	busy.RequestedCPUMilli, busy.RequestedMemoryMiB = 3500, 7000
	d := scheduler.Admit(scheduler.Request{Resources: scheduler.Resources{CPUMilli: 1000, MemoryMiB: 2048}, Replicas: 1},
		[]scheduler.Node{busy})
	if !d.Admitted || d.Placeable != 0 || d.FittingNodes != 0 {
		t.Errorf("a full node admits (headroom changes) but places nothing now: %+v", d)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	if err := (scheduler.Request{Replicas: 1}).Validate(); err == nil || !strings.Contains(err.Error(), "cpu_milli") {
		t.Errorf("empty resources must be refused: %v", err)
	}
	ok := scheduler.Request{Profile: scheduler.HardwareProfile{MinCPUMilli: 500, MinRAMMiB: 512}, Replicas: 1}
	if err := ok.Validate(); err != nil {
		t.Errorf("model minimums satisfy an empty request: %v", err)
	}
}

func TestParse(t *testing.T) {
	t.Parallel()
	p, err := scheduler.ParseProfile([]byte(`{"requires_gpu":false,"min_ram_mib":2048,"future_field":1}`))
	if err != nil || p.MinRAMMiB != 2048 {
		t.Errorf("unknown fields must be tolerated: %+v %v", p, err)
	}
	r, err := scheduler.ParseResources([]byte(`{"cpu_milli":1500,"memory_mib":3000}`))
	if err != nil || r.CPUMilli != 1500 {
		t.Errorf("%+v %v", r, err)
	}
	c := scheduler.Derive(scheduler.Request{Resources: r})
	if c.Requests["cpu"] != "1500m" || c.Requests["memory"] != "3000Mi" {
		t.Errorf("quantities: %+v", c.Requests)
	}
}
