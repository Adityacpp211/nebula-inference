package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/scheduler"
)

// inventoryMaxAge is how old the node cache may be and still count as evidence.
// Older than this, admission reports the inventory as unavailable rather than
// refusing a deployment on stale numbers.
const inventoryMaxAge = 5 * time.Minute

// schedulerNodes converts the node cache into the scheduler's input.
func schedulerNodes(rows []*models.Node, now time.Time) []scheduler.Node {
	type amounts struct {
		CPUMilli  int64 `json:"cpu_milli"`
		MemoryMiB int64 `json:"memory_mib"`
		GPU       int64 `json:"gpu"`
	}
	out := make([]scheduler.Node, 0, len(rows))
	for _, r := range rows {
		if r.Stale(now, inventoryMaxAge) {
			continue
		}
		var alloc, req amounts
		_ = json.Unmarshal(r.Allocatable, &alloc)
		_ = json.Unmarshal(r.Requested, &req)
		labels := map[string]string{}
		_ = json.Unmarshal(r.Labels, &labels)
		var taints []struct {
			Key    string `json:"key"`
			Effect string `json:"effect"`
		}
		_ = json.Unmarshal(r.Taints, &taints)
		n := scheduler.Node{
			Name: r.Name, Labels: labels, Schedulable: r.Schedulable,
			AllocatableCPUMilli: alloc.CPUMilli, AllocatableMemoryMiB: alloc.MemoryMiB, AllocatableGPU: alloc.GPU,
			RequestedCPUMilli: req.CPUMilli, RequestedMemoryMiB: req.MemoryMiB, RequestedGPU: req.GPU,
		}
		for _, t := range taints {
			if t.Effect == "NoSchedule" || t.Effect == "NoExecute" {
				n.NoScheduleTaints = append(n.NoScheduleTaints, t.Key)
			}
		}
		out = append(out, n)
	}
	return out
}

// admit runs capacity admission, returning the decision or a 422 explaining which
// constraint no node can meet (docs/api.md §4: never a deployment that sits Pending
// forever with no explanation).
func admit(version *models.ModelVersion, resources json.RawMessage, replicas int32,
	nodes []*models.Node, now time.Time,
) (scheduler.Decision, error) {
	profile, err := scheduler.ParseProfile(version.HardwareProfile)
	if err != nil {
		return scheduler.Decision{}, httpx.ErrInternal(err)
	}
	res, err := scheduler.ParseResources(resources)
	if err != nil {
		return scheduler.Decision{}, httpx.ErrInvalidRequest(
			"resources must be {\"cpu_milli\": int, \"memory_mib\": int, \"gpu_count\": int}", "invalid_resources", "resources")
	}
	req := scheduler.Request{Profile: profile, Resources: res, Replicas: int(replicas)}
	if err := req.Validate(); err != nil {
		return scheduler.Decision{}, httpx.ErrInvalidRequest(err.Error(), "invalid_resources", "resources")
	}
	d := scheduler.Admit(req, schedulerNodes(nodes, now))
	if !d.Admitted {
		return d, &httpx.APIError{
			Status:  http.StatusUnprocessableEntity,
			Type:    httpx.TypeUnprocessable,
			Code:    "insufficient_capacity",
			Param:   "resources",
			Message: fmt.Sprintf("no node can ever host this deployment (%s): %s", d.Reason, d.CapacityNote),
		}
	}
	return d, nil
}
