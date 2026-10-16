package api

import (
	"github.com/adityasatwar321/nebula/packages/scheduler"
)

// recordAdmission counts a capacity admission decision
// (nebula_capacity_admission_total, docs/observability.md §2.4).
func (a *API) recordAdmission(d scheduler.Decision, err error) {
	if a.Metrics == nil {
		return
	}
	result, reason := "admitted", "fits"
	switch {
	case err != nil && !d.Admitted && d.Reason != "":
		result, reason = "rejected", d.Reason
	case err != nil:
		result, reason = "rejected", "invalid_request"
	case d.Inventory == scheduler.InventoryUnavailable:
		reason = "inventory_unavailable"
	}
	a.Metrics.Counter("nebula_capacity_admission_total").WithLabelValues(result, reason).Inc()
}
