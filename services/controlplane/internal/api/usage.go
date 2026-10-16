package api

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/costing"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// Usage, costs and pricing (docs/architecture.md §9, ADR-0018). Everything here
// reads the hourly rollups the controller's usage ingester writes; ranges are
// therefore whole hours, and the newest hour is still filling.

type usageResponse struct {
	From    time.Time        `json:"from"`
	To      time.Time        `json:"to"`
	GroupBy string           `json:"group_by"`
	Data    []store.UsageRow `json:"data"`
	Totals  store.UsageRow   `json:"totals"`
	// Note says what the numbers are, so nobody mistakes a partial hour or a
	// sampled count for something else.
	Note string `json:"note"`
}

type costsResponse struct {
	From        time.Time              `json:"from"`
	To          time.Time              `json:"to"`
	Currency    string                 `json:"currency"`
	TotalMicros int64                  `json:"total_micros"`
	Data        []store.UsageRow       `json:"data"`
	Profiles    []store.PricingProfile `json:"pricing_profiles"`
	Assumptions []string               `json:"assumptions"`
}

// usageRange reads from/to (RFC 3339), defaulting to the last 24 hours, widened to
// whole hours because that is the rollups' resolution.
func usageRange(r *http.Request) (from, to time.Time, err error) {
	q := r.URL.Query()
	now := time.Now().UTC()
	to = now.Truncate(time.Hour).Add(time.Hour)
	from = to.Add(-24 * time.Hour)
	if t, err := timeParam(q.Get("from"), "from"); err != nil {
		return from, to, err
	} else if t != nil {
		from = t.UTC().Truncate(time.Hour)
	}
	if t, err := timeParam(q.Get("to"), "to"); err != nil {
		return from, to, err
	} else if t != nil {
		to = t.UTC()
		if !to.Equal(to.Truncate(time.Hour)) {
			to = to.Truncate(time.Hour).Add(time.Hour)
		}
	}
	if !from.Before(to) {
		return from, to, httpx.ErrInvalidRequest("from must be before to", "invalid_range", "from")
	}
	if to.Sub(from) > 366*24*time.Hour {
		return from, to, httpx.ErrInvalidRequest("a range may span at most 366 days", "invalid_range", "to")
	}
	return from, to, nil
}

func groupByParam(r *http.Request) (string, error) {
	g := r.URL.Query().Get("group_by")
	switch g {
	case "":
		return "deployment", nil
	case "deployment", "api_key", "hour", "none":
		return g, nil
	}
	return "", httpx.ErrInvalidRequest("group_by must be deployment, api_key, hour or none", "invalid_value", "group_by")
}

// correct scales sampled counts back to estimates of the whole (costing.Corrected).
func correct(rows []store.UsageRow) store.UsageRow {
	var t store.UsageRow
	for i := range rows {
		u := &rows[i]
		if u.MinSampleRate > 0 && u.MinSampleRate < 1 {
			u.Requests = costing.Corrected(u.Requests, u.MinSampleRate)
			u.Errors = costing.Corrected(u.Errors, u.MinSampleRate)
			u.PromptTokens = costing.Corrected(u.PromptTokens, u.MinSampleRate)
			u.CompletionTokens = costing.Corrected(u.CompletionTokens, u.MinSampleRate)
		}
		t.Requests += u.Requests
		t.Errors += u.Errors
		t.PromptTokens += u.PromptTokens
		t.CompletionTokens += u.CompletionTokens
		t.ComputeSeconds += u.ComputeSeconds
		t.CostMicros += u.CostMicros
	}
	return t
}

func (a *API) getUsage(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	from, to, err := usageRange(r)
	if err != nil {
		return err
	}
	groupBy, err := groupByParam(r)
	if err != nil {
		return err
	}
	rows, err := a.Store.Usage.Usage(r.Context(), a.Store.Pool(), ident.OrgID, from, to, groupBy)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	if rows == nil {
		rows = []store.UsageRow{}
	}
	totals := correct(rows)
	a.write(w, r, http.StatusOK, usageResponse{From: from, To: to, GroupBy: groupBy, Data: rows, Totals: totals,
		Note: "Hourly rollups of ingested usage records: the newest hour is still filling, and sampled counts are " +
			"scaled back by their sample rate."})
	return nil
}

func (a *API) getCosts(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	from, to, err := usageRange(r)
	if err != nil {
		return err
	}
	groupBy, err := groupByParam(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	rows, err := a.Store.Usage.Usage(ctx, a.Store.Pool(), ident.OrgID, from, to, groupBy)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	profiles, err := a.Store.Usage.ProfilesUsed(ctx, a.Store.Pool(), ident.OrgID, from, to)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	if rows == nil {
		rows = []store.UsageRow{}
	}
	if profiles == nil {
		profiles = []store.PricingProfile{}
	}
	totals := correct(rows)
	currency := "USD"
	if len(profiles) > 0 {
		currency = profiles[len(profiles)-1].Currency
	}
	a.write(w, r, http.StatusOK, costsResponse{From: from, To: to, Currency: currency, TotalMicros: totals.CostMicros,
		Data: rows, Profiles: profiles, Assumptions: costing.Assumptions()})
	return nil
}

func (a *API) listPricingProfiles(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	profiles, err := a.Store.Usage.Profiles(r.Context(), a.Store.Pool(), ident.OrgID)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	if profiles == nil {
		profiles = []store.PricingProfile{}
	}
	a.write(w, r, http.StatusOK, map[string]any{"data": profiles})
	return nil
}

type createPricingRequest struct {
	Name                string          `json:"name"`
	Currency            string          `json:"currency"`
	CPUCoreHourMicros   *int64          `json:"cpu_core_hour_micros"`
	MemoryGiBHourMicros *int64          `json:"memory_gib_hour_micros"`
	GPUHourMicros       json.RawMessage `json:"gpu_hour_micros"`
	MarkupBasisPoints   int32           `json:"markup_basis_points"`
	EffectiveFrom       *time.Time      `json:"effective_from"`
	// Use prices the organization by this profile family; default true.
	Use *bool `json:"use"`
}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// createPricingProfile adds the next version of a profile. A price change is a new
// version, never an edit: costs already computed keep the version they used.
func (a *API) createPricingProfile(w http.ResponseWriter, r *http.Request) error {
	var req createPricingRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.Name, "name", modelNamePattern,
		"name must be 1-64 lowercase letters, digits, dots, dashes or underscores"); err != nil {
		return err
	}
	if req.CPUCoreHourMicros == nil || req.MemoryGiBHourMicros == nil {
		return httpx.ErrInvalidRequest("cpu_core_hour_micros and memory_gib_hour_micros are required", "missing_field",
			"cpu_core_hour_micros")
	}
	if *req.CPUCoreHourMicros < 0 || *req.MemoryGiBHourMicros < 0 || req.MarkupBasisPoints < 0 {
		return httpx.ErrInvalidRequest("prices and markup must not be negative", "invalid_value", "cpu_core_hour_micros")
	}
	gpu, err := costing.ParseGPUPrices(req.GPUHourMicros)
	if err != nil {
		return httpx.ErrInvalidRequest(err.Error(), "invalid_value", "gpu_hour_micros")
	}
	gpuJSON, _ := json.Marshal(gpu)
	if req.Currency == "" {
		req.Currency = "USD"
	}
	if !currencyPattern.MatchString(req.Currency) {
		return httpx.ErrInvalidRequest("currency must be an ISO 4217 code, e.g. USD", "invalid_value", "currency")
	}
	effective := time.Now().UTC()
	if req.EffectiveFrom != nil {
		effective = req.EffectiveFrom.UTC()
	}
	use := req.Use == nil || *req.Use
	ident := auth.MustFromContext(r.Context())
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	org := ident.OrgID
	p := &store.PricingProfile{ID: id, OrgID: &org, Name: req.Name, Currency: req.Currency,
		CPUCoreHourMicros: *req.CPUCoreHourMicros, MemoryGiBHourMicros: *req.MemoryGiBHourMicros,
		GPUHourMicros: gpuJSON, MarkupBasisPoints: req.MarkupBasisPoints, EffectiveFrom: effective}
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if err := a.Store.Usage.CreateProfile(ctx, q, p, use); err != nil {
			return storeError(err, "pricing profile")
		}
		entry := auditFor(r, store.ActionPricingCreate, store.ResourcePricingProfile)
		entry.ResourceID = &id
		entry.After = p
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusCreated, p)
	return nil
}
