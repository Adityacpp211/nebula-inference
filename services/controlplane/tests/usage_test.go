package controlplane_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

type profileBody struct {
	ID      uuid.UUID `json:"id"`
	Name    string    `json:"name"`
	Version int32     `json:"version"`
	InUse   bool      `json:"in_use"`
	OrgID   *string   `json:"org_id"`
}

// Pricing profiles are versioned, never edited: each create is the next version,
// and with "use" the organization is priced by that family.
func TestPricingProfilesAreVersioned(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	body := map[string]any{"name": "standard", "cpu_core_hour_micros": 36000, "memory_gib_hour_micros": 7200,
		"gpu_hour_micros": map[string]any{"nvidia-l4": 720000}}
	var v1, v2 profileBody
	if code := f.do(t, "POST", "/v1/pricing-profiles", body, &v1); code != http.StatusCreated || v1.Version != 1 || !v1.InUse {
		t.Fatalf("v1: %d %+v", code, v1)
	}
	body["cpu_core_hour_micros"] = 40000
	if code := f.do(t, "POST", "/v1/pricing-profiles", body, &v2); code != http.StatusCreated || v2.Version != 2 {
		t.Fatalf("v2: %d %+v", code, v2)
	}
	var list struct {
		Data []profileBody `json:"data"`
	}
	if code := f.do(t, "GET", "/v1/pricing-profiles", nil, &list); code != 200 {
		t.Fatalf("list: %d", code)
	}
	ours, global := 0, 0
	for _, p := range list.Data {
		if p.OrgID == nil {
			global++
			if p.InUse {
				t.Error("the global default is not in use once the org has its own profile")
			}
			continue
		}
		ours++
		if !p.InUse {
			t.Errorf("%s v%d should be marked in use", p.Name, p.Version)
		}
	}
	if ours != 2 || global < 1 {
		t.Fatalf("profiles: %d ours, %d global", ours, global)
	}

	for name, bad := range map[string]map[string]any{
		"negative": {"name": "x", "cpu_core_hour_micros": -1, "memory_gib_hour_micros": 0},
		"missing":  {"name": "x", "cpu_core_hour_micros": 1},
		"currency": {"name": "x", "cpu_core_hour_micros": 1, "memory_gib_hour_micros": 1, "currency": "dollars"},
		"gpu":      {"name": "x", "cpu_core_hour_micros": 1, "memory_gib_hour_micros": 1, "gpu_hour_micros": map[string]any{"a": -5}},
	} {
		if env := f.expectError(t, http.StatusBadRequest, "POST", "/v1/pricing-profiles", bad); env.Error.Code == "" {
			t.Errorf("%s: no code", name)
		}
	}
}

// Usage and costs read the rollups, group them, total them, and say what they
// assume.
func TestUsageAndCosts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "metered", "v1")
	dep := f.createDeployment(t, "metered", v)
	ctx := dbtest.Context(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	for i, row := range []struct {
		at                         time.Time
		requests, errors, p, c, mc int64
	}{
		{hour, 100, 2, 1000, 3000, 500},
		{hour.Add(-time.Hour), 50, 0, 500, 1500, 250},
		{hour.Add(-48 * time.Hour), 999, 0, 1, 1, 99999}, // outside the default range
	} {
		if _, err := f.Pool.Exec(ctx, `
			INSERT INTO usage_records (id, org_id, bucket_hour, deployment_id, request_count, error_count,
			    prompt_tokens, completion_tokens, compute_seconds, pricing_profile_id, estimated_cost_micros)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, '00000000-0000-7000-8000-000000000001', $9)`,
			f.OrgID, row.at, dep, row.requests, row.errors, row.p, row.c, float64(i+1), row.mc); err != nil {
			t.Fatal(err)
		}
	}

	var usage struct {
		GroupBy string `json:"group_by"`
		Data    []struct {
			Deployment string `json:"deployment"`
			Requests   int64  `json:"requests"`
		} `json:"data"`
		Totals struct {
			Requests         int64 `json:"requests"`
			Errors           int64 `json:"errors"`
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			CostMicros       int64 `json:"estimated_cost_micros"`
		} `json:"totals"`
	}
	if code := f.do(t, "GET", "/v1/usage", nil, &usage); code != 200 {
		t.Fatalf("usage: %d", code)
	}
	if usage.GroupBy != "deployment" || len(usage.Data) != 1 || usage.Data[0].Deployment != "metered" ||
		usage.Totals.Requests != 150 || usage.Totals.Errors != 2 || usage.Totals.PromptTokens != 1500 ||
		usage.Totals.CompletionTokens != 4500 || usage.Totals.CostMicros != 750 {
		t.Fatalf("usage: %+v", usage)
	}
	var byHour struct {
		Data []any `json:"data"`
	}
	if code := f.do(t, "GET", "/v1/usage?group_by=hour", nil, &byHour); code != 200 || len(byHour.Data) != 2 {
		t.Fatalf("by hour: %d %d", code, len(byHour.Data))
	}
	from := hour.Add(-72 * time.Hour).Format(time.RFC3339)
	if code := f.do(t, "GET", "/v1/usage?from="+from, nil, &usage); code != 200 || usage.Totals.Requests != 1149 {
		t.Fatalf("wider range: %d %d", code, usage.Totals.Requests)
	}

	var costs struct {
		Currency    string   `json:"currency"`
		TotalMicros int64    `json:"total_micros"`
		Assumptions []string `json:"assumptions"`
		Profiles    []struct {
			Name    string `json:"name"`
			Version int32  `json:"version"`
		} `json:"pricing_profiles"`
	}
	if code := f.do(t, "GET", "/v1/costs", nil, &costs); code != 200 {
		t.Fatalf("costs: %d", code)
	}
	if costs.TotalMicros != 750 || costs.Currency != "USD" || len(costs.Assumptions) < 4 ||
		len(costs.Profiles) != 1 || costs.Profiles[0].Name != "default" {
		t.Fatalf("costs: %+v", costs)
	}

	for _, q := range []string{"?group_by=model", "?from=yesterday", "?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		if code := f.do(t, "GET", "/v1/usage"+q, nil, nil); code != http.StatusBadRequest {
			t.Errorf("%s: %d", q, code)
		}
	}
}
