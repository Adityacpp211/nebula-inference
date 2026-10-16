package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// UsageRepo reads the hourly usage rollups and manages pricing profiles. The
// rollups are written by the controller's usage ingester; the control plane only
// reads them (docs/architecture.md §9).
type UsageRepo struct{}

// PricingProfile is one immutable version.
type PricingProfile struct {
	ID                  uuid.UUID       `json:"id"`
	OrgID               *uuid.UUID      `json:"org_id"`
	Name                string          `json:"name"`
	Version             int32           `json:"version"`
	Currency            string          `json:"currency"`
	CPUCoreHourMicros   int64           `json:"cpu_core_hour_micros"`
	MemoryGiBHourMicros int64           `json:"memory_gib_hour_micros"`
	GPUHourMicros       json.RawMessage `json:"gpu_hour_micros"`
	MarkupBasisPoints   int32           `json:"markup_basis_points"`
	EffectiveFrom       time.Time       `json:"effective_from"`
	CreatedAt           time.Time       `json:"created_at"`
	InUse               bool            `json:"in_use"`
}

const pricingColumns = `p.id, p.org_id, p.name, p.version, p.currency, p.cpu_core_hour_micros,
	p.memory_gib_hour_micros, p.gpu_hour_micros, p.markup_basis_points, p.effective_from, p.created_at`

// Profiles lists the pricing profiles an organization can see — its own and the
// global ones — every version, newest first, marking the family it is priced by.
func (r *UsageRepo) Profiles(ctx context.Context, q Querier, orgID uuid.UUID) ([]PricingProfile, error) {
	rows, err := q.Query(ctx, `
		WITH current AS (
		    SELECT p.org_id, p.name FROM organizations o JOIN pricing_profiles p ON p.id = o.pricing_profile_id
		     WHERE o.id = $1)
		SELECT `+pricingColumns+`,
		       coalesce((SELECT p.org_id IS NOT DISTINCT FROM c.org_id AND p.name = c.name FROM current c),
		                p.org_id IS NULL AND p.name = 'default')
		  FROM pricing_profiles p
		 WHERE p.org_id = $1 OR p.org_id IS NULL
		 ORDER BY p.org_id NULLS LAST, p.name, p.version DESC`, orgID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []PricingProfile
	for rows.Next() {
		var p PricingProfile
		if err := rows.Scan(&p.ID, &p.OrgID, &p.Name, &p.Version, &p.Currency, &p.CPUCoreHourMicros,
			&p.MemoryGiBHourMicros, &p.GPUHourMicros, &p.MarkupBasisPoints, &p.EffectiveFrom, &p.CreatedAt,
			&p.InUse); err != nil {
			return nil, classify(err)
		}
		out = append(out, p)
	}
	return out, classify(rows.Err())
}

// CreateProfile adds the next version of an organization's profile. Versions are
// never updated: a price change is a new row, so a cost already computed can
// always be recomputed under the version it used (ADR-0018). With use, the
// organization is priced by this profile family from its effective time.
func (r *UsageRepo) CreateProfile(ctx context.Context, q Querier, p *PricingProfile, use bool) error {
	err := q.QueryRow(ctx, `
		INSERT INTO pricing_profiles (id, org_id, name, version, currency, cpu_core_hour_micros,
		    memory_gib_hour_micros, gpu_hour_micros, markup_basis_points, effective_from)
		SELECT $1, $2, $3, coalesce(max(version), 0) + 1, $4, $5, $6, $7, $8, $9
		  FROM pricing_profiles WHERE org_id = $2 AND name = $3
		RETURNING version, created_at`,
		p.ID, p.OrgID, p.Name, p.Currency, p.CPUCoreHourMicros, p.MemoryGiBHourMicros, p.GPUHourMicros,
		p.MarkupBasisPoints, p.EffectiveFrom).Scan(&p.Version, &p.CreatedAt)
	if err != nil {
		return classify(err)
	}
	if use {
		if _, err := q.Exec(ctx, `UPDATE organizations SET pricing_profile_id = $2 WHERE id = $1`, p.OrgID, p.ID); err != nil {
			return classify(err)
		}
		p.InUse = true
	}
	return nil
}

// UsageRow is one group of the rollups over a range.
type UsageRow struct {
	Key              string     `json:"key,omitempty"`
	DeploymentID     *uuid.UUID `json:"deployment_id,omitempty"`
	Deployment       *string    `json:"deployment,omitempty"`
	APIKeyID         *uuid.UUID `json:"api_key_id,omitempty"`
	Hour             *time.Time `json:"hour,omitempty"`
	Requests         int64      `json:"requests"`
	Errors           int64      `json:"errors"`
	PromptTokens     int64      `json:"prompt_tokens"`
	CompletionTokens int64      `json:"completion_tokens"`
	ComputeSeconds   float64    `json:"compute_seconds"`
	CostMicros       int64      `json:"estimated_cost_micros"`
	MinSampleRate    float64    `json:"-"`
}

// Usage groups an organization's rollups in [from, to) by deployment, API key,
// hour, or not at all.
func (r *UsageRepo) Usage(ctx context.Context, q Querier, orgID uuid.UUID, from, to time.Time, groupBy string) ([]UsageRow, error) {
	var key, sel string
	switch groupBy {
	case "deployment":
		key, sel = "u.deployment_id", "u.deployment_id, max(d.name), NULL::uuid, NULL::timestamptz"
	case "api_key":
		key, sel = "u.api_key_id", "NULL::uuid, NULL::text, u.api_key_id, NULL::timestamptz"
	case "hour":
		key, sel = "u.bucket_hour", "NULL::uuid, NULL::text, NULL::uuid, u.bucket_hour"
	default:
		key, sel = "1", "NULL::uuid, NULL::text, NULL::uuid, NULL::timestamptz"
	}
	rows, err := q.Query(ctx, `
		SELECT `+sel+`, sum(u.request_count), sum(u.error_count), sum(u.prompt_tokens),
		       sum(u.completion_tokens), sum(u.compute_seconds)::float8, sum(u.estimated_cost_micros),
		       min(u.sample_rate)
		  FROM usage_records u LEFT JOIN deployments d ON d.id = u.deployment_id
		 WHERE u.org_id = $1 AND u.bucket_hour >= $2 AND u.bucket_hour < $3
		 GROUP BY `+key+`
		 ORDER BY sum(u.estimated_cost_micros) DESC, sum(u.request_count) DESC`, orgID, from, to)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []UsageRow
	for rows.Next() {
		var u UsageRow
		if err := rows.Scan(&u.DeploymentID, &u.Deployment, &u.APIKeyID, &u.Hour, &u.Requests, &u.Errors,
			&u.PromptTokens, &u.CompletionTokens, &u.ComputeSeconds, &u.CostMicros, &u.MinSampleRate); err != nil {
			return nil, classify(err)
		}
		out = append(out, u)
	}
	return out, classify(rows.Err())
}

// ProfilesUsed lists the pricing profile versions the rollups in range were
// priced under.
func (r *UsageRepo) ProfilesUsed(ctx context.Context, q Querier, orgID uuid.UUID, from, to time.Time) ([]PricingProfile, error) {
	rows, err := q.Query(ctx, `
		SELECT `+pricingColumns+`, false
		  FROM pricing_profiles p
		 WHERE p.id IN (SELECT DISTINCT pricing_profile_id FROM usage_records
		                 WHERE org_id = $1 AND bucket_hour >= $2 AND bucket_hour < $3)
		 ORDER BY p.effective_from`, orgID, from, to)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []PricingProfile
	for rows.Next() {
		var p PricingProfile
		if err := rows.Scan(&p.ID, &p.OrgID, &p.Name, &p.Version, &p.Currency, &p.CPUCoreHourMicros,
			&p.MemoryGiBHourMicros, &p.GPUHourMicros, &p.MarkupBasisPoints, &p.EffectiveFrom, &p.CreatedAt,
			&p.InUse); err != nil {
			return nil, classify(err)
		}
		out = append(out, p)
	}
	return out, classify(rows.Err())
}
