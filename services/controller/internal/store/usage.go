package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/costing"
	"github.com/adityasatwar321/nebula/packages/db"
)

// UsageRow is one request as the usage ingester stores it.
type UsageRow struct {
	ID               uuid.UUID
	OrgID            uuid.UUID
	CreatedAt        time.Time
	RouteID          *uuid.UUID
	DeploymentID     *uuid.UUID
	ModelVersion     string // "model:version", resolved to an id here
	APIKeyID         *uuid.UUID
	Variant          *string
	Endpoint         string
	Priority         string
	StatusCode       int
	ErrorClass       *string
	Outcome          string
	Streamed         bool
	QueueWaitMS      int64
	TTFTMS           *int64
	DurationMS       int64
	ComputeMS        int64
	Attempts         int
	PromptTokens     int64
	CompletionTokens int64
	TraceID          *string
	SampleRate       float64
}

// IngestResult is what one batch changed.
type IngestResult struct {
	// Inserted counts new rows; the rest were duplicates of rows already stored.
	Inserted int
	// CostByDeployment is the estimated cost of the new rows, for the cost metric.
	CostByDeployment map[[2]string]int64 // org id, deployment id
	// Buckets is how many hourly rollups were recomputed.
	Buckets int
}

type deploymentCost struct {
	resources costing.Resources
	slots     int64
}

// IngestUsage stores a batch of requests, each with its estimated cost under the
// pricing profile version in force when it ran, and recomputes the hourly
// rollups the batch touched — all in one transaction.
//
// Idempotent under redelivery: a request already stored is skipped
// (ON CONFLICT DO NOTHING on its id and time), and a rollup is recomputed from the
// stored requests rather than incremented, so running a batch twice changes
// nothing (docs/events.md §4.1).
func (s *Store) IngestUsage(ctx context.Context, rows []UsageRow) (IngestResult, error) {
	res := IngestResult{CostByDeployment: map[[2]string]int64{}}
	err := db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		deps := map[uuid.UUID]*deploymentCost{}
		versions := map[[2]string]*uuid.UUID{}
		families := map[uuid.UUID][]costing.Profile{} // org → its profile family, oldest first
		// A request older than its organization's first version — a late record, or
		// one from before the organization chose a profile — is priced by the global
		// default in force then, rather than holding up the batch forever.
		fallback, err := defaultFamily(ctx, tx)
		if err != nil {
			return err
		}
		priceAt := func(org uuid.UUID, at time.Time) *costing.Profile {
			if p := versionAt(families[org], at); p != nil {
				return p
			}
			return versionAt(fallback, at)
		}
		buckets := map[[2]string]bool{} // org, hour

		for _, r := range rows {
			var dc *deploymentCost
			if r.DeploymentID != nil {
				var err error
				if dc, err = s.deploymentCost(ctx, tx, deps, *r.DeploymentID); err != nil {
					return err
				}
			}
			hour := r.CreatedAt.UTC().Truncate(time.Hour)
			pkey := [2]string{r.OrgID.String(), hour.Format(time.RFC3339)}
			if _, ok := families[r.OrgID]; !ok {
				family, err := profileFamily(ctx, tx, r.OrgID)
				if err != nil {
					return err
				}
				families[r.OrgID] = family
			}
			p := priceAt(r.OrgID, r.CreatedAt)
			if p == nil {
				return fmt.Errorf("no pricing profile version is in force for %s at %s", r.OrgID, r.CreatedAt)
			}
			var cost int64
			if dc != nil {
				cost = costing.RequestCostMicros(*p, dc.resources, dc.slots, r.ComputeMS)
			}
			vkey := [2]string{r.OrgID.String(), r.ModelVersion}
			mv, ok := versions[vkey]
			if !ok && r.ModelVersion != "" {
				var id uuid.UUID
				err := tx.QueryRow(ctx, `
					SELECT mv.id FROM model_versions mv JOIN models m ON m.id = mv.model_id
					 WHERE m.org_id = $1 AND m.name || ':' || mv.version = $2`, r.OrgID, r.ModelVersion).Scan(&id)
				if err == nil {
					mv = &id
				} else if !errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("model version: %w", err)
				}
				versions[vkey] = mv
			}

			tag, err := tx.Exec(ctx, `
				INSERT INTO requests (id, org_id, created_at, route_id, deployment_id, model_version_id, api_key_id,
				    variant, endpoint, priority, status_code, error_class, outcome, streamed, queue_wait_ms, ttft_ms,
				    duration_ms, compute_ms, attempts, prompt_tokens, completion_tokens, trace_id, sample_rate,
				    estimated_cost_micros)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
				    $21, $22, $23, $24)
				ON CONFLICT (id, created_at) DO NOTHING`,
				r.ID, r.OrgID, r.CreatedAt, r.RouteID, r.DeploymentID, mv, r.APIKeyID, r.Variant, r.Endpoint,
				r.Priority, r.StatusCode, r.ErrorClass, r.Outcome, r.Streamed, r.QueueWaitMS, r.TTFTMS, r.DurationMS,
				r.ComputeMS, r.Attempts, r.PromptTokens, r.CompletionTokens, r.TraceID, r.SampleRate, cost)
			if err != nil {
				return fmt.Errorf("request %s: %w", r.ID, err)
			}
			if tag.RowsAffected() == 1 {
				res.Inserted++
				dep := ""
				if r.DeploymentID != nil {
					dep = r.DeploymentID.String()
				}
				res.CostByDeployment[[2]string{r.OrgID.String(), dep}] += cost
			}
			buckets[pkey] = true
		}

		for key := range buckets {
			org, _ := uuid.Parse(key[0])
			hour, _ := time.Parse(time.RFC3339, key[1])
			// The bucket names the newest version any of its requests was priced
			// under: the one in force at its latest request. Each request keeps the
			// price it was computed at; the rollup only sums them.
			var latest time.Time
			if err := tx.QueryRow(ctx, `SELECT max(created_at) FROM requests
				 WHERE org_id = $1 AND created_at >= $2 AND created_at < $2 + interval '1 hour'`,
				org, hour).Scan(&latest); err != nil {
				return fmt.Errorf("rollup %s: %w", key[1], err)
			}
			p := priceAt(org, latest)
			if p == nil {
				return fmt.Errorf("no pricing profile version is in force for %s at %s", org, latest)
			}
			if err := rollup(ctx, tx, org, hour, p.ID); err != nil {
				return err
			}
			res.Buckets++
		}
		return nil
	})
	return res, err
}

// rollup recomputes one organization's hour from its stored requests.
func rollup(ctx context.Context, tx pgx.Tx, org uuid.UUID, hour time.Time, profile uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO usage_records (id, org_id, bucket_hour, deployment_id, model_version_id, api_key_id,
		    request_count, error_count, prompt_tokens, completion_tokens, compute_seconds, sample_rate,
		    pricing_profile_id, estimated_cost_micros, computed_at)
		SELECT gen_random_uuid(), org_id, $2, deployment_id,
		       CASE WHEN count(DISTINCT model_version_id) = 1 THEN (array_agg(model_version_id))[1] END,
		       api_key_id, count(*), count(*) FILTER (WHERE error_class IS NOT NULL),
		       coalesce(sum(prompt_tokens), 0), coalesce(sum(completion_tokens), 0),
		       coalesce(sum(compute_ms), 0) / 1000.0, min(sample_rate), $3,
		       coalesce(sum(estimated_cost_micros), 0), now()
		  FROM requests
		 WHERE org_id = $1 AND created_at >= $2 AND created_at < $2 + interval '1 hour'
		 GROUP BY org_id, deployment_id, api_key_id
		ON CONFLICT ON CONSTRAINT uq_usage_records__bucket DO UPDATE SET
		    model_version_id = EXCLUDED.model_version_id, request_count = EXCLUDED.request_count,
		    error_count = EXCLUDED.error_count, prompt_tokens = EXCLUDED.prompt_tokens,
		    completion_tokens = EXCLUDED.completion_tokens, compute_seconds = EXCLUDED.compute_seconds,
		    sample_rate = EXCLUDED.sample_rate, pricing_profile_id = EXCLUDED.pricing_profile_id,
		    estimated_cost_micros = EXCLUDED.estimated_cost_micros, computed_at = now()`, org, hour, profile)
	if err != nil {
		return fmt.Errorf("rollup %s %s: %w", org, hour.Format(time.RFC3339), err)
	}
	return nil
}

// deploymentCost reads what one replica requests and how many requests it runs
// at once. A deleted deployment's row remains, so late usage still prices.
func (s *Store) deploymentCost(ctx context.Context, tx pgx.Tx, cache map[uuid.UUID]*deploymentCost, id uuid.UUID) (*deploymentCost, error) {
	if dc, ok := cache[id]; ok {
		return dc, nil
	}
	var resources, overrides, versionConfig json.RawMessage
	err := tx.QueryRow(ctx, `
		SELECT d.resources, d.runtime_overrides, mv.runtime_config
		  FROM deployments d JOIN model_versions mv ON mv.id = d.model_version_id
		 WHERE d.id = $1`, id).Scan(&resources, &overrides, &versionConfig)
	if errors.Is(err, pgx.ErrNoRows) {
		cache[id] = nil
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("deployment %s: %w", id, err)
	}
	// Resources the costing model cannot read price the request at zero rather
	// than fail its batch: the request still counts, its cost is unknown.
	var dc *deploymentCost
	if r, perr := costing.ParseResources(resources); perr == nil {
		dc = &deploymentCost{resources: r, slots: slotsOf(overrides, versionConfig)}
	}
	cache[id] = dc
	return dc, nil
}

// slotsOf is a replica's parallel slots, from the deployment's runtime overrides
// or the version's runtime config: the mock's parallel_slots, llama.cpp's
// n_parallel. One when neither says.
func slotsOf(configs ...json.RawMessage) int64 {
	for _, raw := range configs {
		var c struct {
			ParallelSlots int64 `json:"parallel_slots"`
			NParallel     int64 `json:"n_parallel"`
		}
		if json.Unmarshal(raw, &c) != nil {
			continue
		}
		if c.ParallelSlots > 0 {
			return c.ParallelSlots
		}
		if c.NParallel > 0 {
			return c.NParallel
		}
	}
	return 1
}

// profileFamily is every version of the profile an organization is priced by —
// the family of the version it points at, or the global default — in version
// order.
func profileFamily(ctx context.Context, tx pgx.Tx, org uuid.UUID) ([]costing.Profile, error) {
	rows, err := tx.Query(ctx, `
		WITH chosen AS (
		    SELECT op.org_id AS org_id, coalesce(op.name, 'default') AS name
		      FROM organizations o LEFT JOIN pricing_profiles op ON op.id = o.pricing_profile_id
		     WHERE o.id = $1)
		SELECT p.id, p.name, p.version, p.currency, p.cpu_core_hour_micros, p.memory_gib_hour_micros,
		       p.gpu_hour_micros, p.markup_basis_points, p.effective_from
		  FROM pricing_profiles p, chosen c
		 WHERE p.org_id IS NOT DISTINCT FROM c.org_id AND p.name = c.name
		 ORDER BY p.version`, org)
	if err != nil {
		return nil, fmt.Errorf("pricing profiles for %s: %w", org, err)
	}
	defer rows.Close()
	var out []costing.Profile
	for rows.Next() {
		var p costing.Profile
		var gpu json.RawMessage
		if err := rows.Scan(&p.ID, &p.Name, &p.Version, &p.Currency, &p.CPUCoreHourMicros, &p.MemoryGiBHourMicros,
			&gpu, &p.MarkupBasisPoints, &p.EffectiveFrom); err != nil {
			return nil, err
		}
		if p.GPUHourMicros, err = costing.ParseGPUPrices(gpu); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// defaultFamily is every version of the global default profile.
func defaultFamily(ctx context.Context, tx pgx.Tx) ([]costing.Profile, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, name, version, currency, cpu_core_hour_micros, memory_gib_hour_micros, gpu_hour_micros,
		       markup_basis_points, effective_from
		  FROM pricing_profiles WHERE org_id IS NULL AND name = 'default' ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("default pricing profile: %w", err)
	}
	defer rows.Close()
	var out []costing.Profile
	for rows.Next() {
		var p costing.Profile
		var gpu json.RawMessage
		if err := rows.Scan(&p.ID, &p.Name, &p.Version, &p.Currency, &p.CPUCoreHourMicros, &p.MemoryGiBHourMicros,
			&gpu, &p.MarkupBasisPoints, &p.EffectiveFrom); err != nil {
			return nil, err
		}
		if p.GPUHourMicros, err = costing.ParseGPUPrices(gpu); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// versionAt is the version in force at a time: the newest whose effective_from
// is not after it.
func versionAt(family []costing.Profile, at time.Time) *costing.Profile {
	var in *costing.Profile
	for i := range family {
		if !family[i].EffectiveFrom.After(at) && (in == nil || family[i].Version > in.Version) {
			in = &family[i]
		}
	}
	return in
}
