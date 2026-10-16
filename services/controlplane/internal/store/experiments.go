package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/abtest"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

// ExperimentRepo is A/B experiments: a fixed traffic split on a route, observed
// over a window and compared per arm. Assignment is not stored: it is the
// gateway's hash of the bucketing key (the request, or the OpenAI "user").
type ExperimentRepo struct{}

const experimentColumns = `id, org_id, route_id, name, hypothesis, bucketing, state, started_at, stopped_at,
	conclusion, created_at`

func scanExperiment(row interface{ Scan(...any) error }) (*models.Experiment, error) {
	var e models.Experiment
	if err := row.Scan(&e.ID, &e.OrgID, &e.RouteID, &e.Name, &e.Hypothesis, &e.Bucketing, &e.State, &e.StartedAt,
		&e.StoppedAt, &e.Conclusion, &e.CreatedAt); err != nil {
		return nil, classify(err)
	}
	return &e, nil
}

// Create starts an experiment, running from now. One running experiment per
// route is the database's partial unique index.
func (r *ExperimentRepo) Create(ctx context.Context, q Querier, e *models.Experiment) error {
	return classify(q.QueryRow(ctx, `
		INSERT INTO experiments (id, org_id, route_id, name, hypothesis, bucketing, state, started_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'running', now())
		RETURNING state, started_at, created_at`,
		e.ID, e.OrgID, e.RouteID, e.Name, e.Hypothesis, e.Bucketing).Scan(&e.State, &e.StartedAt, &e.CreatedAt))
}

// Get returns one of an org's experiments.
func (r *ExperimentRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.Experiment, error) {
	return scanExperiment(q.QueryRow(ctx, `SELECT `+experimentColumns+` FROM experiments WHERE id = $1 AND org_id = $2`,
		id, orgID))
}

// List pages through an org's experiments, newest first.
func (r *ExperimentRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, p Page) ([]*models.Experiment, error) {
	rows, err := q.Query(ctx, `SELECT `+experimentColumns+` FROM experiments
		 WHERE org_id = $1 AND ($2::uuid IS NULL OR id < $2) ORDER BY id DESC LIMIT $3`, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*models.Experiment
	for rows.Next() {
		e, err := scanExperiment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, classify(rows.Err())
}

// Stop ends a running experiment, recording its conclusion if one is given.
func (r *ExperimentRepo) Stop(ctx context.Context, q Querier, orgID, id uuid.UUID, conclusion *string) (*models.Experiment, error) {
	e, err := scanExperiment(q.QueryRow(ctx, `
		UPDATE experiments SET state = 'stopped', stopped_at = now(), conclusion = coalesce($3, conclusion)
		 WHERE id = $1 AND org_id = $2 AND state = 'running'
		RETURNING `+experimentColumns, id, orgID, conclusion))
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: only a running experiment can be stopped", ErrConflict)
	}
	return e, err
}

// failedRequest is what counts as an error in a comparison: anything the client
// saw fail — a 5xx, or a request that failed, broke mid-stream or ran out of time.
const failedRequest = `(status_code >= 500 OR outcome IN ('failed', 'stream_interrupted', 'deadline_exceeded'))`

// Arms are the deployments that served the route in [from, to), with exact
// totals and up to sampleSize requests each for the intervals. The sample is
// ordered by a hash of the request id, so the same window always yields the same
// sample.
func (r *ExperimentRepo) Arms(ctx context.Context, q Querier, routeID uuid.UUID, from, to time.Time, sampleSize int) ([]abtest.Arm, error) {
	rows, err := q.Query(ctx, `
		SELECT r.deployment_id, coalesce(d.name, ''), coalesce(t.label, ''), coalesce(t.is_baseline, false),
		       count(*), count(*) FILTER (WHERE `+failedRequest+`),
		       coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY r.duration_ms), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY r.duration_ms), 0),
		       coalesce(sum(r.completion_tokens), 0), coalesce(sum(r.compute_ms), 0),
		       coalesce(sum(r.estimated_cost_micros), 0)
		  FROM requests r
		  LEFT JOIN deployments d ON d.id = r.deployment_id
		  LEFT JOIN route_targets t ON t.route_id = r.route_id AND t.deployment_id = r.deployment_id
		 WHERE r.route_id = $1 AND r.created_at >= $2 AND r.created_at < $3 AND r.deployment_id IS NOT NULL
		 GROUP BY r.deployment_id, d.name, t.label, t.is_baseline
		 ORDER BY coalesce(t.is_baseline, false) DESC, count(*) DESC`, routeID, from, to)
	if err != nil {
		return nil, classify(err)
	}
	var arms []abtest.Arm
	for rows.Next() {
		var a abtest.Arm
		var dep uuid.UUID
		var baseline bool
		if err := rows.Scan(&dep, &a.Deployment, &a.Label, &baseline, &a.Requests, &a.Errors, &a.P50MS, &a.P95MS,
			&a.CompletionTokens, &a.ComputeMS, &a.CostMicros); err != nil {
			rows.Close()
			return nil, classify(err)
		}
		a.DeploymentID = dep.String()
		if a.Label == "" {
			a.Label = a.Deployment
		}
		arms = append(arms, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	for i := range arms {
		srows, err := q.Query(ctx, `
			SELECT duration_ms, `+failedRequest+`
			  FROM requests
			 WHERE route_id = $1 AND deployment_id = $2 AND created_at >= $3 AND created_at < $4
			 ORDER BY md5(id::text) LIMIT $5`, routeID, arms[i].DeploymentID, from, to, sampleSize)
		if err != nil {
			return nil, classify(err)
		}
		for srows.Next() {
			var s abtest.Sample
			if err := srows.Scan(&s.DurationMS, &s.Error); err != nil {
				srows.Close()
				return nil, classify(err)
			}
			arms[i].Samples = append(arms[i].Samples, s)
		}
		srows.Close()
		arms[i].Finish()
	}
	return arms, nil
}
