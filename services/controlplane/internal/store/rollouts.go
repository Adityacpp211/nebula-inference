package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// RolloutRepo is canary rollouts (docs/architecture.md §7.3). The control plane
// creates them and applies an operator's verbs; the controller's rollout
// reconciler advances them.
type RolloutRepo struct{}

const rolloutColumns = `id, org_id, route_id, baseline_deployment_id, canary_deployment_id, steps,
	current_step, analysis, state, last_analysis, abort_reason, created_by, started_at, step_entered_at,
	finished_at, created_at`

func scanRollout(row interface{ Scan(...any) error }) (*models.CanaryRollout, error) {
	var r models.CanaryRollout
	if err := row.Scan(&r.ID, &r.OrgID, &r.RouteID, &r.BaselineDeploymentID, &r.CanaryDeploymentID, &r.Steps,
		&r.CurrentStep, &r.Analysis, &r.State, &r.LastAnalysis, &r.AbortReason, &r.CreatedBy, &r.StartedAt,
		&r.StepEnteredAt, &r.FinishedAt, &r.CreatedAt); err != nil {
		return nil, classify(err)
	}
	return &r, nil
}

// Create starts a rollout: the canary joins the route at weight 0 (the reconciler
// moves it), the current target is marked the baseline, and the row is pending.
// A second active rollout on the route is refused by the database
// (uq_canary_rollouts__one_active_per_route), not by a check that could race.
func (r *RolloutRepo) Create(ctx context.Context, q Querier, ro *models.CanaryRollout) error {
	if _, err := q.Exec(ctx, `UPDATE route_targets SET is_baseline = false
		 WHERE route_id = $1 AND is_baseline AND deployment_id <> $2`, ro.RouteID, ro.BaselineDeploymentID); err != nil {
		return classify(err)
	}
	if _, err := q.Exec(ctx, `UPDATE route_targets SET is_baseline = true, label = coalesce(label, 'baseline')
		 WHERE route_id = $1 AND deployment_id = $2`, ro.RouteID, ro.BaselineDeploymentID); err != nil {
		return classify(err)
	}
	if _, err := q.Exec(ctx, `
		INSERT INTO route_targets (id, route_id, deployment_id, weight, is_baseline, label)
		VALUES (gen_random_uuid(), $1, $2, 0, false, 'canary')
		ON CONFLICT (route_id, deployment_id) DO UPDATE SET weight = 0, label = coalesce(route_targets.label, 'canary')`,
		ro.RouteID, ro.CanaryDeploymentID); err != nil {
		return classify(err)
	}
	_, err := q.Exec(ctx, `
		INSERT INTO canary_rollouts (id, org_id, route_id, baseline_deployment_id, canary_deployment_id, steps,
		                             analysis, state, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)`,
		ro.ID, ro.OrgID, ro.RouteID, ro.BaselineDeploymentID, ro.CanaryDeploymentID, ro.Steps, ro.Analysis, ro.CreatedBy)
	return classify(err)
}

// Get returns one of an org's rollouts.
func (r *RolloutRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.CanaryRollout, error) {
	return scanRollout(q.QueryRow(ctx, `SELECT `+rolloutColumns+` FROM canary_rollouts WHERE id = $1 AND org_id = $2`,
		id, orgID))
}

// List pages through an org's rollouts, newest first, optionally by state.
func (r *RolloutRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, state *models.RolloutState, p Page) ([]*models.CanaryRollout, error) {
	rows, err := q.Query(ctx, `SELECT `+rolloutColumns+`
		  FROM canary_rollouts
		 WHERE org_id = $1 AND ($2::uuid IS NULL OR id < $2) AND ($3::rollout_state IS NULL OR state = $3)
		 ORDER BY id DESC LIMIT $4`, orgID, p.Cursor, state, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*models.CanaryRollout
	for rows.Next() {
		ro, err := scanRollout(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ro)
	}
	return out, classify(rows.Err())
}

// Verb is an operator's change to a rollout.
type Verb struct {
	From []models.RolloutState
	To   models.RolloutState
	// CanaryWeight is the canary's new weight, the baseline taking the rest; -1
	// leaves the weights alone.
	CanaryWeight int
	Promote      bool
	// Restart starts a fresh hold window (resume).
	Restart     bool
	Finish      bool
	AbortReason string
}

// Apply moves a rollout by an operator's verb, with its weights, as a
// compare-and-set on the states the verb may start from. ErrConflict means the
// rollout is in none of them.
func (r *RolloutRepo) Apply(ctx context.Context, q Querier, orgID, id uuid.UUID, v Verb) (*models.CanaryRollout, error) {
	from := make([]string, len(v.From))
	for i, s := range v.From {
		from[i] = string(s)
	}
	var reason *string
	if v.AbortReason != "" {
		reason = &v.AbortReason
	}
	ro, err := scanRollout(q.QueryRow(ctx, `
		UPDATE canary_rollouts
		   SET state = $4, abort_reason = coalesce($5, abort_reason),
		       step_entered_at = CASE WHEN $6 THEN now() ELSE step_entered_at END,
		       finished_at = CASE WHEN $7 THEN now() ELSE finished_at END
		 WHERE id = $1 AND org_id = $2 AND state::text = ANY($3)
		RETURNING `+rolloutColumns, id, orgID, from, v.To, reason, v.Restart, v.Finish))
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("%w: the rollout is not in a state that allows this", ErrConflict)
	}
	if err != nil {
		return nil, err
	}
	if v.CanaryWeight >= 0 {
		if _, err := q.Exec(ctx, `
			UPDATE route_targets
			   SET weight = CASE WHEN deployment_id = $3 THEN $4::int ELSE 100 - $4::int END
			 WHERE route_id = $1 AND deployment_id IN ($2, $3)`,
			ro.RouteID, ro.BaselineDeploymentID, ro.CanaryDeploymentID, v.CanaryWeight); err != nil {
			return nil, classify(err)
		}
	}
	if v.Promote {
		if _, err := q.Exec(ctx, `UPDATE route_targets SET is_baseline = false WHERE route_id = $1 AND is_baseline`,
			ro.RouteID); err != nil {
			return nil, classify(err)
		}
		if _, err := q.Exec(ctx, `UPDATE route_targets SET is_baseline = true WHERE route_id = $1 AND deployment_id = $2`,
			ro.RouteID, ro.CanaryDeploymentID); err != nil {
			return nil, classify(err)
		}
	}
	return ro, nil
}

// CanaryWeight is the canary's weight on the route right now.
func (r *RolloutRepo) CanaryWeight(ctx context.Context, q Querier, ro *models.CanaryRollout) (int32, error) {
	var w int32
	err := q.QueryRow(ctx, `SELECT weight FROM route_targets WHERE route_id = $1 AND deployment_id = $2`,
		ro.RouteID, ro.CanaryDeploymentID).Scan(&w)
	return w, classify(err)
}
