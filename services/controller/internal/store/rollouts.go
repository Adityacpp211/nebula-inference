package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

// Rollout is one canary rollout as the rollout reconciler sees it.
type Rollout struct {
	ID            uuid.UUID
	OrgID         uuid.UUID
	RouteID       uuid.UUID
	Route         string
	BaselineID    uuid.UUID
	Baseline      string
	CanaryID      uuid.UUID
	Canary        string
	Steps         json.RawMessage
	CurrentStep   int32
	Analysis      json.RawMessage
	State         models.RolloutState
	LastAnalysis  json.RawMessage
	StepEnteredAt *time.Time
}

// ActiveRollouts lists every rollout that has not finished: pending and
// progressing ones the reconciler advances, and paused ones it reports.
func (s *Store) ActiveRollouts(ctx context.Context) ([]Rollout, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.org_id, r.route_id, rt.model_name, r.baseline_deployment_id, b.name,
		       r.canary_deployment_id, c.name, r.steps, r.current_step, r.analysis, r.state,
		       r.last_analysis, r.step_entered_at
		  FROM canary_rollouts r
		  JOIN routes rt ON rt.id = r.route_id
		  JOIN deployments b ON b.id = r.baseline_deployment_id
		  JOIN deployments c ON c.id = r.canary_deployment_id
		 WHERE r.state IN ('pending', 'progressing', 'paused')
		 ORDER BY r.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rollout
	for rows.Next() {
		var r Rollout
		if err := rows.Scan(&r.ID, &r.OrgID, &r.RouteID, &r.Route, &r.BaselineID, &r.Baseline, &r.CanaryID,
			&r.Canary, &r.Steps, &r.CurrentStep, &r.Analysis, &r.State, &r.LastAnalysis, &r.StepEnteredAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RolloutChange is one move of the state machine.
type RolloutChange struct {
	State models.RolloutState
	Step  int32
	// CanaryWeight is the canary's new weight, the baseline taking the rest; -1
	// leaves the route's weights alone.
	CanaryWeight int
	// Promote makes the canary the route's baseline target.
	Promote bool
	// EnterStep restarts the step's hold window; Start stamps started_at.
	EnterStep, Start, Finish bool
	AbortReason              string
	LastAnalysis             json.RawMessage
	// Action is the audit action, e.g. rollout.advanced; empty writes no audit row.
	Action string
}

// ErrRolloutChanged means the rollout moved since it was read — an operator
// paused, aborted or promoted it — and nothing was written.
var ErrRolloutChanged = errors.New("rollout changed since it was read")

// ApplyRollout moves a rollout, its route's weights and the audit trail in one
// transaction, as a compare-and-set on the state and step it was read in: an
// operator's pause or abort always wins over the reconciler.
func (s *Store) ApplyRollout(ctx context.Context, r Rollout, ch RolloutChange) error {
	return db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var reason *string
		if ch.AbortReason != "" {
			reason = &ch.AbortReason
		}
		tag, err := tx.Exec(ctx, `
			UPDATE canary_rollouts
			   SET state = $4, current_step = $5,
			       last_analysis = coalesce($6, last_analysis),
			       abort_reason = coalesce($7, abort_reason),
			       started_at = CASE WHEN $8 THEN coalesce(started_at, now()) ELSE started_at END,
			       step_entered_at = CASE WHEN $9 THEN now() ELSE step_entered_at END,
			       finished_at = CASE WHEN $10 THEN now() ELSE finished_at END
			 WHERE id = $1 AND state = $2 AND current_step = $3`,
			r.ID, r.State, r.CurrentStep, ch.State, ch.Step, nullJSON(ch.LastAnalysis), reason,
			ch.Start, ch.EnterStep, ch.Finish)
		if err != nil {
			return fmt.Errorf("rollout: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return ErrRolloutChanged
		}
		if ch.CanaryWeight >= 0 {
			// Both rows in one statement: the weights-total-100 constraint is checked
			// at commit, and must never see a half-moved route.
			if _, err := tx.Exec(ctx, `
				UPDATE route_targets
				   SET weight = CASE WHEN deployment_id = $3 THEN $4::int ELSE 100 - $4::int END
				 WHERE route_id = $1 AND deployment_id IN ($2, $3)`,
				r.RouteID, r.BaselineID, r.CanaryID, ch.CanaryWeight); err != nil {
				return fmt.Errorf("route weights: %w", err)
			}
		}
		if ch.Promote {
			// The canary becomes the route's baseline, for the next rollout. Two
			// statements, old flag first: one baseline per route is a unique index,
			// checked row by row, so a swap in one statement depends on row order.
			if _, err := tx.Exec(ctx, `UPDATE route_targets SET is_baseline = false WHERE route_id = $1 AND is_baseline`,
				r.RouteID); err != nil {
				return fmt.Errorf("baseline: %w", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE route_targets SET is_baseline = true WHERE route_id = $1 AND deployment_id = $2`,
				r.RouteID, r.CanaryID); err != nil {
				return fmt.Errorf("baseline: %w", err)
			}
		}
		if ch.Action == "" {
			return nil
		}
		after, _ := json.Marshal(map[string]any{
			"state": ch.State, "step": ch.Step, "canary_weight": ch.CanaryWeight, "reason": ch.AbortReason,
			"route": r.Route, "baseline": r.Baseline, "canary": r.Canary,
		})
		id, err := db.NewID()
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO audit_logs (id, org_id, created_at, actor_type, actor_label, action, resource_type,
			                        resource_id, after)
			VALUES ($1, $2, now(), 'system', 'nebula-controller', $3, 'rollout', $4, $5)`,
			id, r.OrgID, ch.Action, r.ID, after)
		if err != nil {
			return fmt.Errorf("audit: %w", err)
		}
		return nil
	})
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}
