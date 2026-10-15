// Package store is the autoscaler's view of PostgreSQL.
//
// The autoscaler connects as nebula_controller and owns exactly two things
// (docs/architecture.md §4.4): deployments.desired_replicas, and the
// autoscaling_events it writes for every decision worth a row. The column-level
// grants of migration 000007 refuse anything else, and migration 000012 turns a
// desired_replicas change into a generation bump so the controller reconciles it
// at once.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/autoscale"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

// Store is the autoscaler's query set.
type Store struct {
	pool *db.Pool
}

// New builds a store.
func New(pool *db.Pool) *Store { return &Store{pool: pool} }

// Target is one deployment with autoscaling enabled, as the loop needs it.
type Target struct {
	ID       uuid.UUID
	OrgID    uuid.UUID
	OrgSlug  string
	Name     string
	State    models.DeploymentState
	Desired  int32
	Min, Max int32
	// Autoscaling is the raw deployments.autoscaling object; the loop parses it so
	// a malformed one is reported per deployment rather than failing the list.
	Autoscaling json.RawMessage
	// RolloutActive is true while a canary rollout involving the deployment has not
	// finished: replicas do not move under a rollout's analysis.
	RolloutActive bool
}

// Targets lists every live, running deployment whose autoscaling is enabled.
// Stopped, stopping, pending and failed deployments are not scaled: their replica
// count is the lifecycle's to decide, not load's.
func (s *Store) Targets(ctx context.Context) ([]Target, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.org_id, o.slug, d.name, d.state, d.desired_replicas, d.min_replicas,
		       d.max_replicas, d.autoscaling,
		       EXISTS (SELECT 1 FROM canary_rollouts r
		                WHERE (r.baseline_deployment_id = d.id OR r.canary_deployment_id = d.id)
		                  AND r.state IN ('pending', 'progressing', 'paused'))
		  FROM deployments d
		  JOIN organizations o ON o.id = d.org_id
		 WHERE d.deleted_at IS NULL
		   AND d.state IN ('provisioning', 'starting', 'ready', 'degraded')
		   AND d.autoscaling @> '{"enabled": true}'
		 ORDER BY d.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Target
	for rows.Next() {
		var t Target
		if err := rows.Scan(&t.ID, &t.OrgID, &t.OrgSlug, &t.Name, &t.State, &t.Desired, &t.Min, &t.Max,
			&t.Autoscaling, &t.RolloutActive); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ErrConflict means the deployment changed between the read and the write: a user
// scaled it, changed its bounds, or deleted it. Nothing was written; the next
// decision starts from the new state.
var ErrConflict = errors.New("deployment changed since it was read")

// Record writes a decision, and applies it when it changes replicas, in one
// transaction: a change without its explanation, or an explanation of a change
// that did not happen, is never visible.
//
// The update is a compare-and-set on the replica count the decision started from,
// so the autoscaler never overwrites a user's scale it has not seen.
func (s *Store) Record(ctx context.Context, t Target, d autoscale.Decision) error {
	signals, err := json.Marshal(d.Signals)
	if err != nil {
		return err
	}
	if d.Signals == nil {
		signals = []byte("[]")
	}
	id, err := db.NewID()
	if err != nil {
		return err
	}
	return db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if d.Changed() {
			tag, err := tx.Exec(ctx, `
				UPDATE deployments SET desired_replicas = $3
				 WHERE id = $1 AND desired_replicas = $2 AND deleted_at IS NULL
				   AND $3 BETWEEN min_replicas AND max_replicas`, t.ID, d.From, d.To)
			if err != nil {
				return fmt.Errorf("scaling: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return ErrConflict
			}
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO autoscaling_events (id, org_id, deployment_id, from_replicas, to_replicas,
			                                direction, signals, decision, reason)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			id, t.OrgID, t.ID, d.From, d.To, d.Direction, signals, d.Decision, d.Reason)
		if err != nil {
			return fmt.Errorf("recording the decision: %w", err)
		}
		return nil
	})
}
