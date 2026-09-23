package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
)

// DeploymentRepo reads and writes deployments, their revision history and their
// recorded state transitions.
//
// Three invariants are the whole point of this type, and every method is shaped
// to preserve them:
//
//  1. A deployment and its revision 1 appear together or not at all. Every
//     multi-statement method requires the caller's transaction, so there is no
//     path that leaves a deployment with no spec history.
//  2. generation changes on SPEC changes only. UpdateSpec bumps it; TransitionState
//     and the observed-state writers never touch it. That is what makes
//     generation <> observed_generation a trustworthy "there is work to do"
//     predicate for the Phase 5 controller (ADR-0008).
//  3. Every state change is a compare-and-set against the state the caller
//     believed the deployment was in. Two concurrent transitions cannot both
//     succeed; the loser affects no rows and gets ErrConflict.
//
// The database enforces 2 and 3 independently: trg_deployments__state_transition
// refuses an edge that is not in deployment_state_edges and writes the history row
// itself, so a bug here — or a hand-written UPDATE in psql during an incident —
// still cannot produce an illegal state or an unrecorded transition.
type DeploymentRepo struct{}

const deploymentColumns = `id, org_id, model_version_id, name, namespace,
	desired_replicas, min_replicas, max_replicas, resources, runtime_overrides,
	autoscaling, queue_config, routing_policy_id, generation, current_revision,
	observed_generation, state, state_entered_at, state_reason, state_message,
	ready_replicas, updated_replicas, conditions, last_error, last_synced_at,
	created_by, created_at, updated_at, deleted_at`

func scanDeployment(row interface{ Scan(...any) error }) (*models.Deployment, error) {
	var d models.Deployment
	err := row.Scan(&d.ID, &d.OrgID, &d.ModelVersionID, &d.Name, &d.Namespace,
		&d.DesiredReplicas, &d.MinReplicas, &d.MaxReplicas, &d.Resources, &d.RuntimeOverrides,
		&d.Autoscaling, &d.QueueConfig, &d.RoutingPolicyID, &d.Generation, &d.CurrentRevision,
		&d.ObservedGeneration, &d.State, &d.StateEnteredAt, &d.StateReason, &d.StateMessage,
		&d.ReadyReplicas, &d.UpdatedReplicas, &d.Conditions, &d.LastError, &d.LastSyncedAt,
		&d.CreatedBy, &d.CreatedAt, &d.UpdatedAt, &d.DeletedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &d, nil
}

// ---------------------------------------------------------------------------
// spec snapshots
// ---------------------------------------------------------------------------

// DeploymentSpec is the canonical, hashable form of a deployment's desired state.
//
// It exists as its own type rather than "whatever the deployment row happens to
// hold" for two reasons: a struct gives json.Marshal a deterministic field order,
// so the same desired state always produces the same hash; and it names exactly
// which columns are SPEC (and therefore bump generation) as opposed to OBSERVED.
// Adding an observed field to this struct would be a bug, which is easier to spot
// here than in a column list.
type DeploymentSpec struct {
	ModelVersionID   uuid.UUID       `json:"model_version_id"`
	Namespace        string          `json:"namespace"`
	DesiredReplicas  int32           `json:"desired_replicas"`
	MinReplicas      int32           `json:"min_replicas"`
	MaxReplicas      int32           `json:"max_replicas"`
	Resources        json.RawMessage `json:"resources"`
	RuntimeOverrides json.RawMessage `json:"runtime_overrides"`
	Autoscaling      json.RawMessage `json:"autoscaling"`
	QueueConfig      json.RawMessage `json:"queue_config"`
	RoutingPolicyID  *uuid.UUID      `json:"routing_policy_id"`
}

// SpecOf extracts the spec of a deployment.
//
// The jsonb fields are compacted, because json.RawMessage is emitted verbatim:
// two logically identical specs that differ only in whitespace would otherwise
// hash differently and make the controller believe there was work to do.
func SpecOf(d *models.Deployment) (DeploymentSpec, error) {
	res, err := compactJSON(d.Resources, `{}`)
	if err != nil {
		return DeploymentSpec{}, fmt.Errorf("resources: %w", err)
	}
	over, err := compactJSON(d.RuntimeOverrides, `{}`)
	if err != nil {
		return DeploymentSpec{}, fmt.Errorf("runtime_overrides: %w", err)
	}
	auto, err := compactJSON(d.Autoscaling, `{}`)
	if err != nil {
		return DeploymentSpec{}, fmt.Errorf("autoscaling: %w", err)
	}
	queue, err := compactJSON(d.QueueConfig, `{}`)
	if err != nil {
		return DeploymentSpec{}, fmt.Errorf("queue_config: %w", err)
	}
	return DeploymentSpec{
		ModelVersionID:   d.ModelVersionID,
		Namespace:        d.Namespace,
		DesiredReplicas:  d.DesiredReplicas,
		MinReplicas:      d.MinReplicas,
		MaxReplicas:      d.MaxReplicas,
		Resources:        res,
		RuntimeOverrides: over,
		Autoscaling:      auto,
		QueueConfig:      queue,
		RoutingPolicyID:  d.RoutingPolicyID,
	}, nil
}

// Apply writes a spec back onto a deployment, leaving observed fields alone. Used
// by Rollback, which restores an old spec without rewriting history.
func (s DeploymentSpec) Apply(d *models.Deployment) {
	d.ModelVersionID = s.ModelVersionID
	d.Namespace = s.Namespace
	d.DesiredReplicas = s.DesiredReplicas
	d.MinReplicas = s.MinReplicas
	d.MaxReplicas = s.MaxReplicas
	d.Resources = s.Resources
	d.RuntimeOverrides = s.RuntimeOverrides
	d.Autoscaling = s.Autoscaling
	d.QueueConfig = s.QueueConfig
	d.RoutingPolicyID = s.RoutingPolicyID
}

// Encode renders the spec and its hash for storage in a revision row.
func (s DeploymentSpec) Encode() (json.RawMessage, []byte, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding deployment spec: %w", err)
	}
	sum := sha256.Sum256(raw)
	return raw, sum[:], nil
}

// compactJSON normalises a jsonb value, substituting a default when absent.
func compactJSON(in json.RawMessage, fallback string) (json.RawMessage, error) {
	if len(bytes.TrimSpace(in)) == 0 {
		return json.RawMessage(fallback), nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, in); err != nil {
		return nil, err
	}
	return json.RawMessage(buf.Bytes()), nil
}

// ---------------------------------------------------------------------------
// creation
// ---------------------------------------------------------------------------

// Create inserts a deployment together with revision 1.
//
// Both statements must run in the caller's transaction: a deployment with no
// revision would have no recorded spec, so a later rollback would have nothing to
// roll back to and an incident review would have nothing to read. The caller sets
// d.ID (a UUIDv7) and the spec fields; everything else is filled in from the row
// the database returns, so defaults live in the schema and not in two places.
//
// The deployment is created in the pending state. Nothing is provisioned here —
// Kubernetes work belongs to the Phase 5 controller, which picks the row up
// because generation (1) differs from observed_generation (0).
func (r *DeploymentRepo) Create(ctx context.Context, q Querier, d *models.Deployment,
	revisionID uuid.UUID) (*models.DeploymentRevision, error) {
	if d.Resources == nil {
		d.Resources = json.RawMessage(`{}`)
	}
	if d.RuntimeOverrides == nil {
		d.RuntimeOverrides = json.RawMessage(`{}`)
	}
	if d.Autoscaling == nil {
		d.Autoscaling = json.RawMessage(`{}`)
	}
	if d.QueueConfig == nil {
		d.QueueConfig = json.RawMessage(`{}`)
	}

	err := q.QueryRow(ctx, `
		INSERT INTO deployments (
			id, org_id, model_version_id, name, namespace,
			desired_replicas, min_replicas, max_replicas,
			resources, runtime_overrides, autoscaling, queue_config,
			routing_policy_id, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		RETURNING generation, current_revision, observed_generation, state,
		          state_entered_at, created_at, updated_at`,
		d.ID, d.OrgID, d.ModelVersionID, d.Name, d.Namespace,
		d.DesiredReplicas, d.MinReplicas, d.MaxReplicas,
		d.Resources, d.RuntimeOverrides, d.Autoscaling, d.QueueConfig,
		d.RoutingPolicyID, d.CreatedBy,
	).Scan(&d.Generation, &d.CurrentRevision, &d.ObservedGeneration, &d.State,
		&d.StateEnteredAt, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, classify(err)
	}

	return r.insertRevision(ctx, q, revisionID, d, models.ReasonCreate)
}

// insertRevision snapshots the deployment's current spec as the revision it
// already claims to be on. Callers bump current_revision first, so the row and
// the snapshot cannot disagree.
func (r *DeploymentRepo) insertRevision(ctx context.Context, q Querier, id uuid.UUID,
	d *models.Deployment, reason models.RevisionReason) (*models.DeploymentRevision, error) {
	spec, err := SpecOf(d)
	if err != nil {
		return nil, err
	}
	raw, hash, err := spec.Encode()
	if err != nil {
		return nil, err
	}

	rev := &models.DeploymentRevision{
		ID:             id,
		DeploymentID:   d.ID,
		Revision:       d.CurrentRevision,
		ModelVersionID: d.ModelVersionID,
		Spec:           raw,
		SpecHash:       hash,
		Reason:         reason,
		CreatedBy:      d.CreatedBy,
	}
	err = q.QueryRow(ctx, `
		INSERT INTO deployment_revisions (
			id, deployment_id, revision, model_version_id, spec, spec_hash, reason, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING created_at`,
		rev.ID, rev.DeploymentID, rev.Revision, rev.ModelVersionID,
		rev.Spec, rev.SpecHash, rev.Reason, rev.CreatedBy,
	).Scan(&rev.CreatedAt)
	if err != nil {
		return nil, classify(err)
	}
	return rev, nil
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

// Get returns a deployment within a tenant.
func (r *DeploymentRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.Deployment, error) {
	return scanDeployment(q.QueryRow(ctx, `SELECT `+deploymentColumns+`
		  FROM deployments WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID))
}

// GetByName returns a deployment by its name within a tenant.
func (r *DeploymentRepo) GetByName(ctx context.Context, q Querier, orgID uuid.UUID,
	name string) (*models.Deployment, error) {
	return scanDeployment(q.QueryRow(ctx, `SELECT `+deploymentColumns+`
		  FROM deployments WHERE org_id = $1 AND name = $2 AND deleted_at IS NULL`, orgID, name))
}

// DeploymentFilter narrows a listing. A nil field means "any".
type DeploymentFilter struct {
	State          *models.DeploymentState
	ModelVersionID *uuid.UUID
	ModelID        *uuid.UUID
}

// List returns a tenant's deployments, newest first.
//
// The cursor is the id rather than an offset: ids are UUIDv7, so ordering by id
// descending is chronological and a page cannot shift under a caller who is
// paging while deployments are being created (ADR-0021).
func (r *DeploymentRepo) List(ctx context.Context, q Querier, orgID uuid.UUID,
	f DeploymentFilter, p Page) ([]*models.Deployment, error) {
	rows, err := q.Query(ctx, `
		SELECT `+deploymentColumns+`
		  FROM deployments d
		 WHERE d.org_id = $1
		   AND d.deleted_at IS NULL
		   AND ($2::uuid IS NULL OR d.id < $2)
		   AND ($3::deployment_state IS NULL OR d.state = $3)
		   AND ($4::uuid IS NULL OR d.model_version_id = $4)
		   AND ($5::uuid IS NULL OR EXISTS (
		           SELECT 1 FROM model_versions mv
		            WHERE mv.id = d.model_version_id AND mv.model_id = $5))
		 ORDER BY d.id DESC LIMIT $6`,
		orgID, p.Cursor, f.State, f.ModelVersionID, f.ModelID, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, classify(rows.Err())
}

// ListRevisions returns a deployment's spec history, newest first.
func (r *DeploymentRepo) ListRevisions(ctx context.Context, q Querier, orgID, deploymentID uuid.UUID,
	p Page) ([]*models.DeploymentRevision, error) {
	rows, err := q.Query(ctx, `
		SELECT dr.id, dr.deployment_id, dr.revision, dr.model_version_id, dr.spec,
		       dr.spec_hash, dr.reason, dr.created_by, dr.created_at
		  FROM deployment_revisions dr
		  JOIN deployments d ON d.id = dr.deployment_id
		 WHERE dr.deployment_id = $1 AND d.org_id = $2
		 ORDER BY dr.revision DESC LIMIT $3`, deploymentID, orgID, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.DeploymentRevision
	for rows.Next() {
		var rev models.DeploymentRevision
		if err := rows.Scan(&rev.ID, &rev.DeploymentID, &rev.Revision, &rev.ModelVersionID,
			&rev.Spec, &rev.SpecHash, &rev.Reason, &rev.CreatedBy, &rev.CreatedAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, &rev)
	}
	return out, classify(rows.Err())
}

// GetRevision returns one revision of a deployment.
func (r *DeploymentRepo) GetRevision(ctx context.Context, q Querier, orgID, deploymentID uuid.UUID,
	revision int32) (*models.DeploymentRevision, error) {
	var rev models.DeploymentRevision
	err := q.QueryRow(ctx, `
		SELECT dr.id, dr.deployment_id, dr.revision, dr.model_version_id, dr.spec,
		       dr.spec_hash, dr.reason, dr.created_by, dr.created_at
		  FROM deployment_revisions dr
		  JOIN deployments d ON d.id = dr.deployment_id
		 WHERE dr.deployment_id = $1 AND dr.revision = $2 AND d.org_id = $3`,
		deploymentID, revision, orgID).Scan(&rev.ID, &rev.DeploymentID, &rev.Revision,
		&rev.ModelVersionID, &rev.Spec, &rev.SpecHash, &rev.Reason, &rev.CreatedBy, &rev.CreatedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &rev, nil
}

// ListStateTransitions returns a deployment's recorded state changes, newest
// first. This is the answer to "why is it degraded" a day later, which Kubernetes
// Events cannot give because they expire in about an hour.
func (r *DeploymentRepo) ListStateTransitions(ctx context.Context, q Querier, orgID, deploymentID uuid.UUID,
	p Page) ([]*models.DeploymentStateTransition, error) {
	rows, err := q.Query(ctx, `
		SELECT id, org_id, deployment_id, from_state, to_state, reason, message,
		       generation, actor_type, actor_id, occurred_at
		  FROM deployment_state_transitions
		 WHERE deployment_id = $1 AND org_id = $2
		 ORDER BY occurred_at DESC, id DESC LIMIT $3`, deploymentID, orgID, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.DeploymentStateTransition
	for rows.Next() {
		var t models.DeploymentStateTransition
		if err := rows.Scan(&t.ID, &t.OrgID, &t.DeploymentID, &t.FromState, &t.ToState,
			&t.Reason, &t.Message, &t.Generation, &t.ActorType, &t.ActorID,
			&t.OccurredAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, &t)
	}
	return out, classify(rows.Err())
}

// ---------------------------------------------------------------------------
// spec changes
// ---------------------------------------------------------------------------

// UpdateSpec writes a changed desired state and records it as a new revision.
//
// The caller reads the deployment, mutates the spec fields it wants to change,
// and passes the generation it read. The UPDATE is a compare-and-set on that
// generation, so two operators editing the same deployment concurrently cannot
// silently overwrite one another: the second one affects no rows and gets
// ErrConflict, and its caller can re-read and retry.
//
// generation and current_revision advance together, and the new revision is
// written in the same transaction, so "what spec is generation 7" always has an
// answer.
func (r *DeploymentRepo) UpdateSpec(ctx context.Context, q Querier, orgID uuid.UUID,
	d *models.Deployment, expectedGeneration int64, reason models.RevisionReason,
	revisionID uuid.UUID) (*models.DeploymentRevision, error) {
	if !reason.Valid() {
		return nil, fmt.Errorf("%w: unknown revision reason %q", ErrConflict, reason)
	}
	if reason == models.ReasonCreate {
		return nil, fmt.Errorf("%w: reason %q belongs to Create, not an update", ErrConflict, reason)
	}

	spec, err := SpecOf(d)
	if err != nil {
		return nil, err
	}

	err = q.QueryRow(ctx, `
		UPDATE deployments SET
			model_version_id  = $4,
			namespace         = $5,
			desired_replicas  = $6,
			min_replicas      = $7,
			max_replicas      = $8,
			resources         = $9,
			runtime_overrides = $10,
			autoscaling       = $11,
			queue_config      = $12,
			routing_policy_id = $13,
			generation        = generation + 1,
			current_revision  = current_revision + 1
		 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL AND generation = $3
		 RETURNING generation, current_revision, updated_at`,
		d.ID, orgID, expectedGeneration,
		spec.ModelVersionID, spec.Namespace, spec.DesiredReplicas, spec.MinReplicas,
		spec.MaxReplicas, spec.Resources, spec.RuntimeOverrides, spec.Autoscaling,
		spec.QueueConfig, spec.RoutingPolicyID,
	).Scan(&d.Generation, &d.CurrentRevision, &d.UpdatedAt)
	if err != nil {
		cerr := classify(err)
		if !errors.Is(cerr, ErrNotFound) {
			return nil, cerr
		}
		// No row matched: either it is gone, or someone else changed it first.
		cur, getErr := r.Get(ctx, q, orgID, d.ID)
		if getErr != nil {
			return nil, getErr
		}
		return nil, fmt.Errorf("%w: deployment has moved to generation %d, expected %d",
			ErrConflict, cur.Generation, expectedGeneration)
	}

	// Re-derive the spec from the mutated deployment so the snapshot matches the
	// row exactly, including the compacted jsonb.
	spec.Apply(d)
	return r.insertRevision(ctx, q, revisionID, d, reason)
}

// Rollback restores an earlier revision's spec as a NEW revision.
//
// History is never rewritten: rolling back from revision 5 to revision 2 produces
// revision 6 whose spec equals revision 2's. That is what keeps an incident
// reviewable — "we rolled back at 14:02" is a fact in the table rather than an
// absence (ADR-0010).
func (r *DeploymentRepo) Rollback(ctx context.Context, q Querier, orgID, id uuid.UUID,
	targetRevision int32, revisionID uuid.UUID) (*models.Deployment, *models.DeploymentRevision, error) {
	d, err := r.Get(ctx, q, orgID, id)
	if err != nil {
		return nil, nil, err
	}
	if targetRevision == d.CurrentRevision {
		return nil, nil, fmt.Errorf("%w: deployment is already on revision %d",
			ErrConflict, targetRevision)
	}

	rev, err := r.GetRevision(ctx, q, orgID, id, targetRevision)
	if err != nil {
		return nil, nil, err
	}

	var spec DeploymentSpec
	if err := json.Unmarshal(rev.Spec, &spec); err != nil {
		return nil, nil, fmt.Errorf("revision %d of deployment %s has an unreadable spec: %w",
			targetRevision, id, err)
	}
	spec.Apply(d)

	newRev, err := r.UpdateSpec(ctx, q, orgID, d, d.Generation, models.ReasonRollback, revisionID)
	if err != nil {
		return nil, nil, err
	}
	return d, newRev, nil
}

// ---------------------------------------------------------------------------
// state machine
// ---------------------------------------------------------------------------

// TransitionState moves a deployment between lifecycle states.
//
// This is the method the user's atomicity requirement is about, and there are
// three layers of it:
//
//   - The Go state machine (packages/lifecycle) rejects an illegal edge before a
//     statement is sent, so the common case gives a clear error rather than a
//     constraint violation.
//   - The UPDATE is a compare-and-set on the state the caller believed the
//     deployment was in, so a controller and an operator acting at the same
//     instant cannot both succeed.
//   - The trigger validates the edge against deployment_state_edges, stamps
//     state_entered_at, and inserts the deployment_state_transitions row — all in
//     the caller's transaction. The state change and its history entry commit
//     together or not at all.
//
// A transition to the same state is a no-op: it returns OutcomeUnchanged without
// touching the row, so a controller that re-reports "still ready" does not
// restart the state timer or fill history with noise.
func (r *DeploymentRepo) TransitionState(ctx context.Context, q Querier, orgID, id uuid.UUID,
	from, to models.DeploymentState, reason lifecycle.Reason, message *string) (lifecycle.Outcome, error) {
	outcome, err := lifecycle.Transition(from, to)
	if err != nil {
		return outcome, fmt.Errorf("%w: %s", ErrConflict, err.Error())
	}
	if outcome == lifecycle.OutcomeUnchanged {
		return outcome, nil
	}
	if reason != "" && !lifecycle.ValidReason(reason) {
		return lifecycle.OutcomeUnchanged, fmt.Errorf("%w: unknown transition reason %q", ErrConflict, reason)
	}

	var reasonArg *string
	if reason != "" {
		s := string(reason)
		reasonArg = &s
	}

	tag, err := q.Exec(ctx, `
		UPDATE deployments
		   SET state = $4, state_reason = $5, state_message = $6
		 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL AND state = $3`,
		id, orgID, from, to, reasonArg, message)
	if err != nil {
		return lifecycle.OutcomeUnchanged, classify(err)
	}
	if tag.RowsAffected() == 0 {
		cur, getErr := r.Get(ctx, q, orgID, id)
		if getErr != nil {
			return lifecycle.OutcomeUnchanged, getErr
		}
		return lifecycle.OutcomeUnchanged, fmt.Errorf("%w: deployment is %s, not %s",
			ErrConflict, cur.State, from)
	}
	return lifecycle.OutcomeChanged, nil
}

// ---------------------------------------------------------------------------
// deletion
// ---------------------------------------------------------------------------

// SoftDelete marks a deployment deleted.
//
// It refuses unless the deployment is resting (pending, failed or stopped): a
// deployment with running pods must be stopped first, so the Phase 5 controller
// gets an explicit stop to reconcile rather than discovering that its desired
// state vanished. The ck_deployments__delete_only_when_resting constraint enforces
// the same rule, so a hand-written UPDATE cannot skip it either.
func (r *DeploymentRepo) SoftDelete(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	d, err := r.Get(ctx, q, orgID, id)
	if err != nil {
		return err
	}
	if !lifecycle.Deletable(d.State) {
		return fmt.Errorf("%w: a deployment in %s must be stopped before it can be deleted",
			ErrConflict, d.State)
	}

	var routed int
	if err := q.QueryRow(ctx, `
		SELECT count(*) FROM route_targets WHERE deployment_id = $1`, id).Scan(&routed); err != nil {
		return classify(err)
	}
	if routed > 0 {
		return fmt.Errorf("%w: %d route target(s) still send traffic to this deployment",
			ErrInUse, routed)
	}

	tag, err := q.Exec(ctx, `
		UPDATE deployments SET deleted_at = now()
		 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
