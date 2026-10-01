package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// RouteRepo reads and writes routes and their weighted targets.
//
// A route is the public model name; its targets are the deployments serving it
// (docs/data-model.md §5). Weights are replaced as a set, never edited one row at a
// time from the API: the deferred trigger checks the total at COMMIT, and a set
// replacement inside one transaction is the shape that can never be observed
// half-applied.
type RouteRepo struct{}

const routeColumns = `id, org_id, model_name, routing_policy_id, created_at, updated_at`

func scanRoute(row interface{ Scan(...any) error }) (*models.Route, error) {
	var r models.Route
	if err := row.Scan(&r.ID, &r.OrgID, &r.ModelName, &r.RoutingPolicyID, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, classify(err)
	}
	return &r, nil
}

// TargetView is a route target joined with what a reader needs to understand it.
type TargetView struct {
	models.RouteTarget
	DeploymentName  string
	DeploymentState models.DeploymentState
	ModelVersionID  uuid.UUID
	ModelVersion    string // "model:version"
}

// Create inserts a route and its targets. The caller's transaction commits them
// together, which is when the weight total is checked.
func (r *RouteRepo) Create(ctx context.Context, q Querier, rt *models.Route, targets []*models.RouteTarget) error {
	err := q.QueryRow(ctx, `
		INSERT INTO routes (id, org_id, model_name, routing_policy_id)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at`,
		rt.ID, rt.OrgID, rt.ModelName, rt.RoutingPolicyID).Scan(&rt.CreatedAt, &rt.UpdatedAt)
	if err != nil {
		return classify(err)
	}
	return r.insertTargets(ctx, q, rt.ID, targets)
}

func (r *RouteRepo) insertTargets(ctx context.Context, q Querier, routeID uuid.UUID, targets []*models.RouteTarget) error {
	for _, t := range targets {
		t.RouteID = routeID
		err := q.QueryRow(ctx, `
			INSERT INTO route_targets (id, route_id, deployment_id, weight, is_baseline, label)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING updated_at`,
			t.ID, t.RouteID, t.DeploymentID, t.Weight, t.IsBaseline, t.Label).Scan(&t.UpdatedAt)
		if err != nil {
			return classify(err)
		}
	}
	return nil
}

// ReplaceTargets swaps a route's whole target set. Deleting and re-inserting is
// safe because nothing references a route_targets row by id.
func (r *RouteRepo) ReplaceTargets(ctx context.Context, q Querier, routeID uuid.UUID, targets []*models.RouteTarget) error {
	if _, err := q.Exec(ctx, `DELETE FROM route_targets WHERE route_id = $1`, routeID); err != nil {
		return classify(err)
	}
	if err := r.insertTargets(ctx, q, routeID, targets); err != nil {
		return err
	}
	_, err := q.Exec(ctx, `UPDATE routes SET updated_at = now() WHERE id = $1`, routeID)
	return classify(err)
}

// SetPolicy changes a route's routing policy (nil for the default).
func (r *RouteRepo) SetPolicy(ctx context.Context, q Querier, orgID, routeID uuid.UUID, policyID *uuid.UUID) error {
	tag, err := q.Exec(ctx, `UPDATE routes SET routing_policy_id = $3 WHERE id = $1 AND org_id = $2`,
		routeID, orgID, policyID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Get returns one of an org's routes.
func (r *RouteRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.Route, error) {
	return scanRoute(q.QueryRow(ctx, `SELECT `+routeColumns+` FROM routes WHERE id = $1 AND org_id = $2`, id, orgID))
}

// List pages through an org's routes, newest first.
func (r *RouteRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, p Page) ([]*models.Route, error) {
	rows, err := q.Query(ctx, `SELECT `+routeColumns+`
		  FROM routes
		 WHERE org_id = $1 AND ($2::uuid IS NULL OR id < $2)
		 ORDER BY id DESC LIMIT $3`, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*models.Route
	for rows.Next() {
		rt, err := scanRoute(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rt)
	}
	return out, classify(rows.Err())
}

// Targets lists a route's targets with their deployments, heaviest first.
func (r *RouteRepo) Targets(ctx context.Context, q Querier, routeID uuid.UUID) ([]TargetView, error) {
	rows, err := q.Query(ctx, `
		SELECT t.id, t.route_id, t.deployment_id, t.weight, t.is_baseline, t.label, t.updated_at,
		       d.name, d.state, v.id, m.name || ':' || v.version
		  FROM route_targets t
		  JOIN deployments d    ON d.id = t.deployment_id
		  JOIN model_versions v ON v.id = d.model_version_id
		  JOIN models m         ON m.id = v.model_id
		 WHERE t.route_id = $1
		 ORDER BY t.weight DESC, d.name`, routeID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []TargetView
	for rows.Next() {
		var t TargetView
		if err := rows.Scan(&t.ID, &t.RouteID, &t.DeploymentID, &t.Weight, &t.IsBaseline, &t.Label, &t.UpdatedAt,
			&t.DeploymentName, &t.DeploymentState, &t.ModelVersionID, &t.ModelVersion); err != nil {
			return nil, classify(err)
		}
		out = append(out, t)
	}
	return out, classify(rows.Err())
}

// Delete removes a route; its targets go with it (ON DELETE CASCADE).
func (r *RouteRepo) Delete(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	tag, err := q.Exec(ctx, `DELETE FROM routes WHERE id = $1 AND org_id = $2`, id, orgID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// routing policies
// ---------------------------------------------------------------------------

// RoutingPolicyRepo reads routing policies: the built-in rows (org_id NULL) and an
// org's own.
type RoutingPolicyRepo struct{}

const policyColumns = `id, org_id, name, strategy, config, is_default, created_at`

func scanPolicy(row interface{ Scan(...any) error }) (*models.RoutingPolicy, error) {
	var p models.RoutingPolicy
	if err := row.Scan(&p.ID, &p.OrgID, &p.Name, &p.Strategy, &p.Config, &p.IsDefault, &p.CreatedAt); err != nil {
		return nil, classify(err)
	}
	return &p, nil
}

// List returns the policies an org can use, built-ins first.
func (r *RoutingPolicyRepo) List(ctx context.Context, q Querier, orgID uuid.UUID) ([]*models.RoutingPolicy, error) {
	rows, err := q.Query(ctx, `SELECT `+policyColumns+`
		  FROM routing_policies
		 WHERE org_id IS NULL OR org_id = $1
		 ORDER BY org_id NULLS FIRST, name`, orgID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*models.RoutingPolicy
	for rows.Next() {
		p, err := scanPolicy(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, classify(rows.Err())
}

// GetByName finds a policy by name, preferring the org's own over a built-in.
func (r *RoutingPolicyRepo) GetByName(ctx context.Context, q Querier, orgID uuid.UUID, name string) (*models.RoutingPolicy, error) {
	return scanPolicy(q.QueryRow(ctx, `SELECT `+policyColumns+`
		  FROM routing_policies
		 WHERE name = $2 AND (org_id IS NULL OR org_id = $1)
		 ORDER BY org_id NULLS LAST LIMIT 1`, orgID, name))
}

// Get returns a policy visible to an org.
func (r *RoutingPolicyRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.RoutingPolicy, error) {
	return scanPolicy(q.QueryRow(ctx, `SELECT `+policyColumns+`
		  FROM routing_policies WHERE id = $2 AND (org_id IS NULL OR org_id = $1)`, orgID, id))
}

// ---------------------------------------------------------------------------
// the gateway's routing table
// ---------------------------------------------------------------------------

// TableRow is one route target with everything the gateway needs to serve it,
// across every organization. Read by the internal routing-table endpoint only.
type TableRow struct {
	RouteID         uuid.UUID
	OrgID           uuid.UUID
	OrgSlug         string
	ModelName       string
	RouteCreatedAt  time.Time
	RouteUpdatedAt  time.Time
	Strategy        *string
	StrategyConfig  json.RawMessage
	TargetWeight    int32
	TargetLabel     *string
	IsBaseline      bool
	DeploymentID    uuid.UUID
	DeploymentName  string
	DeploymentState models.DeploymentState
	Namespace       string
	ModelVersionID  uuid.UUID
	ModelVersion    string
	Task            models.ModelTask
	Runtime         models.Runtime
	ContextWindow   int32
	RuntimeConfig   json.RawMessage
	Overrides       json.RawMessage
}

// RoutingTable reads every route target of every organization in one query.
//
// A route's policy is its own, or the default. Deployments that are deleted are
// excluded; deployments in other states are included with their state, because
// "this target exists and is stopped" is information the gateway reports, and the
// endpoints (not the row) decide whether traffic can flow.
func (r *RouteRepo) RoutingTable(ctx context.Context, q Querier) ([]TableRow, error) {
	rows, err := q.Query(ctx, `
		SELECT rt.id, rt.org_id, o.slug, rt.model_name, rt.created_at, rt.updated_at,
		       COALESCE(p.strategy, dp.strategy), COALESCE(p.config, dp.config, '{}'::jsonb),
		       t.weight, t.label, t.is_baseline,
		       d.id, d.name, d.state, d.namespace,
		       v.id, m.name || ':' || v.version, m.task, v.runtime, v.context_window,
		       v.runtime_config, d.runtime_overrides
		  FROM routes rt
		  JOIN organizations o     ON o.id = rt.org_id
		  JOIN route_targets t     ON t.route_id = rt.id
		  JOIN deployments d       ON d.id = t.deployment_id AND d.deleted_at IS NULL
		  JOIN model_versions v    ON v.id = d.model_version_id
		  JOIN models m            ON m.id = v.model_id
		  LEFT JOIN routing_policies p  ON p.id = rt.routing_policy_id
		  LEFT JOIN LATERAL (SELECT strategy, config FROM routing_policies
		                      WHERE org_id IS NULL AND is_default ORDER BY name LIMIT 1) dp ON true
		 ORDER BY o.slug, rt.model_name, t.weight DESC, d.name`)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []TableRow
	for rows.Next() {
		var t TableRow
		if err := rows.Scan(&t.RouteID, &t.OrgID, &t.OrgSlug, &t.ModelName, &t.RouteCreatedAt, &t.RouteUpdatedAt,
			&t.Strategy, &t.StrategyConfig, &t.TargetWeight, &t.TargetLabel, &t.IsBaseline,
			&t.DeploymentID, &t.DeploymentName, &t.DeploymentState, &t.Namespace,
			&t.ModelVersionID, &t.ModelVersion, &t.Task, &t.Runtime, &t.ContextWindow,
			&t.RuntimeConfig, &t.Overrides); err != nil {
			return nil, classify(err)
		}
		out = append(out, t)
	}
	return out, classify(rows.Err())
}

// DeploymentRef is a live deployment as a route target sees it.
type DeploymentRef struct {
	ID           uuid.UUID
	Name         string
	State        models.DeploymentState
	Task         models.ModelTask
	ModelVersion string
}

// ResolveDeployment finds a live deployment of an org by id or, when id is nil,
// by name.
func (r *RouteRepo) ResolveDeployment(ctx context.Context, q Querier, orgID uuid.UUID, id *uuid.UUID, name string) (*DeploymentRef, error) {
	var d DeploymentRef
	err := q.QueryRow(ctx, `
		SELECT d.id, d.name, d.state, m.task, m.name || ':' || v.version
		  FROM deployments d
		  JOIN model_versions v ON v.id = d.model_version_id
		  JOIN models m         ON m.id = v.model_id
		 WHERE d.org_id = $1 AND d.deleted_at IS NULL
		   AND (($2::uuid IS NOT NULL AND d.id = $2) OR ($2::uuid IS NULL AND d.name = $3))`,
		orgID, id, name).Scan(&d.ID, &d.Name, &d.State, &d.Task, &d.ModelVersion)
	if err != nil {
		return nil, classify(err)
	}
	return &d, nil
}
