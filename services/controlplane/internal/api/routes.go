package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// Routes are the public model names. A route fans out to weighted targets, each a
// deployment (docs/api.md §4, "Routes, rollouts, experiments"). The gateway reads
// them through the internal routing table and converges within its refresh
// interval; nothing here talks to a gateway.

// routeNamePattern matches ck_routes__model_name_format.
var routeNamePattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._:-]{0,94}[A-Za-z0-9])?$`)

// routeTargetRequest names a deployment by id or by name, and its share.
type routeTargetRequest struct {
	DeploymentID *uuid.UUID `json:"deployment_id,omitempty"`
	Deployment   string     `json:"deployment,omitempty"`
	Weight       *int32     `json:"weight"`
	Label        *string    `json:"label,omitempty"`
	IsBaseline   bool       `json:"is_baseline,omitempty"`
}

type createRouteRequest struct {
	ModelName     string               `json:"model_name"`
	RoutingPolicy *string              `json:"routing_policy,omitempty"`
	Targets       []routeTargetRequest `json:"targets"`
}

// updateRouteRequest replaces the target set, the policy, or both. RoutingPolicy is
// raw so an explicit null ("back to the default") differs from an absent field.
type updateRouteRequest struct {
	RoutingPolicy json.RawMessage      `json:"routing_policy,omitempty"`
	Targets       []routeTargetRequest `json:"targets,omitempty"`
}

type routePolicyResponse struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	Strategy string    `json:"strategy"`
}

type routeTargetResponse struct {
	DeploymentID   uuid.UUID `json:"deployment_id"`
	Deployment     string    `json:"deployment"`
	ModelVersionID uuid.UUID `json:"model_version_id"`
	ModelVersion   string    `json:"model_version"`
	Weight         int32     `json:"weight"`
	Label          *string   `json:"label"`
	IsBaseline     bool      `json:"is_baseline"`
	State          string    `json:"state"`
}

type routeResponse struct {
	ID        uuid.UUID `json:"id"`
	Object    string    `json:"object"`
	ModelName string    `json:"model_name"`
	// RoutingPolicy is null when the route uses the default policy.
	RoutingPolicy *routePolicyResponse  `json:"routing_policy"`
	Targets       []routeTargetResponse `json:"targets"`
	CreatedAt     time.Time             `json:"created_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
	Note          string                `json:"note"`
}

// routeNote says what a route change does and does not do yet, so a 200 is not
// read as "traffic has moved".
const routeNote = "gateways pick up route changes within their refresh interval (a few seconds); " +
	"a target receives traffic only while its deployment has ready, heartbeating replicas"

type routingPolicyResponse struct {
	ID        uuid.UUID       `json:"id"`
	Name      string          `json:"name"`
	Strategy  string          `json:"strategy"`
	Config    json.RawMessage `json:"config"`
	IsDefault bool            `json:"is_default"`
	BuiltIn   bool            `json:"built_in"`
}

func (a *API) listRoutingPolicies(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	ps, err := a.Store.Policies.List(r.Context(), a.Store.Pool(), ident.OrgID)
	if err != nil {
		return storeError(err, "routing policy")
	}
	out := make([]routingPolicyResponse, 0, len(ps))
	for _, p := range ps {
		out = append(out, routingPolicyResponse{ID: p.ID, Name: p.Name, Strategy: p.Strategy,
			Config: p.Config, IsDefault: p.IsDefault, BuiltIn: p.OrgID == nil})
	}
	a.write(w, r, http.StatusOK, newList(out, 0, func(p routingPolicyResponse) string { return p.ID.String() }))
	return nil
}

func (a *API) listRoutes(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	rows, err := a.Store.Routes.List(ctx, a.Store.Pool(), ident.OrgID, p)
	if err != nil {
		return storeError(err, "route")
	}
	out := make([]routeResponse, 0, len(rows))
	for _, rt := range rows {
		resp, err := a.routeResponse(ctx, a.Store.Pool(), ident.OrgID, rt)
		if err != nil {
			return err
		}
		out = append(out, resp)
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(rt routeResponse) string { return rt.ID.String() }))
	return nil
}

func (a *API) getRoute(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "route_id", "route")
	if err != nil {
		return err
	}
	rt, err := a.Store.Routes.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "route")
	}
	resp, err := a.routeResponse(r.Context(), a.Store.Pool(), ident.OrgID, rt)
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusOK, resp)
	return nil
}

func (a *API) createRoute(w http.ResponseWriter, r *http.Request) error {
	var req createRouteRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.ModelName, "model_name", routeNamePattern,
		"model_name must be 1-96 letters, digits, dots, dashes, colons or underscores, "+
			"starting and ending with a letter or digit"); err != nil {
		return err
	}
	if err := checkTargetShape(req.Targets); err != nil {
		return err
	}
	ident := auth.MustFromContext(r.Context())
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	rt := &models.Route{ID: id, OrgID: ident.OrgID, ModelName: req.ModelName}

	var resp routeResponse
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if req.RoutingPolicy != nil {
			pid, err := a.resolvePolicy(ctx, q, ident.OrgID, *req.RoutingPolicy)
			if err != nil {
				return err
			}
			rt.RoutingPolicyID = pid
		}
		targets, err := a.resolveTargets(ctx, q, ident.OrgID, req.Targets)
		if err != nil {
			return err
		}
		if err := a.Store.Routes.Create(ctx, q, rt, targets); err != nil {
			return storeError(err, "route")
		}
		if resp, err = a.routeResponse(ctx, q, ident.OrgID, rt); err != nil {
			return err
		}
		entry := auditFor(r, store.ActionRouteCreate, store.ResourceRoute)
		entry.ResourceID = &rt.ID
		entry.After = resp
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return routeCommitError(err)
	}
	w.Header().Set("Location", "/v1/routes/"+rt.ID.String())
	a.write(w, r, http.StatusCreated, resp)
	return nil
}

// updateRoute replaces a route's targets and/or policy atomically. Weights are
// always given as the complete set, so there is no request whose effect depends on
// what another writer did in between.
func (a *API) updateRoute(w http.ResponseWriter, r *http.Request) error {
	var req updateRouteRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if req.Targets == nil && req.RoutingPolicy == nil {
		return httpx.ErrInvalidRequest("nothing to change: send targets, routing_policy, or both",
			"empty_update", "body")
	}
	if req.Targets != nil {
		if err := checkTargetShape(req.Targets); err != nil {
			return err
		}
	}
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "route_id", "route")
	if err != nil {
		return err
	}

	var resp routeResponse
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		rt, err := a.Store.Routes.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "route")
		}
		before, err := a.routeResponse(ctx, q, ident.OrgID, rt)
		if err != nil {
			return err
		}
		if req.RoutingPolicy != nil {
			var pid *uuid.UUID
			if !bytes.Equal(bytes.TrimSpace(req.RoutingPolicy), []byte("null")) {
				var name string
				if err := json.Unmarshal(req.RoutingPolicy, &name); err != nil {
					return httpx.ErrInvalidRequest("routing_policy must be a policy name or null",
						"invalid_type", "routing_policy")
				}
				if pid, err = a.resolvePolicy(ctx, q, ident.OrgID, name); err != nil {
					return err
				}
			}
			if err := a.Store.Routes.SetPolicy(ctx, q, ident.OrgID, id, pid); err != nil {
				return storeError(err, "route")
			}
		}
		if req.Targets != nil {
			targets, err := a.resolveTargets(ctx, q, ident.OrgID, req.Targets)
			if err != nil {
				return err
			}
			if err := a.Store.Routes.ReplaceTargets(ctx, q, id, targets); err != nil {
				return storeError(err, "route")
			}
		}
		if rt, err = a.Store.Routes.Get(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "route")
		}
		if resp, err = a.routeResponse(ctx, q, ident.OrgID, rt); err != nil {
			return err
		}
		entry := auditFor(r, store.ActionRouteUpdate, store.ResourceRoute)
		entry.ResourceID = &id
		entry.Before, entry.After = before, resp
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return routeCommitError(err)
	}
	a.write(w, r, http.StatusOK, resp)
	return nil
}

func (a *API) deleteRoute(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "route_id", "route")
	if err != nil {
		return err
	}
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		rt, err := a.Store.Routes.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "route")
		}
		before, err := a.routeResponse(ctx, q, ident.OrgID, rt)
		if err != nil {
			return err
		}
		if err := a.Store.Routes.Delete(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "route")
		}
		entry := auditFor(r, store.ActionRouteDelete, store.ResourceRoute)
		entry.ResourceID = &id
		entry.Before = before
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// checkTargetShape validates what can be checked without the database: at least
// one target, weights in range summing to exactly 100, at most one baseline.
//
// The database checks the sum again at COMMIT; checking here too gives the caller
// a 400 that names the problem instead of a constraint message.
func checkTargetShape(ts []routeTargetRequest) error {
	if len(ts) == 0 {
		return httpx.ErrInvalidRequest("at least one target is required", "missing_field", "targets")
	}
	sum, baselines := 0, 0
	for i, t := range ts {
		field := fmt.Sprintf("targets[%d]", i)
		if (t.DeploymentID == nil) == (t.Deployment == "") {
			return httpx.ErrInvalidRequest(field+": give exactly one of deployment_id or deployment",
				"invalid_target", field)
		}
		if t.Weight == nil {
			return httpx.ErrInvalidRequest(field+".weight is required", "missing_field", field+".weight")
		}
		if *t.Weight < 0 || *t.Weight > 100 {
			return httpx.ErrInvalidRequest(field+".weight must be between 0 and 100", "invalid_weight", field+".weight")
		}
		sum += int(*t.Weight)
		if t.IsBaseline {
			baselines++
		}
		if t.Label != nil && len(*t.Label) > 64 {
			return httpx.ErrInvalidRequest(field+".label must be at most 64 characters", "invalid_label", field+".label")
		}
	}
	if sum != 100 {
		return httpx.ErrInvalidRequest(
			fmt.Sprintf("target weights must total exactly 100, got %d: a route that totals less silently drops traffic", sum),
			"invalid_weights", "targets")
	}
	if baselines > 1 {
		return httpx.ErrInvalidRequest("at most one target may be the baseline", "multiple_baselines", "targets")
	}
	return nil
}

// resolveTargets checks every target's deployment in the caller's org and builds
// the rows. All targets must serve the same task: a route is one capability.
func (a *API) resolveTargets(ctx context.Context, q store.Querier, orgID uuid.UUID, ts []routeTargetRequest) ([]*models.RouteTarget, error) {
	out := make([]*models.RouteTarget, 0, len(ts))
	seen := map[uuid.UUID]bool{}
	var task models.ModelTask
	for i, t := range ts {
		field := fmt.Sprintf("targets[%d]", i)
		d, err := a.Store.Routes.ResolveDeployment(ctx, q, orgID, t.DeploymentID, t.Deployment)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, unprocessable(field+": no such deployment in this organization", "deployment_not_found", field)
			}
			return nil, httpx.ErrInternal(err)
		}
		if seen[d.ID] {
			return nil, httpx.ErrInvalidRequest(field+": deployment "+d.Name+" appears twice", "duplicate_target", field)
		}
		seen[d.ID] = true
		if d.Task == models.TaskEmbedding {
			return nil, unprocessable(field+": embedding models cannot be routed yet; no runtime serves embeddings",
				"unsupported_task", field)
		}
		if task != "" && d.Task != task {
			return nil, unprocessable(
				fmt.Sprintf("%s: deployment %s serves %s but the route's other targets serve %s; a route is one capability",
					field, d.Name, d.Task, task), "mixed_tasks", field)
		}
		task = d.Task
		id, err := db.NewID()
		if err != nil {
			return nil, httpx.ErrInternal(err)
		}
		out = append(out, &models.RouteTarget{ID: id, DeploymentID: d.ID, Weight: *t.Weight,
			IsBaseline: t.IsBaseline, Label: t.Label})
	}
	return out, nil
}

// resolvePolicy finds a routing policy by name.
func (a *API) resolvePolicy(ctx context.Context, q store.Querier, orgID uuid.UUID, name string) (*uuid.UUID, error) {
	p, err := a.Store.Policies.GetByName(ctx, q, orgID, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, unprocessable(
				fmt.Sprintf("no routing policy named %q; GET /v1/policies/routing lists them", name),
				"routing_policy_not_found", "routing_policy")
		}
		return nil, httpx.ErrInternal(err)
	}
	return &p.ID, nil
}

func (a *API) routeResponse(ctx context.Context, q store.Querier, orgID uuid.UUID, rt *models.Route) (routeResponse, error) {
	resp := routeResponse{ID: rt.ID, Object: "route", ModelName: rt.ModelName,
		CreatedAt: rt.CreatedAt, UpdatedAt: rt.UpdatedAt, Note: routeNote, Targets: []routeTargetResponse{}}
	if rt.RoutingPolicyID != nil {
		p, err := a.Store.Policies.Get(ctx, q, orgID, *rt.RoutingPolicyID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return resp, httpx.ErrInternal(err)
		}
		if p != nil {
			resp.RoutingPolicy = &routePolicyResponse{ID: p.ID, Name: p.Name, Strategy: p.Strategy}
		}
	}
	ts, err := a.Store.Routes.Targets(ctx, q, rt.ID)
	if err != nil {
		return resp, httpx.ErrInternal(err)
	}
	for _, t := range ts {
		resp.Targets = append(resp.Targets, routeTargetResponse{
			DeploymentID: t.DeploymentID, Deployment: t.DeploymentName,
			ModelVersionID: t.ModelVersionID, ModelVersion: t.ModelVersion,
			Weight: t.Weight, Label: t.Label, IsBaseline: t.IsBaseline, State: string(t.DeploymentState),
		})
	}
	return resp, nil
}

// routeCommitError turns the deferred weight trigger's COMMIT-time failure, which
// the handler checks cannot normally reach, into a 400 rather than a 409.
func routeCommitError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23514" {
		return httpx.ErrInvalidRequest(pgErr.Message, "invalid_weights", "targets")
	}
	return err
}
