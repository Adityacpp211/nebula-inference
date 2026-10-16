package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/packages/rollout"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// Canary rollouts (docs/architecture.md §7.3, docs/api.md "Routes, rollouts,
// experiments"). The control plane creates a rollout and applies an operator's
// verbs; the controller's rollout reconciler moves the weights step by step and
// aborts on a breach. Both write as a compare-and-set on the rollout's state, so
// an operator's pause or abort always wins over a reconciler tick in flight.

type createRolloutRequest struct {
	RouteID            *uuid.UUID      `json:"route_id"`
	Route              string          `json:"route"`
	CanaryDeploymentID *uuid.UUID      `json:"canary_deployment_id"`
	CanaryDeployment   string          `json:"canary_deployment"`
	Steps              json.RawMessage `json:"steps"`
	Analysis           json.RawMessage `json:"analysis"`
}

type abortRolloutRequest struct {
	Reason string `json:"reason"`
}

type rolloutResponse struct {
	ID                   uuid.UUID       `json:"id"`
	RouteID              uuid.UUID       `json:"route_id"`
	BaselineDeploymentID uuid.UUID       `json:"baseline_deployment_id"`
	CanaryDeploymentID   uuid.UUID       `json:"canary_deployment_id"`
	Steps                json.RawMessage `json:"steps"`
	CurrentStep          int32           `json:"current_step"`
	CanaryWeight         int32           `json:"canary_weight"`
	Analysis             json.RawMessage `json:"analysis"`
	State                string          `json:"state"`
	LastAnalysis         json.RawMessage `json:"last_analysis"`
	AbortReason          *string         `json:"abort_reason"`
	StartedAt            *time.Time      `json:"started_at"`
	StepEnteredAt        *time.Time      `json:"step_entered_at"`
	FinishedAt           *time.Time      `json:"finished_at"`
	CreatedAt            time.Time       `json:"created_at"`
}

func (a *API) rolloutResponse(ctx context.Context, q store.Querier, ro *models.CanaryRollout) (rolloutResponse, error) {
	w, err := a.Store.Rollouts.CanaryWeight(ctx, q, ro)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return rolloutResponse{}, httpx.ErrInternal(err)
	}
	last := ro.LastAnalysis
	if len(last) == 0 {
		last = json.RawMessage("null")
	}
	return rolloutResponse{
		ID: ro.ID, RouteID: ro.RouteID, BaselineDeploymentID: ro.BaselineDeploymentID,
		CanaryDeploymentID: ro.CanaryDeploymentID, Steps: ro.Steps, CurrentStep: ro.CurrentStep, CanaryWeight: w,
		Analysis: ro.Analysis, State: string(ro.State), LastAnalysis: last, AbortReason: ro.AbortReason,
		StartedAt: ro.StartedAt, StepEnteredAt: ro.StepEnteredAt, FinishedAt: ro.FinishedAt, CreatedAt: ro.CreatedAt,
	}, nil
}

// createRollout starts a canary on a route that sends everything to one
// deployment. The canary must be ready: a rollout to replicas that are not serving
// would analyse nothing and abort, or worse, pass on no data.
func (a *API) createRollout(w http.ResponseWriter, r *http.Request) error {
	var req createRolloutRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if req.RouteID == nil && req.Route == "" {
		return httpx.ErrInvalidRequest("route_id or route is required", "missing_field", "route")
	}
	if req.CanaryDeploymentID == nil && req.CanaryDeployment == "" {
		return httpx.ErrInvalidRequest("canary_deployment_id or canary_deployment is required", "missing_field",
			"canary_deployment")
	}
	steps, err := rollout.Steps(req.Steps)
	if err != nil {
		return httpx.ErrInvalidRequest(err.Error(), "invalid_steps", "steps")
	}
	analysis, err := rollout.ParseAnalysis(req.Analysis)
	if err != nil {
		return httpx.ErrInvalidRequest(err.Error(), "invalid_analysis", "analysis")
	}
	ident := auth.MustFromContext(r.Context())
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	// Stored resolved, defaults included, so the row says exactly what the gate is.
	stepsJSON, _ := json.Marshal(steps)
	analysisJSON, _ := json.Marshal(analysis)
	ro := &models.CanaryRollout{ID: id, OrgID: ident.OrgID, Steps: stepsJSON, Analysis: analysisJSON,
		State: models.RolloutPending}
	ro.CreatedBy = ident.UserID

	var resp rolloutResponse
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		rt, err := a.resolveRoute(ctx, q, ident.OrgID, req.RouteID, req.Route)
		if err != nil {
			return err
		}
		targets, err := a.Store.Routes.Targets(ctx, q, rt.ID)
		if err != nil {
			return httpx.ErrInternal(err)
		}
		var baseline *store.TargetView
		for i := range targets {
			if targets[i].Weight == 100 {
				baseline = &targets[i]
			}
		}
		if baseline == nil {
			return conflict("a rollout starts from a route that sends every request to one deployment; "+
				"this route splits its traffic", "route_split")
		}
		canary, err := a.Store.Routes.ResolveDeployment(ctx, q, ident.OrgID, req.CanaryDeploymentID, req.CanaryDeployment)
		if err != nil {
			return unprocessable("no deployment matches canary_deployment", "deployment_not_found",
				"canary_deployment")
		}
		if canary.ID == baseline.DeploymentID {
			return httpx.ErrInvalidRequest("the canary is already the route's baseline", "canary_is_baseline",
				"canary_deployment")
		}
		if !lifecycle.Serving(canary.State) {
			return conflict(fmt.Sprintf("the canary deployment is %s; it must be serving before traffic shifts to it",
				canary.State), "canary_not_ready")
		}
		ro.RouteID, ro.BaselineDeploymentID, ro.CanaryDeploymentID = rt.ID, baseline.DeploymentID, canary.ID
		if err := a.Store.Rollouts.Create(ctx, q, ro); err != nil {
			if errors.Is(err, store.ErrConflict) {
				return conflict(trimSentinel(err, store.ErrConflict), "rollout_active")
			}
			return storeError(err, "rollout")
		}
		created, err := a.Store.Rollouts.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "rollout")
		}
		if resp, err = a.rolloutResponse(ctx, q, created); err != nil {
			return err
		}
		entry := auditFor(r, store.ActionRolloutCreate, store.ResourceRollout)
		entry.ResourceID = &id
		entry.After = resp
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/v1/rollouts/"+id.String())
	a.write(w, r, http.StatusCreated, resp)
	return nil
}

// resolveRoute finds an org's route by id or by model name.
func (a *API) resolveRoute(ctx context.Context, q store.Querier, orgID uuid.UUID, id *uuid.UUID, name string) (*models.Route, error) {
	var rt *models.Route
	var err error
	if id != nil {
		rt, err = a.Store.Routes.Get(ctx, q, orgID, *id)
	} else {
		rt, err = a.Store.Routes.GetByName(ctx, q, orgID, name)
	}
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, unprocessable("no route matches route", "route_not_found", "route")
		}
		return nil, httpx.ErrInternal(err)
	}
	return rt, nil
}

func (a *API) listRollouts(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}
	var state *models.RolloutState
	if v := r.URL.Query().Get("state"); v != "" {
		s := models.RolloutState(v)
		if !s.Valid() {
			return httpx.ErrInvalidRequest("state is not a rollout state", "invalid_value", "state")
		}
		state = &s
	}
	rows, err := a.Store.Rollouts.List(r.Context(), a.Store.Pool(), ident.OrgID, state, p)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	out := make([]rolloutResponse, 0, len(rows))
	for _, ro := range rows {
		resp, err := a.rolloutResponse(r.Context(), a.Store.Pool(), ro)
		if err != nil {
			return err
		}
		out = append(out, resp)
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(ro rolloutResponse) string { return ro.ID.String() }))
	return nil
}

func (a *API) getRollout(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "rollout_id", "rollout")
	if err != nil {
		return err
	}
	ro, err := a.Store.Rollouts.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "rollout")
	}
	resp, err := a.rolloutResponse(r.Context(), a.Store.Pool(), ro)
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusOK, resp)
	return nil
}

var active = []models.RolloutState{models.RolloutPending, models.RolloutProgressing, models.RolloutPaused}

func (a *API) pauseRollout(w http.ResponseWriter, r *http.Request) error {
	return a.rolloutVerb(w, r, store.ActionRolloutPause, store.Verb{
		From: []models.RolloutState{models.RolloutPending, models.RolloutProgressing}, To: models.RolloutPaused,
		CanaryWeight: -1})
}

// resumeRollout continues a paused rollout with a fresh hold window: the
// observations from before the pause say nothing about the step now.
func (a *API) resumeRollout(w http.ResponseWriter, r *http.Request) error {
	return a.rolloutVerb(w, r, store.ActionRolloutResume, store.Verb{
		From: []models.RolloutState{models.RolloutPaused}, To: models.RolloutProgressing, CanaryWeight: -1,
		Restart: true})
}

// promoteRollout skips the remaining steps: the canary serves every request and
// becomes the route's baseline.
func (a *API) promoteRollout(w http.ResponseWriter, r *http.Request) error {
	return a.rolloutVerb(w, r, store.ActionRolloutPromote, store.Verb{From: active, To: models.RolloutPromoted,
		CanaryWeight: 100, Promote: true, Finish: true})
}

// abortRollout puts every request back on the baseline.
func (a *API) abortRollout(w http.ResponseWriter, r *http.Request) error {
	var req abortRolloutRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			return err
		}
	}
	reason := "aborted by an operator"
	if req.Reason != "" {
		reason += ": " + req.Reason
	}
	return a.rolloutVerb(w, r, store.ActionRolloutAbort, store.Verb{From: active, To: models.RolloutAborted,
		CanaryWeight: 0, Finish: true, AbortReason: reason})
}

func (a *API) rolloutVerb(w http.ResponseWriter, r *http.Request, action string, v store.Verb) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "rollout_id", "rollout")
	if err != nil {
		return err
	}
	var resp rolloutResponse
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Rollouts.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "rollout")
		}
		ro, err := a.Store.Rollouts.Apply(ctx, q, ident.OrgID, id, v)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				return conflict(fmt.Sprintf("the rollout is %s; this is allowed only from %v", before.State, v.From),
					"invalid_state")
			}
			return storeError(err, "rollout")
		}
		if resp, err = a.rolloutResponse(ctx, q, ro); err != nil {
			return err
		}
		entry := auditFor(r, action, store.ResourceRollout)
		entry.ResourceID = &id
		entry.After = resp
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusOK, resp)
	return nil
}
