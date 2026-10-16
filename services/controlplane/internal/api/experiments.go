package api

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/abtest"
	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// A/B experiments (docs/api.md, "Routes, rollouts, experiments"). An experiment
// observes a route's existing weighted split over a window; the split itself is
// the route's targets (PATCH /v1/routes), and bucketing is the gateway's: on the
// OpenAI "user" field when a request carries one, else per request. Results are
// read from the per-request rows the usage ingester stores (Phase 13).

type createExperimentRequest struct {
	Name       string     `json:"name"`
	RouteID    *uuid.UUID `json:"route_id"`
	Route      string     `json:"route"`
	Hypothesis *string    `json:"hypothesis"`
	Bucketing  string     `json:"bucketing"`
}

type stopExperimentRequest struct {
	Conclusion *string `json:"conclusion"`
}

type experimentResults struct {
	ExperimentID uuid.UUID           `json:"experiment_id"`
	From         time.Time           `json:"from"`
	To           time.Time           `json:"to"`
	Arms         []abtest.Arm        `json:"arms"`
	Comparison   []abtest.Comparison `json:"comparison"`
	Significance abtest.Significance `json:"significance"`
}

const (
	experimentSample    = 2000
	experimentResamples = 2000
)

func (a *API) createExperiment(w http.ResponseWriter, r *http.Request) error {
	var req createExperimentRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.Name, "name", modelNamePattern,
		"name must be 1-64 lowercase letters, digits, dots, dashes or underscores"); err != nil {
		return err
	}
	if req.RouteID == nil && req.Route == "" {
		return httpx.ErrInvalidRequest("route_id or route is required", "missing_field", "route")
	}
	bucketing := models.Bucketing(req.Bucketing)
	switch req.Bucketing {
	case "":
		bucketing = "sticky_user"
	case "request", "sticky_user":
	default:
		return httpx.ErrInvalidRequest("bucketing must be request or sticky_user", "invalid_value", "bucketing")
	}
	ident := auth.MustFromContext(r.Context())
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	e := &models.Experiment{ID: id, OrgID: ident.OrgID, Name: req.Name, Hypothesis: req.Hypothesis, Bucketing: bucketing}
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		rt, err := a.resolveRoute(ctx, q, ident.OrgID, req.RouteID, req.Route)
		if err != nil {
			return err
		}
		e.RouteID = rt.ID
		if err := a.Store.Experiments.Create(ctx, q, e); err != nil {
			if errors.Is(err, store.ErrConflict) {
				return conflict(trimSentinel(err, store.ErrConflict), "experiment_conflict")
			}
			return storeError(err, "experiment")
		}
		entry := auditFor(r, store.ActionExperimentCreate, store.ResourceExperiment)
		entry.ResourceID = &id
		entry.After = e
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/v1/experiments/"+id.String())
	a.write(w, r, http.StatusCreated, e)
	return nil
}

func (a *API) listExperiments(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}
	rows, err := a.Store.Experiments.List(r.Context(), a.Store.Pool(), ident.OrgID, p)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	out := make([]models.Experiment, 0, len(rows))
	for _, e := range rows {
		out = append(out, *e)
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(e models.Experiment) string { return e.ID.String() }))
	return nil
}

func (a *API) getExperiment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "experiment_id", "experiment")
	if err != nil {
		return err
	}
	e, err := a.Store.Experiments.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "experiment")
	}
	a.write(w, r, http.StatusOK, e)
	return nil
}

func (a *API) stopExperiment(w http.ResponseWriter, r *http.Request) error {
	var req stopExperimentRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			return err
		}
	}
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "experiment_id", "experiment")
	if err != nil {
		return err
	}
	var e *models.Experiment
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if e, err = a.Store.Experiments.Stop(ctx, q, ident.OrgID, id, req.Conclusion); err != nil {
			if errors.Is(err, store.ErrConflict) {
				if _, gerr := a.Store.Experiments.Get(ctx, q, ident.OrgID, id); errors.Is(gerr, store.ErrNotFound) {
					return storeError(gerr, "experiment")
				}
				return conflict(trimSentinel(err, store.ErrConflict), "invalid_state")
			}
			return storeError(err, "experiment")
		}
		entry := auditFor(r, store.ActionExperimentStop, store.ResourceExperiment)
		entry.ResourceID = &id
		entry.After = e
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusOK, e)
	return nil
}

// experimentResults compares every arm with the baseline over the experiment's
// window, with bootstrap intervals and the significance disclaimer.
func (a *API) experimentResults(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "experiment_id", "experiment")
	if err != nil {
		return err
	}
	ctx := r.Context()
	e, err := a.Store.Experiments.Get(ctx, a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "experiment")
	}
	from := e.CreatedAt
	if e.StartedAt != nil {
		from = *e.StartedAt
	}
	to := time.Now().UTC()
	if e.StoppedAt != nil {
		to = *e.StoppedAt
	}
	arms, err := a.Store.Experiments.Arms(ctx, a.Store.Pool(), e.RouteID, from, to, experimentSample)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	if arms == nil {
		arms = []abtest.Arm{}
	}
	res := experimentResults{ExperimentID: id, From: from, To: to, Arms: arms, Comparison: []abtest.Comparison{},
		Significance: abtest.Disclaimer()}
	// The resampling is seeded by the experiment, so a stopped experiment's
	// results are the same every time they are read.
	seed := binary.BigEndian.Uint64(id[8:])
	for i := 1; i < len(arms); i++ {
		res.Comparison = append(res.Comparison, abtest.Compare(arms[0], arms[i], experimentResamples, seed))
	}
	a.write(w, r, http.StatusOK, res)
	return nil
}
