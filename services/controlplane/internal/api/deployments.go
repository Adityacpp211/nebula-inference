package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// The note attached to every response that describes desired state the cluster has
// not yet been told about. It is not decoration: without it, a 202 and a state of
// "pending" could be read as "NEBULA is working on it", and in Phase 2 nothing is.
const pendingControllerNote = "no reconciliation has happened: the Kubernetes controller arrives in " +
	"Phase 5. This response records desired state in PostgreSQL only. status.reconciled stays false " +
	"and status.observed_generation stays 0 until a controller reports otherwise."

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

func (a *API) listDeployments(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}

	var f store.DeploymentFilter
	q := r.URL.Query()
	if raw := q.Get("state"); raw != "" {
		state := models.DeploymentState(raw)
		if !lifecycle.ValidState(state) {
			return httpx.ErrInvalidRequest(
				"state must be one of: "+strings.Join(stateStrings(), ", "), "invalid_state", "state")
		}
		f.State = &state
	}
	if raw := q.Get("model_version_id"); raw != "" {
		id, err := uuidParam(raw, "model_version_id")
		if err != nil {
			return err
		}
		f.ModelVersionID = &id
	}
	if raw := q.Get("model_id"); raw != "" {
		id, err := uuidParam(raw, "model_id")
		if err != nil {
			return err
		}
		f.ModelID = &id
	}

	rows, err := a.Store.Deployments.List(r.Context(), a.Store.Pool(), ident.OrgID, f, p)
	if err != nil {
		return storeError(err, "deployment")
	}

	now := time.Now()
	out := make([]deploymentResponse, 0, len(rows))
	for _, d := range rows {
		out = append(out, newDeploymentResponse(d, now))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(d deploymentResponse) string {
		return d.ID.String()
	}))
	return nil
}

func (a *API) getDeployment(w http.ResponseWriter, r *http.Request) error {
	d, err := a.loadDeployment(r)
	if err != nil {
		return err
	}
	a.write(w, r, http.StatusOK, newDeploymentResponse(d, time.Now()))
	return nil
}

// deploymentStatus is the polling endpoint: the same status object the full
// response embeds, without the spec. Separate because a client polling every second
// should not re-transfer a spec that changes on human timescales.
func (a *API) deploymentStatus(w http.ResponseWriter, r *http.Request) error {
	d, err := a.loadDeployment(r)
	if err != nil {
		return err
	}
	full := newDeploymentResponse(d, time.Now())
	a.write(w, r, http.StatusOK, struct {
		ID              uuid.UUID           `json:"id"`
		Generation      int64               `json:"generation"`
		CurrentRevision int32               `json:"current_revision"`
		Status          deploymentStatusDTO `json:"status"`
	}{d.ID, d.Generation, d.CurrentRevision, full.Status})
	return nil
}

func (a *API) listRevisions(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}
	p, err := page(r)
	if err != nil {
		return err
	}
	if _, err := a.Store.Deployments.Get(r.Context(), a.Store.Pool(), ident.OrgID, id); err != nil {
		return storeError(err, "deployment")
	}

	rows, err := a.Store.Deployments.ListRevisions(r.Context(), a.Store.Pool(), ident.OrgID, id, p)
	if err != nil {
		return storeError(err, "deployment revision")
	}
	out := make([]revisionResponse, 0, len(rows))
	for _, rev := range rows {
		out = append(out, newRevisionResponse(rev))
	}
	// Revisions are ordered by revision number, which is dense and small, so the
	// page is complete rather than cursored: a deployment with more than 200
	// revisions is worth paging, and limit already bounds it.
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(rev revisionResponse) string {
		return rev.ID.String()
	}))
	return nil
}

func (a *API) listTransitions(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}
	p, err := page(r)
	if err != nil {
		return err
	}
	if _, err := a.Store.Deployments.Get(r.Context(), a.Store.Pool(), ident.OrgID, id); err != nil {
		return storeError(err, "deployment")
	}

	rows, err := a.Store.Deployments.ListStateTransitions(r.Context(), a.Store.Pool(), ident.OrgID, id, p)
	if err != nil {
		return storeError(err, "deployment")
	}
	out := make([]transitionResponse, 0, len(rows))
	for _, t := range rows {
		out = append(out, newTransitionResponse(t))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(t transitionResponse) string {
		return t.ID.String()
	}))
	return nil
}

// ---------------------------------------------------------------------------
// creation
// ---------------------------------------------------------------------------

// createDeployment records the intent to run a model version.
//
// It returns 202 Accepted, never 201 Created. A deployment is a reconciled intent:
// the row exists, and nothing in the cluster does yet. Returning "created" would be
// a claim about pods that the control plane has not verified and, in Phase 2, could
// not verify (docs/api.md §4).
//
// The version must be READY. That check is the registry earning its place: a
// deployment of an uploading version would fail minutes later inside a pod, with
// the cause buried in kubelet events instead of in this response.
func (a *API) createDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())

	var req createDeploymentRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.Name, "name", deploymentNamePattern,
		"name must be lowercase alphanumeric with dashes, 1-47 characters, starting and ending "+
			"with a letter or digit: it becomes a Kubernetes object name"); err != nil {
		return err
	}
	if req.ModelVersionID == uuid.Nil {
		return httpx.ErrInvalidRequest(
			"model_version_id is required", "missing_field", "model_version_id")
	}

	desired := int32(1)
	if req.Replicas != nil {
		desired = *req.Replicas
	}
	minReplicas, maxReplicas := desired, desired
	if req.MinReplicas != nil {
		minReplicas = *req.MinReplicas
	}
	if req.MaxReplicas != nil {
		maxReplicas = *req.MaxReplicas
	}
	if err := replicaBounds(desired, minReplicas, maxReplicas); err != nil {
		return err
	}

	resources, err := requireJSONObject(req.Resources, "resources")
	if err != nil {
		return err
	}
	overrides, err := requireJSONObject(req.RuntimeOverrides, "runtime_overrides")
	if err != nil {
		return err
	}
	autoscaling, err := requireJSONObject(req.Autoscaling, "autoscaling")
	if err != nil {
		return err
	}
	queue, err := requireJSONObject(req.Queue, "queue")
	if err != nil {
		return err
	}

	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}
	revisionID, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}

	d := &models.Deployment{
		ID:               id,
		OrgID:            ident.OrgID,
		ModelVersionID:   req.ModelVersionID,
		Name:             req.Name,
		DesiredReplicas:  desired,
		MinReplicas:      minReplicas,
		MaxReplicas:      maxReplicas,
		Resources:        resources,
		RuntimeOverrides: overrides,
		Autoscaling:      autoscaling,
		QueueConfig:      queue,
		RoutingPolicyID:  req.RoutingPolicyID,
		CreatedBy:        createdBy(ident),
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		version, err := a.Store.Versions.Get(ctx, q, ident.OrgID, req.ModelVersionID)
		if err != nil {
			return storeError(err, "model version")
		}
		if version.Status != models.VersionReady {
			return unprocessable(
				fmt.Sprintf("model version is %s; only a ready version can be deployed", version.Status),
				"version_not_ready", "model_version_id")
		}

		// The namespace defaults from the organization, not from a constant: a
		// tenant's objects belong in the namespace its quota and network policy are
		// attached to.
		namespace := ""
		if req.Namespace != nil {
			namespace = strings.TrimSpace(*req.Namespace)
		}
		if namespace == "" {
			org, err := a.Store.Organizations.Get(ctx, q, ident.OrgID)
			if err != nil {
				return storeError(err, "organization")
			}
			namespace = org.DefaultNamespace
		}
		if err := requireName(namespace, "namespace", namespacePattern,
			"namespace must be a valid Kubernetes namespace name"); err != nil {
			return err
		}
		d.Namespace = namespace

		rev, err := a.Store.Deployments.Create(ctx, q, d, revisionID)
		if err != nil {
			return storeError(err, "deployment")
		}

		entry := auditFor(r, store.ActionDeploymentCreate, store.ResourceDeployment)
		entry.ResourceID = &d.ID
		entry.After = map[string]any{
			"deployment": newDeploymentResponse(d, time.Now()),
			"revision":   newRevisionResponse(rev),
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	w.Header().Set("Location", "/v1/deployments/"+d.ID.String())
	a.write(w, r, http.StatusAccepted, createdDeploymentResponse{
		deploymentResponse: newDeploymentResponse(d, time.Now()),
		Note:               pendingControllerNote,
	})
	return nil
}

// ---------------------------------------------------------------------------
// spec changes
// ---------------------------------------------------------------------------

// updateDeployment changes desired state and produces a new revision.
//
// The caller must send the generation it read. That is not ceremony: two operators
// editing the same deployment from two terminals is ordinary, and without the
// precondition the second write silently discards the first. With it, the second
// gets a 409 naming the generation that exists now.
func (a *API) updateDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}
	var req updateDeploymentRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if req.Generation <= 0 {
		return httpx.ErrInvalidRequest(
			"generation is required: send the generation you read, so a concurrent change is detected "+
				"rather than overwritten", "missing_field", "generation")
	}

	var out *models.Deployment
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		snapshot := *before

		d := before
		if req.ModelVersionID != nil && *req.ModelVersionID != d.ModelVersionID {
			version, err := a.Store.Versions.Get(ctx, q, ident.OrgID, *req.ModelVersionID)
			if err != nil {
				return storeError(err, "model version")
			}
			if version.Status != models.VersionReady {
				return unprocessable(
					fmt.Sprintf("model version is %s; only a ready version can be deployed", version.Status),
					"version_not_ready", "model_version_id")
			}
			d.ModelVersionID = *req.ModelVersionID
		}
		if req.Replicas != nil {
			d.DesiredReplicas = *req.Replicas
		}
		if req.MinReplicas != nil {
			d.MinReplicas = *req.MinReplicas
		}
		if req.MaxReplicas != nil {
			d.MaxReplicas = *req.MaxReplicas
		}
		if err := replicaBounds(d.DesiredReplicas, d.MinReplicas, d.MaxReplicas); err != nil {
			return err
		}
		if err := applyJSONField(req.Resources, "resources", &d.Resources); err != nil {
			return err
		}
		if err := applyJSONField(req.RuntimeOverrides, "runtime_overrides", &d.RuntimeOverrides); err != nil {
			return err
		}
		if err := applyJSONField(req.Autoscaling, "autoscaling", &d.Autoscaling); err != nil {
			return err
		}
		if err := applyJSONField(req.Queue, "queue", &d.QueueConfig); err != nil {
			return err
		}
		if req.RoutingPolicyID != nil {
			d.RoutingPolicyID = req.RoutingPolicyID
		}

		revisionID, err := db.NewID()
		if err != nil {
			return httpx.ErrInternal(err)
		}
		rev, err := a.Store.Deployments.UpdateSpec(ctx, q, ident.OrgID, d,
			req.Generation, models.ReasonUpdate, revisionID)
		if err != nil {
			return storeError(err, "deployment")
		}
		out = d

		entry := auditFor(r, store.ActionDeploymentUpdate, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = newDeploymentResponse(&snapshot, time.Now())
		entry.After = map[string]any{
			"deployment": newDeploymentResponse(d, time.Now()),
			"revision":   newRevisionResponse(rev),
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusOK, createdDeploymentResponse{
		deploymentResponse: newDeploymentResponse(out, time.Now()),
		Note:               pendingControllerNote,
	})
	return nil
}

// scaleDeployment changes the replica count.
//
// It is its own endpoint rather than a PATCH because it is a different operation:
// it records reason "scale" in the revision history, so a spec diff caused by
// scaling is distinguishable from one caused by an edit — which matters when
// reading back why a deployment changed at 02:00. It takes no generation
// precondition: scaling is idempotent in intent and is also what the autoscaler
// will call (Phase 6), where a generation round-trip would be pure contention.
func (a *API) scaleDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}
	var req scaleRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}

	var out *models.Deployment
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		snapshot := *before

		d := before
		d.DesiredReplicas = req.Replicas
		if req.MinReplicas != nil {
			d.MinReplicas = *req.MinReplicas
		}
		if req.MaxReplicas != nil {
			d.MaxReplicas = *req.MaxReplicas
		}
		// Scaling outside the current bounds is a mistake worth naming rather than
		// silently widening them: a request for 12 replicas against max 5 is either a
		// typo or a bounds change, and the caller knows which.
		if err := replicaBounds(d.DesiredReplicas, d.MinReplicas, d.MaxReplicas); err != nil {
			return err
		}

		revisionID, err := db.NewID()
		if err != nil {
			return httpx.ErrInternal(err)
		}
		rev, err := a.Store.Deployments.UpdateSpec(ctx, q, ident.OrgID, d,
			before.Generation, models.ReasonScale, revisionID)
		if err != nil {
			return storeError(err, "deployment")
		}
		out = d

		entry := auditFor(r, store.ActionDeploymentScale, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = map[string]any{
			"desired_replicas": snapshot.DesiredReplicas,
			"min_replicas":     snapshot.MinReplicas,
			"max_replicas":     snapshot.MaxReplicas,
		}
		entry.After = map[string]any{
			"desired_replicas": d.DesiredReplicas,
			"min_replicas":     d.MinReplicas,
			"max_replicas":     d.MaxReplicas,
			"revision":         rev.Revision,
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusAccepted, createdDeploymentResponse{
		deploymentResponse: newDeploymentResponse(out, time.Now()),
		Note:               pendingControllerNote,
	})
	return nil
}

// rollbackDeployment restores an earlier revision's spec as a new revision.
//
// Omitting revision means "the one before current", which is what an operator wants
// at 02:00 and is the only form that needs no lookup first.
func (a *API) rollbackDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}

	var req rollbackRequest
	// An empty body is legal here and means "the previous revision".
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			return err
		}
	}
	if req.Revision != nil && *req.Revision < 1 {
		return httpx.ErrInvalidRequest("revision must be positive", "invalid_revision", "revision")
	}

	var out *models.Deployment
	var target int32
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		snapshot := *before

		target = before.CurrentRevision - 1
		if req.Revision != nil {
			target = *req.Revision
		}
		if target < 1 {
			return conflict(
				"there is no earlier revision to roll back to", "no_previous_revision")
		}

		revisionID, err := db.NewID()
		if err != nil {
			return httpx.ErrInternal(err)
		}
		d, rev, err := a.Store.Deployments.Rollback(ctx, q, ident.OrgID, id, target, revisionID)
		if err != nil {
			return storeError(err, "deployment revision")
		}
		out = d

		entry := auditFor(r, store.ActionDeploymentRollback, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = newDeploymentResponse(&snapshot, time.Now())
		entry.After = map[string]any{
			"rolled_back_to_revision": target,
			"new_revision":            rev.Revision,
			"deployment":              newDeploymentResponse(d, time.Now()),
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusAccepted, struct {
		createdDeploymentResponse
		RolledBackToRevision int32 `json:"rolled_back_to_revision"`
	}{
		createdDeploymentResponse: createdDeploymentResponse{
			deploymentResponse: newDeploymentResponse(out, time.Now()),
			Note:               pendingControllerNote,
		},
		RolledBackToRevision: target,
	})
	return nil
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

// stopDeployment asks for a deployment to be wound down.
//
// It moves the deployment to STOPPING and stops there. Stopping means "the intent
// to stop is recorded"; only something that has observed the replicas going away may
// write STOPPED, and that is the controller's job (Phase 5). Marking it stopped
// here would be a claim about pods that nothing has checked — and in Phase 2 there
// are no pods to check, which is precisely why the temptation has to be refused
// (axiom A6).
func (a *API) stopDeployment(w http.ResponseWriter, r *http.Request) error {
	return a.lifecycleAction(w, r, lifecycleAction{
		to:          models.DeploymentStopping,
		reason:      lifecycle.ReasonStopRequested,
		auditAction: store.ActionDeploymentStop,
		note: "the deployment is now stopping: the intent is recorded. Only a controller that has " +
			"observed the replicas terminating may mark it stopped, and that controller arrives in " +
			"Phase 5. Until then a stopping deployment stays stopping.",
		refuse: func(d *models.Deployment) error {
			if d.State == models.DeploymentStopped || d.State == models.DeploymentStopping {
				return conflict("the deployment is already "+string(d.State), "already_stopping")
			}
			return nil
		},
	})
}

// startDeployment restarts a stopped deployment.
//
// Only from STOPPED. A failed deployment is not restarted by asking again: the way
// out of failed is a new revision, which is what the controller reconciles
// (failed -> provisioning). Offering "start" on a failed deployment would invite
// retrying an unchanged spec that will fail identically.
func (a *API) startDeployment(w http.ResponseWriter, r *http.Request) error {
	return a.lifecycleAction(w, r, lifecycleAction{
		to:          models.DeploymentPending,
		reason:      lifecycle.ReasonStartRequested,
		auditAction: store.ActionDeploymentStart,
		note:        pendingControllerNote,
		refuse: func(d *models.Deployment) error {
			if d.State != models.DeploymentStopped {
				return conflict(fmt.Sprintf(
					"only a stopped deployment can be started; this one is %s. A failed deployment "+
						"is recovered by updating its spec, which the controller reconciles.", d.State),
					"not_stopped")
			}
			return nil
		},
	})
}

// lifecycleAction is the shared shape of stop and start.
type lifecycleAction struct {
	to          models.DeploymentState
	reason      lifecycle.Reason
	auditAction string
	note        string
	refuse      func(*models.Deployment) error
}

func (a *API) lifecycleAction(w http.ResponseWriter, r *http.Request, act lifecycleAction) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}

	var out *models.Deployment
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		if act.refuse != nil {
			if err := act.refuse(before); err != nil {
				return err
			}
		}

		if _, err := a.Store.Deployments.TransitionState(ctx, q, ident.OrgID, id,
			before.State, act.to, act.reason, nil); err != nil {
			return storeError(err, "deployment")
		}

		after, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		out = after

		entry := auditFor(r, act.auditAction, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = map[string]any{"state": string(before.State)}
		entry.After = map[string]any{"state": string(after.State), "reason": string(act.reason)}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusAccepted, createdDeploymentResponse{
		deploymentResponse: newDeploymentResponse(out, time.Now()),
		Note:               act.note,
	})
	return nil
}

// transitionDeployment performs an explicit state transition.
//
// This is an operator tool and is admin-scoped. It exists for two honest reasons:
//
//   - During an incident, desired state has to be correctable. A deployment wedged
//     in STOPPING because a controller died needs a way out that is recorded rather
//     than a hand-written UPDATE that is not.
//   - Until Phase 5, nothing else advances an in-flight state, so this is how the
//     state machine is exercised end to end. That is stated here rather than hidden:
//     the transition is attributed to the calling credential in
//     deployment_state_transitions, so a state that a human set is distinguishable
//     from one a controller observed.
//
// The caller must send the state it believes the deployment is in. The transition is
// a compare-and-set on that value, and the database independently refuses an edge
// that is not in deployment_state_edges.
func (a *API) transitionDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}

	var req transitionRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireEnum(req.From, "from", stateStrings()); err != nil {
		return err
	}
	if err := requireEnum(req.To, "to", stateStrings()); err != nil {
		return err
	}
	from := models.DeploymentState(req.From)
	to := models.DeploymentState(req.To)

	reason := lifecycle.Reason(req.Reason)
	if req.Reason == "" {
		return httpx.ErrInvalidRequest(
			"reason is required: a transition with no recorded cause cannot be explained afterwards",
			"missing_field", "reason")
	}
	if !lifecycle.ValidReason(reason) {
		return httpx.ErrInvalidRequest(
			"reason must be one of: "+strings.Join(reasonStrings(), ", "), "invalid_reason", "reason")
	}
	if !lifecycle.CanTransition(from, to) {
		allowed := lifecycle.NextStates(from)
		names := make([]string, 0, len(allowed))
		for _, s := range allowed {
			names = append(names, string(s))
		}
		message := fmt.Sprintf("a deployment cannot move from %s to %s", from, to)
		if len(names) > 0 {
			message += "; from " + string(from) + " it may move to: " + strings.Join(names, ", ")
		} else {
			message += "; " + string(from) + " is terminal"
		}
		return unprocessable(message, "illegal_transition", "to")
	}

	var out *models.Deployment
	var outcome lifecycle.Outcome
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}

		outcome, err = a.Store.Deployments.TransitionState(ctx, q, ident.OrgID, id,
			from, to, reason, req.Message)
		if err != nil {
			return storeError(err, "deployment")
		}

		after, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		out = after

		entry := auditFor(r, store.ActionDeploymentTransition, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = map[string]any{"state": string(before.State)}
		entry.After = map[string]any{
			"state":   string(after.State),
			"reason":  string(reason),
			"message": req.Message,
			"manual":  true,
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusOK, struct {
		deploymentResponse
		Changed bool `json:"changed"`
	}{
		deploymentResponse: newDeploymentResponse(out, time.Now()),
		Changed:            outcome == lifecycle.OutcomeChanged,
	})
	return nil
}

// deleteDeployment soft-deletes a deployment.
//
// It refuses unless the deployment is resting. A deployment with pods still running
// must be stopped first, so the controller reconciles an explicit stop rather than
// discovering that the desired state it was working towards has vanished.
func (a *API) deleteDeployment(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return err
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Deployments.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "deployment")
		}
		if err := a.Store.Deployments.SoftDelete(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "deployment")
		}
		entry := auditFor(r, store.ActionDeploymentDelete, store.ResourceDeployment)
		entry.ResourceID = &id
		entry.Before = newDeploymentResponse(before, time.Now())
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// loadDeployment reads the deployment named in the path, outside a transaction.
func (a *API) loadDeployment(r *http.Request) (*models.Deployment, error) {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "deployment_id", "deployment")
	if err != nil {
		return nil, err
	}
	d, err := a.Store.Deployments.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return nil, storeError(err, "deployment")
	}
	return d, nil
}

// applyJSONField overwrites a jsonb field only when the patch supplied one.
//
// An absent field and an explicit null are both "leave it alone", because the
// database columns are NOT NULL: there is no value a caller could mean by null.
func applyJSONField(in json.RawMessage, field string, dst *json.RawMessage) error {
	if strings.TrimSpace(string(in)) == "" || string(in) == "null" {
		return nil
	}
	validated, err := requireJSONObject(in, field)
	if err != nil {
		return err
	}
	*dst = validated
	return nil
}

// stateStrings lists the deployment states, for validation messages.
func stateStrings() []string {
	all := lifecycle.States()
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, string(s))
	}
	return out
}

// reasonStrings lists the transition reasons.
func reasonStrings() []string {
	all := lifecycle.Reasons()
	out := make([]string, 0, len(all))
	for _, r := range all {
		out = append(out, string(r))
	}
	return out
}
