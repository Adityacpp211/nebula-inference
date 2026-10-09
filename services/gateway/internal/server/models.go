package server

import (
	"fmt"
	"net/http"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/openai"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

// listModels is GET /v1/models: the routes the caller's organization can invoke,
// in OpenAI's shape with a nebula block (docs/api.md §2). A route is listed, not a
// deployment — "model" in a request names a route.
func (g *Gateway) listModels(w http.ResponseWriter, r *http.Request) {
	ident := auth.MustFromContext(r.Context())
	rs := g.d.Router.Table().ForOrg(ident.OrgSlug)
	out := openai.ModelList{Object: openai.ObjectList, Data: make([]openai.Model, 0, len(rs))}
	for _, rt := range rs {
		out.Data = append(out.Data, g.modelOf(rt))
	}
	_ = httpx.WriteJSON(w, http.StatusOK, out)
}

// retrieveModel is GET /v1/models/{model} for a route name.
func (g *Gateway) retrieveModel(w http.ResponseWriter, r *http.Request) {
	ident := auth.MustFromContext(r.Context())
	name := r.PathValue("model")
	rt, ok := g.d.Router.Table().Lookup(ident.OrgSlug, name)
	if !ok {
		g.fail(w, r, &httpx.APIError{
			Status:  http.StatusNotFound,
			Message: fmt.Sprintf("the model %q does not exist or you do not have access to it", name),
			Type:    httpx.TypeNotFound,
			Code:    "model_not_found",
			Param:   "model",
		})
		return
	}
	_ = httpx.WriteJSON(w, http.StatusOK, g.modelOf(rt))
}

func (g *Gateway) modelOf(rt *routes.Route) openai.Model {
	targets := make([]openai.ModelTarget, 0, len(rt.Targets))
	for _, t := range rt.Targets {
		targets = append(targets, openai.ModelTarget{
			Deployment: t.Deployment, ModelVersion: t.ModelVersion, Weight: t.Weight, Label: t.Label,
			State: t.State, ReadyEndpoints: g.d.Router.Eligible(rt, t),
		})
	}
	return openai.Model{
		ID:      rt.Model,
		Object:  openai.ObjectModel,
		Created: rt.Created,
		OwnedBy: rt.Org,
		Nebula: &openai.ModelExtension{
			RouteID:       rt.ID,
			ContextWindow: rt.ContextWindow,
			Task:          string(rt.Task),
			Targets:       targets,
			Streaming:     rt.Capabilities.Streaming,
			Embeddings:    false,
			Source:        g.d.RouteSource,
		},
	}
}
