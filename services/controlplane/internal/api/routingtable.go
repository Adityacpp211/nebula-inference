package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
)

// The routing table is what every gateway replica serves from: every route of
// every organization, with its policy and weighted targets (docs/architecture.md
// §6.2). Gateways poll it and cache the last good copy in Redis, so a gateway that
// starts while the control plane is down still routes (axiom A8).
//
// It carries no endpoints. Which pods serve a deployment is Kubernetes' fact, read
// by each gateway from EndpointSlices, and copying it here would make a second,
// slower source of truth for it (axiom A1).

// routingTableResponse is versioned by a hash of its content, so a poll that
// changes nothing is answered 304 and costs the gateway no rebuild.
type routingTableResponse struct {
	Version string              `json:"version"`
	Routes  []routingTableRoute `json:"routes"`
}

type routingTableRoute struct {
	ID            string               `json:"id"`
	Org           string               `json:"org"`
	OrgID         uuid.UUID            `json:"org_id"`
	Model         string               `json:"model"`
	Task          string               `json:"task"`
	ContextWindow int32                `json:"context_window"`
	ChatTemplate  string               `json:"chat_template"`
	Created       int64                `json:"created"`
	Capabilities  routingCapabilities  `json:"capabilities"`
	Policy        routingPolicy        `json:"policy"`
	Targets       []routingTableTarget `json:"targets"`
}

type routingCapabilities struct {
	Streaming bool `json:"streaming"`
	JSONMode  bool `json:"json_mode"`
}

type routingPolicy struct {
	Strategy string          `json:"strategy"`
	Config   json.RawMessage `json:"config,omitempty"`
}

type routingTableTarget struct {
	Deployment    string    `json:"deployment"`
	DeploymentID  uuid.UUID `json:"deployment_id"`
	ModelVersion  string    `json:"model_version"`
	Weight        int32     `json:"weight"`
	Label         string    `json:"label,omitempty"`
	State         string    `json:"state"`
	Namespace     string    `json:"namespace"`
	ContextWindow int32     `json:"context_window"`
}

// chatTemplates are the templates the gateway can render.
var chatTemplates = map[string]bool{"chatml": true, "llama3": true, "plain": true}

// templateOf picks a deployment's chat template: the deployment's override, then
// the version's runtime config, then chatml. Reading it from GGUF metadata is
// TODO(NEB-144).
func templateOf(overrides, versionConfig json.RawMessage) string {
	for _, raw := range []json.RawMessage{overrides, versionConfig} {
		var v struct {
			ChatTemplate string `json:"chat_template"`
		}
		if json.Unmarshal(raw, &v) == nil && chatTemplates[v.ChatTemplate] {
			return v.ChatTemplate
		}
	}
	return "chatml"
}

func (a *API) routingTable(w http.ResponseWriter, r *http.Request) error {
	rows, err := a.Store.Routes.RoutingTable(r.Context(), a.Store.Pool())
	if err != nil {
		return httpx.ErrInternal(err)
	}

	var routes []routingTableRoute
	index := map[uuid.UUID]int{}
	for _, row := range rows {
		i, ok := index[row.RouteID]
		if !ok {
			strategy := ""
			if row.Strategy != nil {
				strategy = *row.Strategy
			}
			routes = append(routes, routingTableRoute{
				ID: row.RouteID.String(), Org: row.OrgSlug, OrgID: row.OrgID, Model: row.ModelName,
				Task:         string(row.Task),
				ChatTemplate: templateOf(row.Overrides, row.RuntimeConfig),
				Created:      row.RouteCreatedAt.Unix(),
				// Both runtimes stream; only llama.cpp constrains output to JSON.
				Capabilities: routingCapabilities{Streaming: true, JSONMode: true},
				Policy:       routingPolicy{Strategy: strategy, Config: row.StrategyConfig},
			})
			i = len(routes) - 1
			index[row.RouteID] = i
		}
		rt := &routes[i]
		label := ""
		if row.TargetLabel != nil {
			label = *row.TargetLabel
		}
		rt.Targets = append(rt.Targets, routingTableTarget{
			Deployment: row.DeploymentName, DeploymentID: row.DeploymentID, ModelVersion: row.ModelVersion,
			Weight: row.TargetWeight, Label: label, State: string(row.DeploymentState),
			Namespace: row.Namespace, ContextWindow: row.ContextWindow,
		})
		// A route's window is its largest target's: the gateway sends a long request
		// only to a target that can hold it.
		if row.ContextWindow > rt.ContextWindow {
			rt.ContextWindow = row.ContextWindow
		}
		if row.Runtime != models.RuntimeLlamaCPP {
			rt.Capabilities.JSONMode = false
		}
	}
	if routes == nil {
		routes = []routingTableRoute{}
	}

	body, err := json.Marshal(routes)
	if err != nil {
		return httpx.ErrInternal(err)
	}
	sum := sha256.Sum256(body)
	version := hex.EncodeToString(sum[:16])
	etag := `"` + version + `"`
	w.Header().Set("ETag", etag)
	if match := r.Header.Get("If-None-Match"); match != "" && strings.Contains(match, etag) {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	a.write(w, r, http.StatusOK, routingTableResponse{Version: version, Routes: routes})
	return nil
}
