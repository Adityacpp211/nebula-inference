// Package routes is the gateway's route table: which routes exist, for which
// organization, with which weighted targets. It answers "which route does this org
// mean by this model name?". Which endpoint serves a request is the router's
// question (services/gateway/internal/router), answered with packages/routing.
//
// A table comes from one of two places:
//
//   - the control plane's routing table (FromPayload), the normal source since
//     Phase 6, with endpoints discovered from Kubernetes;
//   - a static file (Load), with worker endpoints written into it, for running
//     without Kubernetes or a control plane routing table.
//
// Either way a route belongs to exactly one organization: a caller asking for
// another org's model gets the same answer as for a model that does not exist
// (docs/security-boundaries.md §4.2).
package routes

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/adityasatwar321/nebula/packages/routing"
)

// Task is what a route's model does.
type Task string

// Supported tasks. Embeddings are absent because no runtime serves them; a route
// declaring one is refused at load rather than failing per request.
const (
	TaskChat       Task = "chat"
	TaskCompletion Task = "completion"
)

// Template names a chat prompt template. The gateway renders chat messages into
// the prompt the worker receives, because the worker protocol carries a prompt
// (workers/inference/nebula_worker/runtimes/base.py explains why).
type Template string

// Supported templates. Each is documented in the openai package.
const (
	TemplateChatML Template = "chatml"
	TemplateLlama3 Template = "llama3"
	TemplatePlain  Template = "plain"
)

// Capabilities are what the serving runtime can do. They drive validation: a
// parameter the runtime cannot honour is a 400 naming it, not a surprise at
// generation time (docs/api.md §7, obligation 5).
type Capabilities struct {
	Streaming bool `json:"streaming" yaml:"streaming"`
	JSONMode  bool `json:"json_mode" yaml:"json_mode"`
}

// Target is one weighted destination of a route: a deployment serving one
// immutable model version on a set of worker endpoints.
type Target struct {
	// Deployment is the deployment's name, returned to callers in the nebula block.
	Deployment string `json:"deployment" yaml:"deployment"`
	// DeploymentID identifies the deployment for pinning (nebula.deployment_id), in
	// usage records, and in endpoint discovery. Optional in a static table: a target
	// without one cannot be pinned by id, only by name.
	DeploymentID string `json:"deployment_id,omitempty" yaml:"deployment_id,omitempty"`
	// ModelVersion is asserted to the worker on every request; a worker serving a
	// different version answers 409 rather than silently serving the wrong model.
	ModelVersion string `json:"model_version" yaml:"model_version"`
	Weight       int    `json:"weight" yaml:"weight"`
	Label        string `json:"label,omitempty" yaml:"label,omitempty"`
	// Endpoints are worker base URLs, e.g. http://10.0.0.7:8080, in a static
	// table. A table from the control plane has none: its endpoints are discovered.
	Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`

	// State is the deployment's lifecycle state, reported in /v1/models. It does
	// not gate traffic; endpoints do.
	State string `json:"state,omitempty" yaml:"state,omitempty"`
	// Namespace is where the deployment's pods run.
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// ContextWindow of this target's model version; zero means the route's.
	ContextWindow int `json:"context_window,omitempty" yaml:"context_window,omitempty"`
	// Slots is each static endpoint's concurrency, for admission control when no
	// heartbeat reports it. Zero means the gateway's default.
	Slots int `json:"slots,omitempty" yaml:"slots,omitempty"`
}

// Key identifies the target's deployment in the router: its id when known, else
// a name scoped to the route, which is how a static table without ids works.
func (t *Target) Key(r *Route) string {
	if t.DeploymentID != "" {
		return strings.ToLower(t.DeploymentID)
	}
	return "static:" + r.ID + "/" + t.Deployment
}

// Route is the public identity of a capability: the value of "model" in a request.
type Route struct {
	// ID is stable across reloads of the same source. A static file that does not
	// set one gets an id derived from org and model, so it is never random.
	ID    string `json:"id,omitempty" yaml:"id,omitempty"`
	Model string `json:"model" yaml:"model"`
	// Org is the owning organization's slug.
	Org              string        `json:"org" yaml:"org"`
	Task             Task          `json:"task" yaml:"task"`
	ContextWindow    int           `json:"context_window" yaml:"context_window"`
	ChatTemplate     Template      `json:"chat_template,omitempty" yaml:"chat_template,omitempty"`
	DefaultMaxTokens int           `json:"default_max_tokens,omitempty" yaml:"default_max_tokens,omitempty"`
	Timeout          time.Duration `json:"-" yaml:"-"`
	TimeoutRaw       string        `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Capabilities     Capabilities  `json:"capabilities" yaml:"capabilities"`
	// Policy is the routing policy; an empty strategy means the default.
	Policy  routing.Policy `json:"policy" yaml:"policy"`
	Targets []*Target      `json:"targets" yaml:"targets"`
	// Created is reported as the OpenAI "created" field. Taken from the source so it
	// is stable; defaults to the time the table was loaded.
	Created int64 `json:"created,omitempty" yaml:"created,omitempty"`
}

// file is the on-disk shape.
type file struct {
	Routes []*Route `json:"routes" yaml:"routes"`
}

// Table is an immutable, validated route table.
type Table struct {
	// Version identifies the content, for logs and snapshots.
	Version string

	byOrgModel map[string]*Route
	byOrg      map[string][]*Route
}

// Empty is a table with no routes. The gateway serves the admin proxy with it, and
// every inference request is a 404.
func Empty() *Table {
	return &Table{byOrgModel: map[string]*Route{}, byOrg: map[string][]*Route{}}
}

// Load reads and validates a route file. YAML and JSON are both accepted, chosen
// by extension; an unknown field is an error, because a misspelt "weight" that
// silently defaults to zero would drop a target's traffic.
func Load(path string, now time.Time) (*Table, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the operator's own configuration file
	if err != nil {
		return nil, fmt.Errorf("reading route table: %w", err)
	}
	var f file
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		err = dec.Decode(&f)
	default:
		dec := yaml.NewDecoder(strings.NewReader(string(b)))
		dec.KnownFields(true)
		err = dec.Decode(&f)
	}
	if err != nil {
		return nil, fmt.Errorf("parsing route table %s: %w", path, err)
	}
	return Build(f.Routes, now)
}

// Payload is the control plane's internal routing table
// (GET /internal/v1/routing-table).
type Payload struct {
	Version string   `json:"version"`
	Routes  []*Route `json:"routes"`
}

// FromPayload builds a table from the control plane's routing table. Endpoints
// are not part of it; the router discovers them.
func FromPayload(p *Payload, now time.Time) (*Table, error) {
	t, err := build(p.Routes, now, false)
	if err != nil {
		return nil, err
	}
	t.Version = p.Version
	return t, nil
}

// modelNamePattern matches the routes.model_name CHECK constraint, so a name valid
// in a file is valid in PostgreSQL too.
var modelNamePattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._:-]{0,94}[A-Za-z0-9])?$`)

// Build validates a static table and indexes it. Every problem is reported, not
// just the first, matching the configuration package's rule.
func Build(in []*Route, now time.Time) (*Table, error) { return build(in, now, true) }

func build(in []*Route, now time.Time, static bool) (*Table, error) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	t := Empty()
	for i, r := range in {
		where := fmt.Sprintf("routes[%d]", i)
		if r == nil {
			add("%s: empty entry", where)
			continue
		}
		if r.Model != "" {
			where = fmt.Sprintf("routes[%d] (%s)", i, r.Model)
		}
		if !modelNamePattern.MatchString(r.Model) {
			add("%s: model %q is not a valid route name", where, r.Model)
		}
		if strings.TrimSpace(r.Org) == "" {
			add("%s: org is required: every route belongs to exactly one organization", where)
		}
		switch r.Task {
		case "":
			r.Task = TaskChat
		case TaskChat, TaskCompletion:
		default:
			add("%s: task %q is not supported (chat, completion)", where, r.Task)
		}
		if r.ContextWindow < 1 {
			add("%s: context_window must be positive", where)
		}
		switch r.ChatTemplate {
		case "":
			r.ChatTemplate = TemplateChatML
		case TemplateChatML, TemplateLlama3, TemplatePlain:
		default:
			add("%s: chat_template %q is not known (chatml, llama3, plain)", where, r.ChatTemplate)
		}
		if r.DefaultMaxTokens < 0 || (r.ContextWindow > 0 && r.DefaultMaxTokens > r.ContextWindow) {
			add("%s: default_max_tokens must be between 0 and context_window", where)
		}
		if r.TimeoutRaw != "" {
			d, err := time.ParseDuration(r.TimeoutRaw)
			if err != nil || d <= 0 {
				add("%s: timeout %q is not a positive duration", where, r.TimeoutRaw)
			}
			r.Timeout = d
		}
		if r.Created == 0 {
			r.Created = now.Unix()
		}
		if r.ID == "" {
			r.ID = derivedID(r.Org, r.Model)
		}

		if len(r.Targets) == 0 {
			add("%s: at least one target is required", where)
		}
		sum := 0
		names := map[string]bool{}
		for j, tg := range r.Targets {
			tw := fmt.Sprintf("%s.targets[%d]", where, j)
			if tg == nil {
				add("%s: empty entry", tw)
				continue
			}
			if tg.Deployment == "" {
				add("%s: deployment is required", tw)
			} else if names[tg.Deployment] {
				add("%s: deployment %q appears twice", tw, tg.Deployment)
			}
			names[tg.Deployment] = true
			if tg.ModelVersion == "" {
				add("%s: model_version is required: it is asserted to the worker on every request", tw)
			}
			if tg.Weight < 0 || tg.Weight > 100 {
				add("%s: weight must be between 0 and 100", tw)
			}
			sum += tg.Weight
			if static && len(tg.Endpoints) == 0 {
				add("%s: at least one endpoint is required", tw)
			}
			if !static && tg.DeploymentID == "" {
				add("%s: deployment_id is required", tw)
			}
			for k, ep := range tg.Endpoints {
				u, err := url.Parse(ep)
				if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") {
					add("%s.endpoints[%d]: %q must be a worker base URL such as http://host:port", tw, k, ep)
					continue
				}
				tg.Endpoints[k] = strings.TrimSuffix(ep, "/")
			}
		}
		// The same rule the database enforces on route_targets: a route summing to
		// 90 would silently drop a tenth of its traffic.
		if len(r.Targets) > 0 && sum != 100 {
			add("%s: target weights must sum to 100, got %d", where, sum)
		}

		key := r.Org + "\x00" + r.Model
		if _, dup := t.byOrgModel[key]; dup {
			add("%s: org %q already has a route named %q", where, r.Org, r.Model)
			continue
		}
		t.byOrgModel[key] = r
		t.byOrg[r.Org] = append(t.byOrg[r.Org], r)
	}
	if len(problems) > 0 {
		return nil, &LoadError{Problems: problems}
	}
	for _, rs := range t.byOrg {
		sort.Slice(rs, func(i, j int) bool { return rs[i].Model < rs[j].Model })
	}
	return t, nil
}

// LoadError lists every problem in a route table.
type LoadError struct{ Problems []string }

func (e *LoadError) Error() string {
	return fmt.Sprintf("invalid route table (%d problem(s)):\n  - %s", len(e.Problems), strings.Join(e.Problems, "\n  - "))
}

// Lookup returns org's route for model. A route owned by another org is reported
// exactly as a missing one.
func (t *Table) Lookup(org, model string) (*Route, bool) {
	r, ok := t.byOrgModel[org+"\x00"+model]
	return r, ok
}

// ForOrg lists org's routes, sorted by model name.
func (t *Table) ForOrg(org string) []*Route { return t.byOrg[org] }

// All lists every route, sorted by org then model name.
func (t *Table) All() []*Route {
	out := make([]*Route, 0, len(t.byOrgModel))
	for _, org := range routing.SortedKeys(t.byOrg) {
		out = append(out, t.byOrg[org]...)
	}
	return out
}

// Len counts routes across all orgs.
func (t *Table) Len() int { return len(t.byOrgModel) }

// ErrNoTarget means a pin named a deployment the route does not have.
var ErrNoTarget = errors.New("the route has no such deployment")

// Pinned returns the target a pin (deployment name or id) names.
func (r *Route) Pinned(pin string) (*Target, error) {
	for _, tg := range r.Targets {
		if tg.Deployment == pin || (tg.DeploymentID != "" && strings.EqualFold(tg.DeploymentID, pin)) {
			return tg, nil
		}
	}
	return nil, ErrNoTarget
}

// TargetContext is a target's context window, falling back to the route's.
func (r *Route) TargetContext(t *Target) int {
	if t.ContextWindow > 0 {
		return t.ContextWindow
	}
	return r.ContextWindow
}

// derivedID makes a stable identifier from org and model.
func derivedID(org, model string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(org + "\x00" + model))
	return fmt.Sprintf("static-%016x", h.Sum64())
}
