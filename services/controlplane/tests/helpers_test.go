// Package controlplane_test exercises the control plane's HTTP API and store
// against a real PostgreSQL.
//
// It lives here rather than in tests/integration because Go's internal-package rule
// keeps services/controlplane/internal out of reach from the repository root. The
// database harness is shared through packages/testsupport/dbtest, so both suites
// mean the same thing by "a migrated database".
//
// These tests go through the real HTTP handler with the real middleware chain and a
// real database. That is deliberate: the things Phase 2 has to get right — a
// transition and its history row committing together, row-level security, a trigger
// refusing an illegal edge — are invisible to a test with a mocked store.
package controlplane_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
	"github.com/adityasatwar321/nebula/packages/testsupport/netx"
	"github.com/adityasatwar321/nebula/packages/version"
	cpapi "github.com/adityasatwar321/nebula/services/controlplane/internal/api"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/server"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// testPepper is a fixed pepper. Fixed on purpose: a key generated in one part of a
// test has to verify in another, and a random pepper per helper call would make
// that fail in a way that looks like an authentication bug.
const testPepper = "integration-test-pepper-0123456789abcdef"

// fixture is a migrated database with a store, an API and one tenant holding an
// API key for every scope.
type fixture struct {
	Pool   *pgxpool.Pool
	Store  *store.Store
	API    *cpapi.API
	Server *httptest.Server
	Hasher *auth.Hasher

	OrgID  uuid.UUID
	UserID uuid.UUID
	KeyID  uuid.UUID
	Key    string // plaintext, for Authorization headers
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := dbtest.Migrated(t)
	return newFixtureOn(t, pool)
}

// newFixtureOn builds a fixture over an existing pool, so two tenants can share a
// database and tenant isolation can actually be tested.
func newFixtureOn(t *testing.T, pool *pgxpool.Pool) *fixture {
	return newFixtureWith(t, pool, nil)
}

// newFixtureWith builds a fixture with extra configuration, keyed by environment
// variable name.
func newFixtureWith(t *testing.T, pool *pgxpool.Pool, extra map[string]string) *fixture {
	t.Helper()
	ctx := dbtest.Context(t)

	cfg := testConfigWith(t, extra)
	st := store.New(pool)

	api, err := cpapi.New(cfg, telemetry.Discard(), st)
	if err != nil {
		t.Fatalf("building the api: %v", err)
	}

	f := &fixture{Pool: pool, Store: st, API: api, Hasher: api.Hasher}
	f.seed(ctx, t, uniqueSlug(t))

	handler := server.New(server.Options{
		Config: cfg,
		Logger: telemetry.Discard(),
		Probes: newProbes(cfg),
		API:    api,
	})
	f.Server = netx.NewServer(t, handler)

	return f
}

func testConfigWith(t *testing.T, extra map[string]string) *config.Config {
	t.Helper()
	cfg, err := config.Loader{
		Service: "nebula-controlplane",
		Getenv: func(k string) string {
			if v, ok := extra[k]; ok {
				return v
			}
			switch k {
			case "NEBULA_DATABASE_URL":
				return dbtest.BaseURL()
			case "NEBULA_AUTH_KEY_PEPPER":
				return testPepper
			case "NEBULA_INTERNAL_AUTH_SECRET":
				return testInternalSecret
			case "NEBULA_DEV_MOCK_RUNTIME":
				// The registry refuses mock artifacts unless the stub is enabled, and
				// these tests register mock artifacts, so the tests must enable it
				// rather than the handler quietly allowing them.
				return "true"
			}
			return ""
		},
	}.Load()
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	return cfg
}

func newProbes(cfg *config.Config) *telemetry.Probes {
	p := telemetry.NewProbes(telemetry.ProbesOptions{
		Info:     version.Get("nebula-controlplane"),
		Instance: "integration-test",
		Env:      cfg.Env.String(),
		Logger:   telemetry.Discard(),
		ConfigFn: cfg.Redact,
	})
	p.MarkReady()
	return p
}

// uniqueSlug keeps two fixtures in one database from colliding on the slug's
// unique index.
func uniqueSlug(t *testing.T) string {
	t.Helper()
	raw := strings.ToLower(uuid.NewString())
	return "t" + strings.ReplaceAll(raw[:12], "-", "")
}

// seed creates the organization, an owner and a full-scope API key directly through
// the store, because an API that can only be bootstrapped through itself cannot be
// bootstrapped at all.
func (f *fixture) seed(ctx context.Context, t *testing.T, slug string) {
	t.Helper()

	orgID := db.MustNewID()
	userID := db.MustNewID()
	keyID := db.MustNewID()

	generated, err := f.Hasher.Generate()
	if err != nil {
		t.Fatalf("generating an api key: %v", err)
	}

	err = f.Store.InTx(ctx, func(tx pgx.Tx) error {
		org := &models.Organization{
			ID: orgID, Slug: slug, Name: "Test " + slug, DefaultNamespace: "nebula-" + slug,
		}
		if err := f.Store.Organizations.Create(ctx, tx, org); err != nil {
			return err
		}
		if err := db.SetSessionOrg(ctx, tx, orgID); err != nil {
			return err
		}
		name := "Test Owner"
		user := &models.User{
			ID: userID, OrgID: orgID, Email: slug + "@example.test",
			Name: &name, Role: models.RoleOwner,
		}
		if err := f.Store.Users.Create(ctx, tx, user); err != nil {
			return err
		}
		key := &models.APIKey{
			ID: keyID, OrgID: orgID, UserID: &userID, Name: "test key",
			Prefix: generated.Prefix, KeyHash: generated.Hash,
			Scopes: auth.Strings(auth.AllScopes()), Priority: models.PriorityNormal,
		}
		return f.Store.APIKeys.Create(ctx, tx, key)
	})
	if err != nil {
		t.Fatalf("seeding the tenant: %v", err)
	}

	f.OrgID, f.UserID, f.KeyID, f.Key = orgID, userID, keyID, generated.Plaintext
}

// actor is the store actor for this fixture's API key.
func (f *fixture) actor() store.Actor {
	return store.Actor{Type: string(models.ActorAPIKey), ID: f.KeyID}
}

// inTx runs fn in a tenant-scoped transaction, as a handler would.
func (f *fixture) inTx(t *testing.T, fn func(q store.Querier) error) error {
	t.Helper()
	return f.Store.InTxForOrg(dbtest.Context(t), f.OrgID, f.actor(), func(tx pgx.Tx) error {
		return fn(tx)
	})
}

// mustInTx fails the test when the transaction does not commit.
func (f *fixture) mustInTx(t *testing.T, fn func(q store.Querier) error) {
	t.Helper()
	if err := f.inTx(t, fn); err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// do sends an authenticated request and decodes the response into out (which may
// be nil). It returns the status so a test can assert on it.
func (f *fixture) do(t *testing.T, method, path string, body, out any) int {
	t.Helper()
	return f.doWithKey(t, f.Key, method, path, body, out)
}

func (f *fixture) doWithKey(t *testing.T, key, method, path string, body, out any) int {
	t.Helper()

	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encoding request body: %v", err)
		}
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}

	req, err := http.NewRequestWithContext(dbtest.Context(t), method, f.Server.URL+path, reader)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := f.Server.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	// Every response, including errors, must carry the request id. Asserting it here
	// means every HTTP test in this package checks it.
	if resp.Header.Get("X-Request-Id") == "" {
		t.Errorf("%s %s returned no X-Request-Id", method, path)
	}

	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decoding the response to %s %s (status %d): %v", method, path, resp.StatusCode, err)
		}
	}
	return resp.StatusCode
}

// apiError is the error envelope, for asserting on failures.
type apiError struct {
	Error struct {
		Message   string `json:"message"`
		Type      string `json:"type"`
		Code      string `json:"code"`
		Param     string `json:"param"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

// expectError asserts a status and returns the decoded envelope.
func (f *fixture) expectError(t *testing.T, want int, method, path string, body any) apiError {
	t.Helper()
	var env apiError
	got := f.do(t, method, path, body, &env)
	if got != want {
		t.Fatalf("%s %s = %d, want %d (message: %q)", method, path, got, want, env.Error.Message)
	}
	if env.Error.Message == "" {
		t.Errorf("%s %s returned status %d with an empty message", method, path, got)
	}
	if env.Error.RequestID == "" {
		t.Errorf("%s %s returned an error envelope with no request_id", method, path)
	}
	return env
}

// ---------------------------------------------------------------------------
// registry fixtures
// ---------------------------------------------------------------------------

// createReadyVersion registers a model and a ready version through the HTTP API,
// which is also the check that the documented path from nothing to a deployable
// version works end to end.
func (f *fixture) createReadyVersion(t *testing.T, modelName, versionLabel string) (modelID, versionID uuid.UUID) {
	t.Helper()

	var model struct {
		ID uuid.UUID `json:"id"`
	}
	if code := f.do(t, http.MethodPost, "/v1/models", map[string]any{
		"name": modelName,
		"task": "chat",
	}, &model); code != http.StatusCreated {
		t.Fatalf("creating model %s = %d, want 201", modelName, code)
	}

	checksum := fakeChecksum(modelName + versionLabel)
	var created struct {
		ID     uuid.UUID `json:"id"`
		Status string    `json:"status"`
		Upload any       `json:"upload"`
	}
	if code := f.do(t, http.MethodPost, "/v1/models/"+model.ID.String()+"/versions", map[string]any{
		"version":         versionLabel,
		"format":          "mock",
		"runtime":         "mock",
		"artifact_uri":    "s3://nebula-test/" + checksum,
		"size_bytes":      1024,
		"checksum_sha256": checksum,
		"context_window":  4096,
	}, &created); code != http.StatusCreated {
		t.Fatalf("creating version %s = %d, want 201", versionLabel, code)
	}
	if created.Upload != nil {
		t.Error("an upload target was issued, but object storage does not exist until Phase 3")
	}

	var ready struct {
		Status       string `json:"status"`
		Verification string `json:"verification"`
	}
	if code := f.do(t, http.MethodPost,
		"/v1/model-versions/"+created.ID.String()+"/finalize",
		map[string]any{"checksum_sha256": checksum}, &ready); code != http.StatusOK {
		t.Fatalf("finalizing version = %d, want 200", code)
	}
	if ready.Status != string(models.VersionReady) {
		t.Fatalf("finalized version has status %q, want ready", ready.Status)
	}
	if ready.Verification != "declared_checksum" {
		// The weaker verification must be reported. A response that claimed a
		// stronger one would be the kind of lie this project refuses.
		t.Errorf("verification = %q, want declared_checksum", ready.Verification)
	}

	return model.ID, created.ID
}

// createDeployment creates a deployment on a ready version.
func (f *fixture) createDeployment(t *testing.T, name string, versionID uuid.UUID) uuid.UUID {
	t.Helper()
	var out struct {
		ID     uuid.UUID `json:"id"`
		Status struct {
			State string `json:"state"`
		} `json:"status"`
	}
	if code := f.do(t, http.MethodPost, "/v1/deployments", map[string]any{
		"name":             name,
		"model_version_id": versionID,
		"replicas":         2,
		"min_replicas":     1,
		"max_replicas":     4,
		"resources":        map[string]any{"cpu_milli": 500, "memory_mib": 512},
	}, &out); code != http.StatusAccepted {
		t.Fatalf("creating deployment %s = %d, want 202 Accepted", name, code)
	}
	if out.Status.State != string(models.DeploymentPending) {
		t.Fatalf("new deployment is %q, want pending", out.Status.State)
	}
	return out.ID
}

// transition drives a deployment through the state machine over HTTP.
func (f *fixture) transition(t *testing.T, id uuid.UUID, from, to models.DeploymentState, reason string) int {
	t.Helper()
	return f.do(t, http.MethodPost, "/v1/deployments/"+id.String()+"/transition", map[string]any{
		"from":   string(from),
		"to":     string(to),
		"reason": reason,
	}, nil)
}

// fakeChecksum produces a deterministic 64-character hex digest from a label. Not a
// real digest of any bytes, and nothing in Phase 2 claims it is: the registry
// records what the client declared and says so.
func fakeChecksum(label string) string {
	raw := make([]byte, 32)
	for i := range raw {
		if i < len(label) {
			raw[i] = label[i]
		} else {
			raw[i] = byte(i)
		}
	}
	return hex.EncodeToString(raw)
}

// waitFor is used where a timestamp comparison needs the clock to have moved.
// PostgreSQL's now() is transaction-scoped, so a millisecond is enough.
func waitFor() { time.Sleep(2 * time.Millisecond) }

// decodeJSONBody decodes a response body, used where a test needs the raw response
// rather than the convenience of do().
func decodeJSONBody(resp *http.Response, out any) error {
	return json.NewDecoder(resp.Body).Decode(out)
}
