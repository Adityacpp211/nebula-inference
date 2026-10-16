package controlplane_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/testsupport/dbtest"
)

// The autoscaling object is parsed by the autoscaler's own parser on write, so a
// typo is a 400 rather than a deployment that silently never scales.
func TestAutoscalingSettingsAreValidated(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "scaly", "v1")

	base := func(autoscaling any) map[string]any {
		return map[string]any{"name": "scaly", "model_version_id": v, "replicas": 1, "min_replicas": 1,
			"max_replicas": 4, "resources": map[string]any{"cpu_milli": 500, "memory_mib": 512},
			"autoscaling": autoscaling}
	}
	for name, bad := range map[string]any{
		// The shape an older draft of docs/api.md showed: bounds live on the deployment.
		"bounds inside":  map[string]any{"enabled": true, "min_replicas": 1},
		"tolerance":      map[string]any{"enabled": true, "tolerance": 1.5},
		"negative":       map[string]any{"enabled": true, "cooldown_s": -1},
		"window too big": map[string]any{"enabled": true, "scale_down_stabilization_s": 86400},
	} {
		env := f.expectError(t, http.StatusBadRequest, http.MethodPost, "/v1/deployments", base(bad))
		if env.Error.Code != "invalid_autoscaling" || !strings.Contains(env.Error.Message, "autoscaling") {
			t.Errorf("%s: %q %q", name, env.Error.Code, env.Error.Message)
		}
	}

	var dep struct {
		ID         uuid.UUID `json:"id"`
		Generation int64     `json:"generation"`
	}
	if code := f.do(t, http.MethodPost, "/v1/deployments",
		base(map[string]any{"enabled": true, "target_concurrency": 2}), &dep); code != http.StatusAccepted {
		t.Fatalf("valid settings: %d", code)
	}
	env := f.expectError(t, http.StatusBadRequest, http.MethodPatch, "/v1/deployments/"+dep.ID.String(),
		map[string]any{"generation": dep.Generation, "autoscaling": map[string]any{"enabeld": true}})
	if env.Error.Code != "invalid_autoscaling" {
		t.Errorf("patch: %q", env.Error.Code)
	}
}

// Every recorded decision is readable, newest first, with its signal vector, and
// only by the deployment's own organization.
func TestAutoscalingEventsAreReadable(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, v := f.createReadyVersion(t, "evts", "v1")
	id := f.createDeployment(t, "evts", v)

	ctx := dbtest.Context(t)
	for i, row := range []struct {
		from, to int32
		dir, dec string
	}{
		{2, 2, "hold", "suppressed_stabilization"},
		{2, 4, "up", "scaled"},
		{4, 4, "hold", "clamped_max"},
	} {
		if _, err := f.Pool.Exec(ctx, `
			INSERT INTO autoscaling_events (id, org_id, deployment_id, from_replicas, to_replicas, direction,
			                                signals, decision, reason)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			db.MustNewID(), f.OrgID, id, row.from, row.to, row.dir,
			`[{"name":"concurrency_per_replica","observed":8,"target":2,"ratio":4,"source":"prometheus","stale":false,"selected":true}]`,
			row.dec, "reason "+string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}

	type event struct {
		ID           uuid.UUID `json:"id"`
		FromReplicas int32     `json:"from_replicas"`
		ToReplicas   int32     `json:"to_replicas"`
		Direction    string    `json:"direction"`
		Decision     string    `json:"decision"`
		Reason       string    `json:"reason"`
		Signals      []struct {
			Name     string  `json:"name"`
			Ratio    float64 `json:"ratio"`
			Selected bool    `json:"selected"`
		} `json:"signals"`
	}
	var page struct {
		Data       []event `json:"data"`
		NextCursor *string `json:"next_cursor"`
	}
	path := "/v1/deployments/" + id.String() + "/autoscaling-events"
	if code := f.do(t, http.MethodGet, path+"?limit=2", nil, &page); code != 200 || len(page.Data) != 2 || page.NextCursor == nil {
		t.Fatalf("first page: %d %+v", code, page)
	}
	if page.Data[0].Decision != "clamped_max" || page.Data[1].Decision != "scaled" || page.Data[1].ToReplicas != 4 {
		t.Errorf("newest first: %+v", page.Data)
	}
	if s := page.Data[1].Signals; len(s) != 1 || s[0].Ratio != 4 || !s[0].Selected {
		t.Errorf("signals: %+v", s)
	}
	cursor := *page.NextCursor
	page.Data, page.NextCursor = nil, nil
	if code := f.do(t, http.MethodGet, path+"?limit=2&cursor="+cursor, nil, &page); code != 200 ||
		len(page.Data) != 1 || page.Data[0].Decision != "suppressed_stabilization" {
		t.Fatalf("second page: %d %+v", code, page)
	}

	if code := f.do(t, http.MethodGet, "/v1/deployments/"+uuid.NewString()+"/autoscaling-events", nil, nil); code != http.StatusNotFound {
		t.Errorf("unknown deployment: %d", code)
	}
}
