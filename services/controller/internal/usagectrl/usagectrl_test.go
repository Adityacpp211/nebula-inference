package usagectrl_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
	"github.com/adityasatwar321/nebula/services/controller/internal/usagectrl"
)

type fakeStore struct {
	mu    sync.Mutex
	rows  map[uuid.UUID]store.UsageRow
	fails atomic.Int32 // fail this many batches first
}

func (f *fakeStore) IngestUsage(_ context.Context, rows []store.UsageRow) (store.IngestResult, error) {
	if f.fails.Load() > 0 {
		f.fails.Add(-1)
		return store.IngestResult{}, errors.New("database down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	res := store.IngestResult{CostByDeployment: map[[2]string]int64{}}
	for _, r := range rows {
		if _, ok := f.rows[r.ID]; !ok {
			f.rows[r.ID] = r
			res.Inserted++
		}
	}
	return res, nil
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func server(t *testing.T) (string, jetstream.JetStream) {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir(),
		// Explicit limits: without them the server sizes JetStream from the host's
		// memory and disk, which is slow on a loaded CI or Windows machine.
		JetStreamMaxMemory: 64 << 20, JetStreamMaxStore: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(90 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	if _, err := js.CreateOrUpdateStream(context.Background(), usagectrl.StreamConfig()); err != nil {
		t.Fatal(err)
	}
	return ns.ClientURL(), js
}

func publish(t *testing.T, js jetstream.JetStream, n int, org uuid.UUID) {
	t.Helper()
	for range n {
		rec := map[string]any{"event_id": uuid.NewString(), "org_id": org.String(), "request_id": uuid.NewString(),
			"priority": "normal", "status_code": 200, "outcome": "completed", "started_at": time.Now().UTC(),
			"duration_ms": 10, "compute_ms": 8, "prompt_tokens": 3, "completion_tokens": 4, "sample_rate": 1}
		b, _ := json.Marshal(rec)
		if _, err := js.Publish(context.Background(), usagectrl.Subject, b); err != nil {
			t.Fatal(err)
		}
	}
}

func pending(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	s, _ := js.Stream(context.Background(), usagectrl.StreamName)
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Records are stored, acknowledged — and so removed from the WorkQueue — and an
// unreadable one is removed and counted rather than redelivered forever.
func TestIngestsAndAcknowledges(t *testing.T) {
	url, js := server(t)
	st := &fakeStore{rows: map[uuid.UUID]store.UsageRow{}}
	m := telemetry.NewMetrics()
	ing := &usagectrl.Ingester{URL: url, Store: st, Logger: telemetry.Discard(), Metrics: m}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Run(ctx) }()

	org := uuid.New()
	publish(t, js, 1200, org)
	if _, err := js.Publish(context.Background(), usagectrl.Subject, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "1200 rows", func() bool { return st.count() == 1200 })
	eventually(t, "an empty queue", func() bool { return pending(t, js) == 0 })
	for _, r := range st.rows {
		if r.OrgID != org || r.Priority != "NORMAL" || r.ComputeMS != 8 || r.PromptTokens != 3 {
			t.Fatalf("row %+v", r)
		}
		break
	}
	if got := testutil.ToFloat64(m.Counter("nebula_usage_events_dropped_total").WithLabelValues("unreadable")); got != 1 {
		t.Errorf("unreadable records counted %v, want 1", got)
	}
}

// A batch the store cannot commit is not acknowledged: it is redelivered, and
// nothing is lost.
func TestFailedBatchIsRedelivered(t *testing.T) {
	url, js := server(t)
	st := &fakeStore{rows: map[uuid.UUID]store.UsageRow{}}
	st.fails.Store(2)
	ing := &usagectrl.Ingester{URL: url, Store: st, Logger: telemetry.Discard()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Run(ctx) }()
	publish(t, js, 40, uuid.New())
	eventually(t, "all 40 rows after two failed batches", func() bool { return st.count() == 40 })
	eventually(t, "an empty queue", func() bool { return pending(t, js) == 0 })
}

// A standby controller ingests nothing; the leader does.
func TestOnlyTheLeaderIngests(t *testing.T) {
	url, js := server(t)
	st := &fakeStore{rows: map[uuid.UUID]store.UsageRow{}}
	var leader atomic.Bool
	ing := &usagectrl.Ingester{URL: url, Store: st, Logger: telemetry.Discard(), IsLeader: leader.Load}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Run(ctx) }()
	publish(t, js, 10, uuid.New())
	time.Sleep(time.Second)
	if st.count() != 0 {
		t.Fatal("a standby ingested")
	}
	leader.Store(true)
	eventually(t, "the leader to ingest", func() bool { return st.count() == 10 })
}
