package usage_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/adityasatwar321/nebula/services/gateway/internal/usage"
)

func jetStreamServer(t *testing.T) (string, jetstream.JetStream) {
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
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "NEBULA_USAGE",
		Subjects: []string{usage.Subject}, Retention: jetstream.WorkQueuePolicy, Duplicates: 2 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	return ns.ClientURL(), js
}

func messages(t *testing.T, js jetstream.JetStream) uint64 {
	t.Helper()
	s, err := js.Stream(context.Background(), "NEBULA_USAGE")
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// Records reach the stream, and a redelivered record — same event id — is
// dropped by the stream's duplicate window rather than counted twice.
func TestRecordsArePublishedOnceEach(t *testing.T) {
	t.Parallel()
	url, js := jetStreamServer(t)
	sink := usage.NewJetStreamSink(url, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sink.Run(ctx) }()

	for i := range 50 {
		sink.Emit(ctx, usage.Record{RequestID: string(rune('a' + i%26)), OrgID: "org"})
	}
	dup := usage.Record{EventID: "fixed-event-id", RequestID: "r"}
	sink.Emit(ctx, dup)
	sink.Emit(ctx, dup)
	deadline := time.Now().Add(10 * time.Second)
	for messages(t, js) < 51 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if n := messages(t, js); n != 51 {
		t.Fatalf("%d messages, want 51 (50 records and one deduplicated pair)", n)
	}
}

// Nothing is ever lost silently: a full buffer sheds and counts what it sheds.
func TestFullBufferShedsAndCounts(t *testing.T) {
	t.Parallel()
	dropped := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "dropped"}, []string{"reason"})
	sink := usage.NewJetStreamSink("nats://127.0.0.1:1", slog.New(slog.NewTextHandler(io.Discard, nil)), dropped)
	for range usage.BufferSize + 7 {
		sink.Emit(context.Background(), usage.Record{RequestID: "r"})
	}
	if got := testutil.ToFloat64(dropped.WithLabelValues("buffer_full")); got != 7 {
		t.Fatalf("dropped %v, want 7", got)
	}
	if sink.Pending() != usage.BufferSize {
		t.Fatalf("pending %d", sink.Pending())
	}
}

// With JetStream down the buffer holds records, and they are published once it
// returns.
func TestRecordsWaitForJetStream(t *testing.T) {
	t.Parallel()
	url, js := jetStreamServer(t)
	_ = js.DeleteStream(context.Background(), "NEBULA_USAGE")
	sink := usage.NewJetStreamSink(url, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sink.Run(ctx) }()
	for range 5 {
		sink.Emit(ctx, usage.Record{RequestID: "r"})
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "NEBULA_USAGE",
		Subjects: []string{usage.Subject}, Retention: jetstream.WorkQueuePolicy}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for messages(t, js) < 5 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if n := messages(t, js); n != 5 {
		t.Fatalf("%d messages after the stream returned, want 5", n)
	}
}
