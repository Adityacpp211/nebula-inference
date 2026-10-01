package router

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/packages/routing"
	"github.com/adityasatwar321/nebula/services/gateway/internal/routes"
)

// ---------------------------------------------------------------------------
// routes: the control plane's routing table, with a Redis snapshot
// ---------------------------------------------------------------------------

// FetchFunc fetches the routing table; controlplane.Client.RoutingTable.
type FetchFunc func(ctx context.Context, etag string) (body []byte, newETag string, err error)

// SnapshotStore keeps the last good routing state where a cold-starting replica
// can read it. Redis in production.
type SnapshotStore interface {
	Load(ctx context.Context) ([]byte, error)
	Save(ctx context.Context, b []byte) error
}

// TableSource keeps the router's table in step with the control plane.
type TableSource struct {
	Fetch       FetchFunc
	NotModified error // the fetch error meaning "unchanged"
	Router      *Router
	Snapshots   SnapshotStore // nil: no cold-start snapshot
	Interval    time.Duration
	Logger      *slog.Logger
	Now         func() time.Time

	etag      string
	lastSaved [32]byte
	// Synced is set once any table (live or from a snapshot) is installed.
	Synced atomic.Bool
	// FromSnapshot is set while the installed table came from a snapshot rather
	// than the control plane, which /healthz reports.
	FromSnapshot atomic.Bool
}

// Run polls until ctx ends. The first poll happens at once; if it fails, the
// snapshot is loaded so the gateway routes with the control plane down (axiom A8).
func (s *TableSource) Run(ctx context.Context) {
	if err := s.Poll(ctx); err != nil {
		s.Logger.WarnContext(ctx, "the control plane routing table is unavailable at startup; trying the snapshot",
			slog.String("cause", err.Error()))
		if err := s.LoadSnapshot(ctx); err != nil {
			s.Logger.WarnContext(ctx, "no routing snapshot either; serving no routes until the control plane answers",
				slog.String("cause", err.Error()))
		}
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		err := s.Poll(ctx)
		switch {
		case err != nil && !failing:
			failing = true
			s.Logger.WarnContext(ctx, "routing table refresh failed; serving the last good table",
				slog.String("cause", err.Error()))
		case err == nil && failing:
			failing = false
			s.Logger.InfoContext(ctx, "routing table refresh recovered")
		}
		s.save(ctx)
	}
}

// Poll fetches the table once and installs it if it changed.
func (s *TableSource) Poll(ctx context.Context) error {
	body, etag, err := s.Fetch(ctx, s.etag)
	if err != nil {
		if s.NotModified != nil && errors.Is(err, s.NotModified) {
			s.FromSnapshot.Store(false)
			return nil
		}
		return err
	}
	if err := s.install(body); err != nil {
		return err
	}
	s.etag = etag
	s.FromSnapshot.Store(false)
	s.save(ctx)
	return nil
}

func (s *TableSource) install(body []byte) error {
	var p routes.Payload
	if err := json.Unmarshal(body, &p); err != nil {
		return fmt.Errorf("decoding the routing table: %w", err)
	}
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	t, err := routes.FromPayload(&p, now())
	if err != nil {
		// A table the control plane produced and this gateway cannot accept is a
		// version skew; keep serving the previous table rather than none.
		return fmt.Errorf("the control plane's routing table is invalid here: %w", err)
	}
	prev := s.Router.Table()
	s.Router.SetTable(t)
	s.Router.SetPayload(body)
	s.Synced.Store(true)
	if prev.Version != t.Version {
		s.Logger.Info("routing table installed", slog.String("table_version", t.Version), slog.Int("routes", t.Len()))
	}
	return nil
}

// LoadSnapshot installs the last saved routing state.
func (s *TableSource) LoadSnapshot(ctx context.Context) error {
	if s.Snapshots == nil {
		return errors.New("no snapshot store is configured")
	}
	b, err := s.Snapshots.Load(ctx)
	if err != nil {
		return err
	}
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("decoding the routing snapshot: %w", err)
	}
	if err := s.install(snap.Table); err != nil {
		return err
	}
	s.Router.Restore(snap)
	s.FromSnapshot.Store(true)
	s.Logger.InfoContext(ctx, "routing state restored from the snapshot",
		slog.Time("saved_at", snap.SavedAt), slog.Int("routes", s.Router.Table().Len()))
	return nil
}

// save writes the snapshot when it changed. Never while serving from a snapshot:
// that would re-date old state as new.
func (s *TableSource) save(ctx context.Context) {
	if s.Snapshots == nil || s.FromSnapshot.Load() {
		return
	}
	snap, err := s.Router.Snapshot()
	if err != nil {
		return
	}
	saved := snap.SavedAt
	snap.SavedAt = time.Time{}
	b, _ := json.Marshal(snap)
	sum := sha256.Sum256(b)
	if sum == s.lastSaved {
		return
	}
	snap.SavedAt = saved
	b, _ = json.Marshal(snap)
	if err := s.Snapshots.Save(ctx, b); err != nil {
		s.Logger.DebugContext(ctx, "saving the routing snapshot failed", slog.String("cause", err.Error()))
		return
	}
	s.lastSaved = sum
}

// RedisSnapshots stores the snapshot under one key.
type RedisSnapshots struct {
	Client    redis.UniversalClient
	Key       string
	OpTimeout time.Duration
}

// Load implements SnapshotStore.
func (r RedisSnapshots) Load(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, r.OpTimeout)
	defer cancel()
	return r.Client.Get(ctx, r.Key).Bytes()
}

// Save implements SnapshotStore. No expiry: an old snapshot is still better than
// none during an outage, and SavedAt says how old it is.
func (r RedisSnapshots) Save(ctx context.Context, b []byte) error {
	ctx, cancel := context.WithTimeout(ctx, r.OpTimeout)
	defer cancel()
	return r.Client.Set(ctx, r.Key, b, 0).Err()
}

// ---------------------------------------------------------------------------
// endpoints: Kubernetes EndpointSlices
// ---------------------------------------------------------------------------

// EndpointSlices discovers worker endpoints. Every Service the controller creates
// carries the deployment-id label, which the EndpointSlice controller copies onto
// the Service's slices, so one label selector finds every worker slice.
type EndpointSlices struct {
	Client    kubernetes.Interface
	Namespace string
	Router    *Router
	Logger    *slog.Logger
	// Resync re-derives every deployment's endpoints periodically, a backstop to
	// events.
	Resync time.Duration

	synced atomic.Bool
}

// Synced reports whether the informer has completed its first list.
func (e *EndpointSlices) Synced() bool { return e.synced.Load() }

// Run watches until ctx ends.
func (e *EndpointSlices) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(e.Client, e.Resync,
		informers.WithNamespace(e.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = k8s.LabelDeploymentID }))
	inf := factory.Discovery().V1().EndpointSlices()
	lister := inf.Lister()

	var mu sync.Mutex
	refresh := func(obj any) {
		es, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok {
			if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				es, ok = tomb.Obj.(*discoveryv1.EndpointSlice)
				if !ok {
					return
				}
			} else {
				return
			}
		}
		id := es.Labels[k8s.LabelDeploymentID]
		if id == "" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		sel := labels.SelectorFromSet(labels.Set{k8s.LabelDeploymentID: id})
		slices, err := lister.EndpointSlices(e.Namespace).List(sel)
		if err != nil {
			return
		}
		e.Router.SetEndpoints(id, Merge(slices))
	}
	if _, err := inf.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    refresh,
		UpdateFunc: func(_, obj any) { refresh(obj) },
		DeleteFunc: refresh,
	}); err != nil {
		return err
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.Informer().HasSynced) {
		return ctx.Err()
	}
	e.synced.Store(true)
	e.Logger.InfoContext(ctx, "endpoint discovery synced", slog.String("namespace", e.Namespace))
	<-ctx.Done()
	factory.Shutdown()
	return nil
}

// Merge turns one deployment's EndpointSlices into endpoints.
//
// An endpoint is ready only when Kubernetes says it is ready and it is not
// terminating: a terminating pod still serving in-flight work must not be sent
// new work (its drain is in progress).
func Merge(slices []*discoveryv1.EndpointSlice) []Discovered {
	var out []Discovered
	seen := map[string]bool{}
	for _, es := range slices {
		if es.AddressType != discoveryv1.AddressTypeIPv4 && es.AddressType != discoveryv1.AddressTypeIPv6 {
			continue
		}
		port := int32(0)
		for _, p := range es.Ports {
			if p.Port != nil && (p.Name == nil || *p.Name == "http" || port == 0) {
				port = *p.Port
			}
		}
		if port == 0 {
			continue
		}
		for _, ep := range es.Endpoints {
			if len(ep.Addresses) == 0 {
				continue
			}
			id := ep.Addresses[0]
			if ep.TargetRef != nil && ep.TargetRef.Name != "" {
				id = ep.TargetRef.Name
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			ready := ep.Conditions.Ready == nil || *ep.Conditions.Ready
			if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
				ready = false
			}
			host := ep.Addresses[0]
			if es.AddressType == discoveryv1.AddressTypeIPv6 {
				host = "[" + host + "]"
			}
			out = append(out, Discovered{ID: id, URL: "http://" + host + ":" + strconv.Itoa(int(port)), Ready: ready})
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// heartbeats: core NATS
// ---------------------------------------------------------------------------

// HeartbeatSubject is the wildcard every worker heartbeat matches.
const HeartbeatSubject = "nebula.worker.heartbeat.>"

// Heartbeats subscribes to worker heartbeats.
type Heartbeats struct {
	URL    string
	Router *Router
	Logger *slog.Logger
	// Settle is how long the connection must have been up before silence is
	// believed: right after a (re)connect every heartbeat looks old.
	Settle time.Duration
	Now    func() time.Time
	// Also subscribes further subjects on the same connection, such as the
	// credential revocation broadcast.
	Also map[string]func(data []byte)

	conn    atomic.Pointer[nats.Conn]
	upSince atomic.Int64 // unix nanos, 0 while down
	applied atomic.Uint64
}

// Live reports whether heartbeats are flowing well enough for their absence to
// mean something. Pass it as Options.HeartbeatsLive.
func (h *Heartbeats) Live() bool {
	c := h.conn.Load()
	since := h.upSince.Load()
	if c == nil || !c.IsConnected() || since == 0 {
		return false
	}
	return h.now().Sub(time.Unix(0, since)) >= h.Settle
}

// Publish sends a message on the heartbeat connection, best effort: nothing that
// uses it may depend on delivery.
func (h *Heartbeats) Publish(subject string, data []byte) error {
	c := h.conn.Load()
	if c == nil {
		return errors.New("not connected to NATS")
	}
	return c.Publish(subject, data)
}

// Applied counts heartbeats applied, for health output and tests.
func (h *Heartbeats) Applied() uint64 { return h.applied.Load() }

func (h *Heartbeats) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// Run connects and subscribes until ctx ends. The client reconnects forever: NATS
// is a soft dependency, and a gateway without it still routes (docs/components.md).
func (h *Heartbeats) Run(ctx context.Context) error {
	markUp := func() { h.upSince.Store(h.now().UnixNano()) }
	nc, err := nats.Connect(h.URL,
		nats.Name("nebula-gateway"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.RetryOnFailedConnect(true),
		nats.ConnectHandler(func(*nats.Conn) { markUp(); h.Logger.Info("heartbeats connected") }),
		nats.ReconnectHandler(func(*nats.Conn) { markUp(); h.Logger.Info("heartbeats reconnected") }),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			h.upSince.Store(0)
			if err != nil {
				h.Logger.Warn("heartbeats disconnected; routing on readiness and local observation",
					slog.String("cause", err.Error()))
			}
		}),
	)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	h.conn.Store(nc)
	if nc.IsConnected() {
		markUp()
	}
	sub, err := nc.Subscribe(HeartbeatSubject, func(m *nats.Msg) {
		var hb routing.Heartbeat
		if err := json.Unmarshal(m.Data, &hb); err != nil {
			return
		}
		if h.Router.ObserveHeartbeat(hb) {
			h.applied.Add(1)
		}
	})
	if err != nil {
		nc.Close()
		return fmt.Errorf("subscribing to heartbeats: %w", err)
	}
	// Heartbeats are latest-wins; dropping a backlog is correct, blocking is not.
	_ = sub.SetPendingLimits(64*1024, 64<<20)
	for subject, fn := range h.Also {
		if _, err := nc.Subscribe(subject, func(m *nats.Msg) { fn(m.Data) }); err != nil {
			nc.Close()
			return fmt.Errorf("subscribing to %s: %w", subject, err)
		}
	}
	<-ctx.Done()
	_ = sub.Unsubscribe()
	nc.Close()
	return nil
}
