// Package run wires the controller's loops: informers feeding a rate-limited work
// queue, a database poll, a periodic full resync with orphan collection, and the
// node inventory — all behind leader election, so exactly one replica writes.
package run

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"

	"github.com/adityasatwar321/nebula/packages/k8s"
	"github.com/adityasatwar321/nebula/services/controller/internal/deploymentctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/inventoryctrl"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Options configures the controller.
type Options struct {
	Kube            kubernetes.Interface
	Store           *store.Store
	Reconciler      *deploymentctrl.Reconciler
	Namespace       string
	SystemNamespace string
	Identity        string
	LeaderElection  bool
	LeaseDuration   time.Duration
	RenewDeadline   time.Duration
	RetryPeriod     time.Duration
	ResyncInterval  time.Duration
	PollInterval    time.Duration
	Workers         int
	Logger          *slog.Logger
}

// Controller is the running controller.
type Controller struct {
	o      Options
	queue  workqueue.TypedRateLimitingInterface[string]
	leader atomic.Bool
	synced atomic.Bool
}

// New builds a controller.
func New(o Options) *Controller {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	return &Controller{
		o: o,
		// Exponential backoff per key, 5 ms to 5 minutes, plus an overall bucket:
		// a deployment that keeps failing slows down instead of hot-looping.
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "deployments"}),
	}
}

// IsLeader reports whether this replica is the active one.
func (c *Controller) IsLeader() bool { return c.leader.Load() }

// Synced reports whether the informer caches have synced.
func (c *Controller) Synced() bool { return c.synced.Load() }

// Run blocks until ctx ends: campaigning for the lease when leader election is on,
// working when it holds it, and exiting when it loses it (a replica that lost the
// lease must stop writing at once; restarting is the simplest way to guarantee it).
func (c *Controller) Run(ctx context.Context) error {
	if !c.o.LeaderElection {
		c.leader.Store(true)
		return c.work(ctx)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: "nebula-controller", Namespace: c.o.SystemNamespace},
		Client:     c.o.Kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: c.o.Identity},
	}
	// A child context: if the work stops (an informer that never syncs, say), the
	// lease is released and the process exits instead of holding leadership while
	// doing nothing.
	leCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 1)
	leaderelection.RunOrDie(leCtx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   c.o.LeaseDuration,
		RenewDeadline:   c.o.RenewDeadline,
		RetryPeriod:     c.o.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				c.leader.Store(true)
				c.o.Logger.Info("acquired leadership", slog.String("identity", c.o.Identity))
				errc <- c.work(ctx)
				cancel()
			},
			OnStoppedLeading: func() {
				c.leader.Store(false)
				c.o.Logger.Info("lost leadership", slog.String("identity", c.o.Identity))
			},
			OnNewLeader: func(id string) {
				if id != c.o.Identity {
					c.o.Logger.Info("following", slog.String("leader", id))
				}
			},
		},
	})
	select {
	case err := <-errc:
		return err
	default:
		if ctx.Err() == nil {
			return fmt.Errorf("leadership lost")
		}
		return nil
	}
}

// work runs every loop until ctx ends.
func (c *Controller) work(ctx context.Context) error {
	defer c.queue.ShutDown()
	log := c.o.Logger

	// Workload objects: namespaced, and only the ones NEBULA manages.
	managed := informers.NewSharedInformerFactoryWithOptions(c.o.Kube, c.o.ResyncInterval,
		informers.WithNamespace(c.o.Namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) { lo.LabelSelector = k8s.ManagedSelector() }))
	// Nodes and every pod: capacity admission needs requests of pods NEBULA does
	// not manage too, because they occupy the same nodes.
	cluster := informers.NewSharedInformerFactory(c.o.Kube, c.o.ResyncInterval)

	enqueueObj := func(obj any) {
		if m, ok := obj.(metav1.Object); ok {
			if id := m.GetLabels()[k8s.LabelDeploymentID]; id != "" {
				c.queue.Add(id)
			}
		}
	}
	deps := managed.Apps().V1().Deployments().Informer()
	_, _ = deps.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueueObj,
		UpdateFunc: func(_, obj any) { enqueueObj(obj) },
		// The deliberate case: a Deployment deleted out from under NEBULA is
		// reconciled — and recreated — at once, not at the next resync.
		DeleteFunc: func(obj any) {
			if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = t.Obj
			}
			enqueueObj(obj)
		},
	})

	events := &inventoryctrl.Events{Store: c.o.Store, Logger: log}
	pods := cluster.Core().V1().Pods()
	_, _ = pods.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if p, ok := obj.(*corev1.Pod); ok && isWorker(p) {
				events.Observe(ctx, p)
				enqueueObj(p)
			}
		},
		UpdateFunc: func(_, obj any) {
			if p, ok := obj.(*corev1.Pod); ok && isWorker(p) {
				events.Observe(ctx, p)
				enqueueObj(p)
			}
		},
		DeleteFunc: func(obj any) {
			if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = t.Obj
			}
			if p, ok := obj.(*corev1.Pod); ok && isWorker(p) {
				events.Forget(ctx, p)
				enqueueObj(p)
			}
		},
	})
	nodes := cluster.Core().V1().Nodes()
	_ = nodes.Informer()

	managed.Start(ctx.Done())
	cluster.Start(ctx.Done())
	for typ, ok := range managed.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	for typ, ok := range cluster.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("informer cache for %v did not sync", typ)
		}
	}
	c.synced.Store(true)
	log.InfoContext(ctx, "informer caches synced; reconciling")

	var wg sync.WaitGroup
	for range c.o.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.next(ctx) {
			}
		}()
	}

	inv := &inventoryctrl.Nodes{Nodes: nodes.Lister(), Pods: pods.Lister(), Store: c.o.Store,
		Interval: 15 * time.Second, Logger: log}
	wg.Add(1)
	go func() { defer wg.Done(); inv.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); c.poll(ctx) }()

	<-ctx.Done()
	c.queue.ShutDown()
	wg.Wait()
	return nil
}

func isWorker(p *corev1.Pod) bool {
	return p.Labels[k8s.LabelManagedBy] == k8s.ManagedBy && p.Labels[k8s.LabelDeploymentID] != ""
}

// poll asks PostgreSQL what changed, and on every resync interval re-queues every
// deployment and collects orphans. NATS reconcile signals (Phase 6) will make the
// poll a backstop rather than the trigger.
func (c *Controller) poll(ctx context.Context) {
	pollT := time.NewTicker(c.o.PollInterval)
	defer pollT.Stop()
	resyncT := time.NewTicker(c.o.ResyncInterval)
	defer resyncT.Stop()
	c.resync(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-pollT.C:
			ids, err := c.o.Store.ListNeedingWork(ctx)
			if err != nil {
				c.o.Logger.WarnContext(ctx, "polling for work failed", slog.String("cause", err.Error()))
				continue
			}
			for _, id := range ids {
				c.queue.Add(id.String())
			}
		case <-resyncT.C:
			c.resync(ctx)
		}
	}
}

func (c *Controller) resync(ctx context.Context) {
	live, err := c.o.Store.ListLiveIDs(ctx)
	if err != nil {
		// No live set, no garbage collection: an object the controller cannot prove
		// is an orphan may be serving traffic (docs/architecture.md §8.4).
		c.o.Logger.WarnContext(ctx, "resync skipped: cannot read deployments", slog.String("cause", err.Error()))
		return
	}
	for id := range live {
		c.queue.Add(id.String())
	}
	if n, err := c.o.Reconciler.CollectOrphans(ctx, live); err != nil {
		c.o.Logger.WarnContext(ctx, "orphan collection failed", slog.String("cause", err.Error()))
	} else if n > 0 {
		c.o.Logger.InfoContext(ctx, "collected orphaned workload objects", slog.Int("deployments", n))
	}
}

// next processes one key. The queue guarantees a key is never processed by two
// workers at once, so reconciles of one deployment are serialized.
func (c *Controller) next(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	id, err := uuid.Parse(key)
	if err != nil {
		c.queue.Forget(key)
		return true
	}
	started := time.Now()
	res, err := c.o.Reconciler.Reconcile(ctx, id)
	if err != nil {
		c.o.Logger.WarnContext(ctx, "reconcile failed; retrying with backoff",
			slog.String("deployment_id", key), slog.Int("retries", c.queue.NumRequeues(key)),
			slog.String("cause", err.Error()))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	if res.RequeueAfter > 0 {
		c.queue.AddAfter(key, res.RequeueAfter)
	}
	c.o.Logger.DebugContext(ctx, "reconciled", slog.String("deployment_id", key),
		slog.Duration("took", time.Since(started)))
	return true
}
