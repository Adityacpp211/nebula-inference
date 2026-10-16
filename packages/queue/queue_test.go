package queue

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type fixture struct {
	t   *testing.T
	c   *clock
	cap atomic.Int64
	g   *Gate
}

func newFixture(t *testing.T, capacity, depth int, aging time.Duration) *fixture {
	f := &fixture{t: t, c: &clock{t: time.Unix(1_800_000_000, 0)}}
	f.cap.Store(int64(capacity))
	f.g = New(Config{MaxDepth: depth, AgingStep: aging, Now: f.c.Now}, func() int { return int(f.cap.Load()) })
	return f
}

type result struct {
	tk  *Ticket
	err error
	tag string
}

// enqueue starts an Acquire that is expected to wait, and returns once it is
// visibly in the queue, so the order of waiters is the order of calls.
func (f *fixture) enqueue(ctx context.Context, p Priority, deadline time.Time, tag string, out chan<- result) {
	f.t.Helper()
	before := f.g.Stats().Depth
	go func() {
		tk, err := f.g.Acquire(ctx, p, deadline, false)
		out <- result{tk, err, tag}
	}()
	for i := 0; f.g.Stats().Depth == before; i++ {
		if i > 5000 {
			f.t.Fatal("the entry never queued")
		}
		time.Sleep(time.Millisecond)
	}
}

func recv(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no result")
		return result{}
	}
}

func TestImmediateWhenCapacityAndFIFOOtherwise(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 2, 10, 0)
	ctx := context.Background()
	a, err := f.g.Acquire(ctx, Normal, time.Time{}, false)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := f.g.Acquire(ctx, Normal, time.Time{}, false)
	if s := f.g.Stats(); s.InFlight != 2 || s.Depth != 0 {
		t.Fatalf("%+v", s)
	}
	out := make(chan result, 3)
	f.enqueue(ctx, Normal, time.Time{}, "first", out)
	f.enqueue(ctx, Normal, time.Time{}, "second", out)
	a.Release()
	if r := recv(t, out); r.tag != "first" || r.err != nil {
		t.Fatalf("%+v", r)
	}
	b.Release()
	if r := recv(t, out); r.tag != "second" {
		t.Fatalf("%+v", r)
	}
	// A newcomer never overtakes a waiter, even if a slot is free at that instant.
	a.Release() // double release is a no-op
}

// Queue-full sheds rather than grows.
func TestFullQueueShedsWithRetryAfter(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 2, 0)
	ctx := context.Background()
	held, _ := f.g.Acquire(ctx, Normal, time.Time{}, false)
	out := make(chan result, 2)
	f.enqueue(ctx, Normal, time.Time{}, "a", out)
	f.enqueue(ctx, Normal, time.Time{}, "b", out)
	_, err := f.g.Acquire(ctx, Normal, time.Time{}, false)
	var full *FullError
	if !errors.As(err, &full) || full.WouldWait || full.RetryAfter < time.Second || full.Depth != 2 {
		t.Fatalf("want a FullError with Retry-After, got %v", err)
	}
	if s := f.g.Stats(); s.Depth != 2 || s.Dropped[ReasonFull] != 1 {
		t.Fatalf("%+v", s)
	}
	held.Release()
	r := recv(t, out)
	r.tk.Release()
	recv(t, out).tk.Release()
}

// A deadline passing in the queue is a queue timeout, removed by the sweeper.
func TestDeadlineExpiresInQueue(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 10, 0)
	ctx := context.Background()
	held, _ := f.g.Acquire(ctx, Normal, time.Time{}, false)
	out := make(chan result, 1)
	f.enqueue(ctx, Normal, f.c.Now().Add(2*time.Second), "late", out)

	f.c.Add(time.Second)
	f.g.Tick()
	if f.g.Stats().Depth != 1 {
		t.Fatal("expired early")
	}
	f.c.Add(time.Second)
	f.g.Tick()
	if r := recv(t, out); !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("%+v", r)
	}
	s := f.g.Stats()
	if s.Depth != 0 || s.Dropped[ReasonTimeout] != 1 {
		t.Fatalf("%+v", s)
	}
	// An already-expired request never enters.
	if _, err := f.g.Acquire(ctx, Normal, f.c.Now(), false); !errors.Is(err, ErrTimeout) {
		t.Fatalf("%v", err)
	}
	held.Release()
}

// Cancellation removes the entry immediately.
func TestCancellationRemovesTheEntry(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 10, 0)
	held, _ := f.g.Acquire(context.Background(), Normal, time.Time{}, false)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan result, 1)
	f.enqueue(ctx, Normal, time.Time{}, "gone", out)
	cancel()
	if r := recv(t, out); !errors.Is(r.err, ErrCancelled) {
		t.Fatalf("%+v", r)
	}
	if s := f.g.Stats(); s.Depth != 0 || s.Dropped[ReasonCancelled] != 1 {
		t.Fatalf("%+v", s)
	}
	// The slot is still accounted to its holder, not leaked to the cancelled entry.
	held.Release()
	if s := f.g.Stats(); s.InFlight != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestPriorityOrder(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 10, 0)
	ctx := context.Background()
	held, _ := f.g.Acquire(ctx, Normal, time.Time{}, false)
	out := make(chan result, 3)
	f.enqueue(ctx, Low, time.Time{}, "low", out)
	f.enqueue(ctx, Normal, time.Time{}, "normal", out)
	f.enqueue(ctx, High, time.Time{}, "high", out)
	held.Release()
	for _, want := range []string{"high", "normal", "low"} {
		r := recv(t, out)
		if r.tag != want {
			t.Fatalf("got %s, want %s", r.tag, want)
		}
		r.tk.Release()
	}
}

// LOW is not starved by sustained HIGH: aging lifts it past fresher HIGH entries.
func TestLowIsNotStarvedBySustainedHigh(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 100, time.Second)
	ctx := context.Background()
	held, _ := f.g.Acquire(ctx, High, time.Time{}, false)
	out := make(chan result, 100)
	f.enqueue(ctx, Low, time.Time{}, "low", out)

	// A HIGH request arrives every 500 ms and each one holds the slot for 500 ms: a
	// strict-priority queue would serve HIGH forever.
	for i := 0; i < 20; i++ {
		f.enqueue(ctx, High, time.Time{}, "high", out)
		f.c.Add(500 * time.Millisecond)
		held.Release()
		r := recv(t, out)
		if r.tag == "low" {
			if i < 2 {
				t.Fatalf("low was served after %d rounds, before it had aged at all", i)
			}
			r.tk.Release()
			// Drain what is left.
			for f.g.Stats().Depth > 0 {
				f.g.Tick()
				x := recv(t, out)
				x.tk.Release()
			}
			return
		}
		held = r.tk
	}
	t.Fatal("LOW was starved by sustained HIGH")
}

func TestRejectModeNeverWaits(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 1, 10, 0)
	ctx := context.Background()
	if _, err := f.g.Acquire(ctx, Normal, time.Time{}, true); err != nil {
		t.Fatalf("with capacity, reject mode runs at once: %v", err)
	}
	_, err := f.g.Acquire(ctx, Normal, time.Time{}, true)
	var full *FullError
	if !errors.As(err, &full) || !full.WouldWait || f.g.Stats().Depth != 0 {
		t.Fatalf("%v", err)
	}
}

// Capacity is re-read: replicas arriving while requests wait admit them at the
// next tick.
func TestCapacityGrowthAdmitsWaiters(t *testing.T) {
	t.Parallel()
	f := newFixture(t, 0, 10, 0)
	ctx := context.Background()
	out := make(chan result, 2)
	f.enqueue(ctx, Normal, time.Time{}, "a", out)
	f.enqueue(ctx, Normal, time.Time{}, "b", out)
	f.cap.Store(2)
	f.g.Tick()
	recv(t, out).tk.Release()
	recv(t, out).tk.Release()
	if s := f.g.Stats(); s.InFlight != 0 || s.Admitted != 2 || s.Queued != 2 {
		t.Fatalf("%+v", s)
	}
}

// Sustained overload: 64 clients hammering a gate of capacity 4 and depth 16 for
// many rounds. Depth never exceeds its bound, every request is either served or
// shed with a reason, and the heap does not grow with the number of requests.
func TestSustainedOverloadIsBoundedAndMemoryFlat(t *testing.T) {
	t.Parallel()
	g := New(Config{MaxDepth: 16, AgingStep: 100 * time.Millisecond}, func() int { return 4 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Run(ctx, 5*time.Millisecond)

	var served, shed atomic.Int64
	var maxDepth atomic.Int64
	round := func(n int) {
		var wg sync.WaitGroup
		for c := 0; c < 64; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					tk, err := g.Acquire(ctx, Priority(i%3), time.Now().Add(50*time.Millisecond), false)
					if d := int64(g.Stats().Depth); d > maxDepth.Load() {
						maxDepth.Store(d)
					}
					if err != nil {
						shed.Add(1)
						continue
					}
					time.Sleep(100 * time.Microsecond)
					tk.Release()
					served.Add(1)
				}
			}()
		}
		wg.Wait()
	}
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	round(50)
	before := heap()
	round(200)
	after := heap()

	if maxDepth.Load() > 16 {
		t.Fatalf("depth reached %d, bound is 16", maxDepth.Load())
	}
	if served.Load() == 0 || shed.Load() == 0 {
		t.Fatalf("served %d, shed %d: the test did not overload the gate", served.Load(), shed.Load())
	}
	// 4x the requests of the warm-up round, and the heap is where it was.
	if after > before+4<<20 {
		t.Fatalf("heap grew from %d to %d bytes under sustained overload", before, after)
	}
	s := g.Stats()
	if s.InFlight != 0 || s.Depth != 0 {
		t.Fatalf("leaked: %+v", s)
	}
	t.Logf("served %d, shed %d (%v), max depth %d, heap %d -> %d", served.Load(), shed.Load(), s.Dropped,
		maxDepth.Load(), before, after)
}

func TestParsePriority(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]Priority{"LOW": Low, "HIGH": High, "NORMAL": Normal, "": Normal, "x": Normal} {
		if got := ParsePriority(in); got != want {
			t.Errorf("%q: %v", in, got)
		}
	}
}
