package reliability_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/reliability"
)

// One case per row of the classification table.
func TestClassificationTable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		f    reliability.Failure
		want reliability.Class
	}{
		{reliability.Failure{ClientGone: true, Connect: true}, reliability.ClassClientGone},
		{reliability.Failure{Started: true, Status: 503}, reliability.ClassStarted},
		{reliability.Failure{Connect: true}, reliability.ClassConnect},
		{reliability.Failure{Status: 429}, reliability.ClassSaturated},
		{reliability.Failure{Status: 503, Reason: "draining"}, reliability.ClassUnavailable},
		{reliability.Failure{Timeout: true}, reliability.ClassTimeout},
		{reliability.Failure{Status: 400}, reliability.ClassClientError},
		{reliability.Failure{Status: 500}, reliability.ClassServerError},
		{reliability.Failure{Status: 200}, reliability.ClassOK},
	} {
		if got := reliability.Classify(tc.f); got != tc.want {
			t.Errorf("%+v: %s, want %s", tc.f, got.Name, tc.want.Name)
		}
	}
	if n := len(reliability.Table()); n != 9 {
		t.Errorf("table has %d rows; a new row needs a case above", n)
	}
}

// The critical negative: once output reached the client, nothing makes a failure
// retryable — not a connection error, not a 503, not a 429.
func TestStartedIsNeverRetried(t *testing.T) {
	t.Parallel()
	for _, f := range []reliability.Failure{
		{Started: true}, {Started: true, Connect: true}, {Started: true, Status: 503},
		{Started: true, Status: 429}, {Started: true, Timeout: true}, {Started: true, Status: 502},
	} {
		if c := reliability.Classify(f); c.Retry {
			t.Errorf("%+v classified %s, which retries", f, c.Name)
		}
	}
	for _, c := range reliability.Table() {
		if c.Retry && (c == reliability.ClassStarted || c == reliability.ClassTimeout || c == reliability.ClassServerError) {
			t.Errorf("%s may have generated output and must not retry", c.Name)
		}
	}
}

func TestBackoffIsFullJitterAndCapped(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	base, ceiling := 50*time.Millisecond, time.Second
	for n := 1; n <= 10; n++ {
		limit := base << (n - 1)
		if limit > ceiling {
			limit = ceiling
		}
		var lo, hi time.Duration = limit, 0
		for range 2000 {
			d := reliability.Backoff(n, base, ceiling, r)
			if d < 0 || d > limit {
				t.Fatalf("attempt %d: %s outside [0, %s]", n, d, limit)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		// Full jitter spans the whole range, not only its upper half.
		if lo > limit/10 || hi < limit*9/10 {
			t.Errorf("attempt %d: observed [%s, %s] of [0, %s]", n, lo, hi, limit)
		}
	}
	if reliability.Backoff(0, base, ceiling, r) != 0 {
		t.Error("attempt 0 waits")
	}
}

// Under a total outage every request fails and wants a retry; the budget keeps
// retries at ratio × requests plus the floor, whatever the attempt cap.
func TestBudgetCapsAmplificationUnderTotalOutage(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	b := &reliability.Budget{Ratio: 0.2, MinPerSecond: 1, Window: 10 * time.Second, Now: func() time.Time { return now }}
	requests, retries := 0, 0
	for second := range 60 {
		now = now.Add(time.Second)
		for range 100 {
			b.Request()
			requests++
			for range 3 { // an attempt cap of 3 would otherwise mean 3 retries each
				if b.Retry() {
					retries++
				}
			}
		}
		_ = second
	}
	amplification := float64(requests+retries) / float64(requests)
	if amplification > 1.25 {
		t.Fatalf("%d requests caused %d retries: amplification %.2f, want <= 1.25", requests, retries, amplification)
	}
	if retries == 0 {
		t.Fatal("the budget allowed no retries at all")
	}
}

// A quiet service still gets its floor.
func TestBudgetFloor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	b := &reliability.Budget{Ratio: 0.1, MinPerSecond: 1, Window: 5 * time.Second, Now: func() time.Time { return now }}
	b.Request()
	got := 0
	for range 20 {
		if b.Retry() {
			got++
		}
	}
	if got != 5 {
		t.Errorf("floor of 1/s over 5s plus 0.1 of one request allowed %d retries, want 5", got)
	}
	// The window slides: old retries stop counting.
	now = now.Add(10 * time.Second)
	if !b.Retry() {
		t.Error("the window did not slide")
	}
}
