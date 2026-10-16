package abtest_test

import (
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/abtest"
)

func arm(label string, n int, centre, spread float64, errorEvery int, seed uint64) abtest.Arm {
	r := rand.New(rand.NewPCG(seed, 1))
	a := abtest.Arm{Label: label, Requests: int64(n)}
	for i := range n {
		e := errorEvery > 0 && i%errorEvery == 0
		if e {
			a.Errors++
		}
		a.Samples = append(a.Samples, abtest.Sample{DurationMS: centre + (r.Float64()-0.5)*spread, Error: e})
	}
	return a
}

// Arms with identical data: the interval spans zero, and the result says so.
// (Two different draws from one distribution exclude zero about one time in
// twenty — that is what a 95% interval means.)
func TestIdenticalArmsSpanZero(t *testing.T) {
	t.Parallel()
	c := abtest.Compare(arm("baseline", 800, 400, 100, 50, 1), arm("variant", 800, 400, 100, 50, 1), 2000, 7)
	if c.P50MS == nil || c.P50MS.CI95[0] > 0 || c.P50MS.CI95[1] < 0 || !strings.Contains(c.P50MS.Note, "spans zero") {
		t.Fatalf("p50: %+v", c.P50MS)
	}
	if c.ErrorRate.CI95[0] > 0 || c.ErrorRate.CI95[1] < 0 {
		t.Fatalf("error rate: %+v", c.ErrorRate)
	}
}

// A clearly slower variant: the interval excludes zero and is centred on the
// difference.
func TestClearDifferenceExcludesZero(t *testing.T) {
	t.Parallel()
	c := abtest.Compare(arm("baseline", 500, 400, 100, 0, 1), arm("variant", 500, 600, 100, 0, 2), 2000, 7)
	if c.P50MS.CI95[0] <= 0 || c.P50MS.Delta < 180 || c.P50MS.Delta > 220 || c.P50MS.Note != "" {
		t.Fatalf("p50: %+v", c.P50MS)
	}
	if c.P50MS.Method != "bootstrap_2k" {
		t.Errorf("method %q", c.P50MS.Method)
	}
}

// The same data and seed give the same intervals: a result is reproducible.
func TestDeterministic(t *testing.T) {
	t.Parallel()
	b, v := arm("b", 300, 400, 200, 10, 1), arm("v", 300, 420, 200, 8, 2)
	x, y := abtest.Compare(b, v, 1000, 42), abtest.Compare(b, v, 1000, 42)
	if x.P50MS.Delta != y.P50MS.Delta || x.P50MS.CI95 != y.P50MS.CI95 || x.ErrorRate.CI95 != y.ErrorRate.CI95 {
		t.Fatalf("%+v vs %+v", x.P50MS, y.P50MS)
	}
}

func TestTooFewSamples(t *testing.T) {
	t.Parallel()
	c := abtest.Compare(arm("b", 10, 400, 10, 0, 1), arm("v", 500, 400, 10, 0, 2), 1000, 1)
	if c.P50MS != nil || !strings.Contains(c.Note, "too few") {
		t.Fatalf("%+v", c)
	}
	if d := abtest.Disclaimer(); d.Claimed || !strings.Contains(d.Reason, "must not be read") {
		t.Fatalf("%+v", d)
	}
}

func TestFinish(t *testing.T) {
	t.Parallel()
	a := abtest.Arm{Requests: 200, Errors: 3, CompletionTokens: 4000, ComputeMS: 20_000, CostMicros: 1000}
	a.Finish()
	if a.ErrorRate != 0.015 || a.TokensPerSecond != 200 || a.CostPer1kTokens != 250 {
		t.Fatalf("%+v", a)
	}
}
