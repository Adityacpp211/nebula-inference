package costing_test

import (
	"encoding/json"
	"testing"

	"github.com/adityasatwar321/nebula/packages/costing"
)

// A profile with round numbers, so every golden value can be checked by hand.
var priced = costing.Profile{
	Name: "test", Version: 1, Currency: "USD",
	CPUCoreHourMicros:   36_000, // $0.036 per core-hour
	MemoryGiBHourMicros: 7_200,  // $0.0072 per GiB-hour
	GPUHourMicros:       map[string]int64{"nvidia-l4": 720_000},
}

// Golden values: known inputs, exact micros, no float drift.
func TestGoldenCosts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		p         costing.Profile
		r         costing.Resources
		slots     int64
		computeMS int64
		want      int64
	}{
		// 1 core + 1 GiB = 36 000 + 7 200 = 43 200 µ/h = 12 µ/s; one slot, 1 s.
		{"one second, one slot", priced, costing.Resources{CPUMilli: 1000, MemoryMiB: 1024}, 1, 1000, 12},
		// The same replica shared by 4 slots: 3 µ per request-second.
		{"four slots", priced, costing.Resources{CPUMilli: 1000, MemoryMiB: 1024}, 4, 1000, 3},
		// An hour of a whole replica is its hourly price.
		{"one replica-hour", priced, costing.Resources{CPUMilli: 2000, MemoryMiB: 4096}, 1, 3_600_000, 72_000 + 28_800},
		// 500 m CPU, 512 MiB: 18 000 + 3 600 = 21 600 µ/h = 6 µ/s; 250 ms on one of 2 slots = 0.75 → 1.
		{"rounds half up", priced, costing.Resources{CPUMilli: 500, MemoryMiB: 512}, 2, 250, 1},
		// 0.5 exactly rounds up: 6 µ/s × 0.25 s / 3 slots = 0.5.
		{"exact half", priced, costing.Resources{CPUMilli: 500, MemoryMiB: 512}, 3, 250, 1},
		// 0.4 rounds down.
		{"below half", priced, costing.Resources{CPUMilli: 500, MemoryMiB: 512}, 3, 200, 0},
		// A GPU: 720 000 µ/h = 200 µ/s, plus 1 core = 10 µ/s, for 2 s = 420.
		{"gpu", priced, costing.Resources{CPUMilli: 1000, GPUCount: 1, GPUType: "nvidia-l4"}, 1, 2000, 420},
		// An unpriced GPU type costs nothing rather than a guess.
		{"unpriced gpu", priced, costing.Resources{GPUCount: 1, GPUType: "unknown"}, 1, 2000, 0},
		// 25% markup on 12 µ = 15.
		{"markup", withMarkup(2500), costing.Resources{CPUMilli: 1000, MemoryMiB: 1024}, 1, 1000, 15},
		{"no compute", priced, costing.Resources{CPUMilli: 1000}, 1, 0, 0},
		{"zero slots is one", priced, costing.Resources{CPUMilli: 1000, MemoryMiB: 1024}, 0, 1000, 12},
		// The seeded default prices everything at zero.
		{"default profile", costing.Profile{}, costing.Resources{CPUMilli: 4000, MemoryMiB: 8192}, 1, 60_000, 0},
	} {
		if got := costing.RequestCostMicros(tc.p, tc.r, tc.slots, tc.computeMS); got != tc.want {
			t.Errorf("%s: %d µ, want %d", tc.name, got, tc.want)
		}
	}
}

func withMarkup(bp int32) costing.Profile {
	p := priced
	p.MarkupBasisPoints = bp
	return p
}

// Summing many small requests and costing them one by one agree to within the
// per-request rounding, and the arithmetic is the same on every run.
func TestNoDrift(t *testing.T) {
	t.Parallel()
	r := costing.Resources{CPUMilli: 1000, MemoryMiB: 1024}
	var sum int64
	for range 1000 {
		sum += costing.RequestCostMicros(priced, r, 1, 1000)
	}
	if sum != 12_000 {
		t.Fatalf("1000 one-second requests cost %d µ, want 12 000", sum)
	}
}

// A price change is a new profile version. A cost computed under the old version
// is unchanged when recomputed under that version, whatever the new one says.
func TestHistoricalCostIsReproducible(t *testing.T) {
	t.Parallel()
	r := costing.Resources{CPUMilli: 2000, MemoryMiB: 2048}
	before := costing.RequestCostMicros(priced, r, 2, 1234)
	raised := priced
	raised.Version, raised.CPUCoreHourMicros = 2, priced.CPUCoreHourMicros*3
	if costing.RequestCostMicros(raised, r, 2, 1234) == before {
		t.Fatal("the new version should price differently")
	}
	if again := costing.RequestCostMicros(priced, r, 2, 1234); again != before {
		t.Fatalf("recomputed under the same version: %d, was %d", again, before)
	}
}

func TestSamplingCorrection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		sampled int64
		rate    float64
		want    int64
	}{
		{100, 1, 100}, {100, 0.1, 1000}, {7, 0.25, 28}, {1, 0.3, 3}, {5, 0, 5}, {5, 1.5, 5},
	} {
		if got := costing.Corrected(tc.sampled, tc.rate); got != tc.want {
			t.Errorf("Corrected(%d, %v) = %d, want %d", tc.sampled, tc.rate, got, tc.want)
		}
	}
}

func TestParsing(t *testing.T) {
	t.Parallel()
	g, err := costing.ParseGPUPrices(json.RawMessage(`{"nvidia-l4": 800000}`))
	if err != nil || g["nvidia-l4"] != 800000 {
		t.Fatalf("%v %v", g, err)
	}
	if _, err := costing.ParseGPUPrices(json.RawMessage(`{"x": -1}`)); err == nil {
		t.Error("a negative price must be refused")
	}
	r, err := costing.ParseResources(json.RawMessage(`{"cpu_milli": 500, "memory_mib": 256}`))
	if err != nil || r.CPUMilli != 500 || r.MemoryMiB != 256 {
		t.Fatalf("%+v %v", r, err)
	}
	if len(costing.Assumptions()) < 4 {
		t.Error("the assumptions must be stated")
	}
}
