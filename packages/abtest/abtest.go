// Package abtest compares the arms of an A/B experiment (docs/api.md, "Routes,
// rollouts, experiments"). It reports observed differences with bootstrap
// confidence intervals and declines, in words, to claim statistical significance:
// no multiple-comparison or sequential-testing correction is applied, so an
// interval read repeatedly while traffic arrives is not a significance test
// (axiom A6).
package abtest

import (
	"math"
	"math/rand/v2"
	"sort"
)

// Sample is one request, as the comparison needs it.
type Sample struct {
	DurationMS float64
	Error      bool
}

// Arm is one side of the experiment: its exact totals, and a sample of its
// requests for the intervals.
type Arm struct {
	Label            string   `json:"label"`
	DeploymentID     string   `json:"deployment_id"`
	Deployment       string   `json:"deployment"`
	Requests         int64    `json:"requests"`
	Errors           int64    `json:"errors"`
	ErrorRate        float64  `json:"error_rate"`
	P50MS            float64  `json:"p50_ms"`
	P95MS            float64  `json:"p95_ms"`
	TokensPerSecond  float64  `json:"tokens_per_second"`
	CostPer1kTokens  float64  `json:"cost_per_1k_tokens_micros"`
	CompletionTokens int64    `json:"completion_tokens"`
	ComputeMS        int64    `json:"-"`
	CostMicros       int64    `json:"-"`
	Samples          []Sample `json:"-"`
}

// Finish derives an arm's rates from its totals.
func (a *Arm) Finish() {
	if a.Requests > 0 {
		a.ErrorRate = round(float64(a.Errors)/float64(a.Requests), 4)
	}
	if a.ComputeMS > 0 {
		a.TokensPerSecond = round(float64(a.CompletionTokens)/(float64(a.ComputeMS)/1000), 1)
	}
	if a.CompletionTokens > 0 {
		a.CostPer1kTokens = round(float64(a.CostMicros)*1000/float64(a.CompletionTokens), 1)
	}
}

// Interval is one metric's comparison, variant minus baseline.
type Interval struct {
	Delta    float64    `json:"delta"`
	Relative *float64   `json:"relative,omitempty"`
	CI95     [2]float64 `json:"ci95"`
	Method   string     `json:"method"`
	Note     string     `json:"note,omitempty"`
}

// Comparison is the variant against the baseline.
type Comparison struct {
	Baseline  string    `json:"baseline"`
	Variant   string    `json:"variant"`
	P50MS     *Interval `json:"p50_ms,omitempty"`
	ErrorRate *Interval `json:"error_rate,omitempty"`
	Note      string    `json:"note,omitempty"`
}

// Significance is the explicit disclaimer every result carries.
type Significance struct {
	Claimed bool   `json:"claimed"`
	Reason  string `json:"reason"`
}

// Disclaimer is what the results say about significance.
func Disclaimer() Significance {
	return Significance{Claimed: false, Reason: "Bootstrap confidence intervals are reported. No multiple-comparison " +
		"correction or sequential-testing correction is applied, so these intervals must not be read as a " +
		"peeking-safe significance test."}
}

// MinSamples is the smallest sample per arm an interval is computed from.
const MinSamples = 30

// Compare bootstraps the difference in median latency and in error rate. The
// resampling is seeded, so the same data always gives the same intervals.
func Compare(base, variant Arm, resamples int, seed uint64) Comparison {
	c := Comparison{Baseline: base.Label, Variant: variant.Label}
	if len(base.Samples) < MinSamples || len(variant.Samples) < MinSamples {
		c.Note = "too few requests in an arm to compute intervals; at least 30 each are needed"
		return c
	}
	r := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)) //nolint:gosec // seeded on purpose: results must be reproducible
	medians := make([]float64, resamples)
	rates := make([]float64, resamples)
	bufB := make([]float64, len(base.Samples))
	bufV := make([]float64, len(variant.Samples))
	for i := range resamples {
		mb, eb := resample(r, base.Samples, bufB)
		mv, ev := resample(r, variant.Samples, bufV)
		medians[i] = mv - mb
		rates[i] = ev - eb
	}
	p50 := median(durations(variant.Samples)) - median(durations(base.Samples))
	c.P50MS = interval(round(p50, 1), medians, 1, base.P50MS)
	er := errorRate(variant.Samples) - errorRate(base.Samples)
	c.ErrorRate = interval(round(er, 4), rates, 4, 0)
	method := "bootstrap_" + itoa(resamples)
	c.P50MS.Method, c.ErrorRate.Method = method, method
	return c
}

func interval(delta float64, draws []float64, digits int, base float64) *Interval {
	sort.Float64s(draws)
	lo, hi := quantile(draws, 0.025), quantile(draws, 0.975)
	iv := &Interval{Delta: delta, CI95: [2]float64{round(lo, digits), round(hi, digits)}}
	if base > 0 {
		rel := round(delta/base, 3)
		iv.Relative = &rel
	}
	if lo <= 0 && hi >= 0 {
		iv.Note = "interval spans zero; no difference detected at this sample size"
	}
	return iv
}

// resample draws len(s) samples with replacement and returns their median
// duration and error rate.
func resample(r *rand.Rand, s []Sample, buf []float64) (med, rate float64) {
	errs := 0
	for i := range buf {
		x := s[r.IntN(len(s))]
		buf[i] = x.DurationMS
		if x.Error {
			errs++
		}
	}
	sort.Float64s(buf)
	return quantile(buf, 0.5), float64(errs) / float64(len(buf))
}

func durations(s []Sample) []float64 {
	out := make([]float64, len(s))
	for i, x := range s {
		out[i] = x.DurationMS
	}
	sort.Float64s(out)
	return out
}

func median(sorted []float64) float64 { return quantile(sorted, 0.5) }

func errorRate(s []Sample) float64 {
	if len(s) == 0 {
		return 0
	}
	n := 0
	for _, x := range s {
		if x.Error {
			n++
		}
	}
	return float64(n) / float64(len(s))
}

// quantile interpolates linearly between the closest ranks of sorted data.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	pos := q * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(pos-float64(lo))
}

func round(v float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(v*p) / p
}

func itoa(n int) string {
	if n >= 1000 && n%1000 == 0 {
		return formatInt(n/1000) + "k"
	}
	return formatInt(n)
}

func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
