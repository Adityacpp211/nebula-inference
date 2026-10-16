// Package rollout decides a canary rollout's next step (docs/architecture.md §7.3).
// It is pure: a rollout's configuration and both arms' observations in, a verdict
// out. The controller's rollout reconciler owns the clock, the queries and the
// writes around it.
//
//	promote:  [ 0% ] → 10% → 25% → 50% → 100%
//	                     after each step: hold, then analyse
//	                       error rate ≤ baseline + max_error_rate_increase ?
//	                       p95 latency ≤ baseline × max_p95_ratio ?
//	                       min_requests reached ?
//	                     all pass → next step; any fail → abort, weights to baseline
//
// A step with fewer than min_requests observations does not pass: by
// on_insufficient_data it extends the window, pauses for an operator, or fails.
// Reaching the last step, which is always 100, is promotion; there is nothing left
// to compare against once the baseline serves no traffic.
package rollout

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Steps parses a rollout's canary weights: strictly increasing percentages ending
// at 100. An empty list takes the documented default.
func Steps(raw json.RawMessage) ([]int, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []int{10, 25, 50, 100}, nil
	}
	var steps []int
	if err := json.Unmarshal(raw, &steps); err != nil {
		return nil, fmt.Errorf("steps must be an array of percentages: %w", err)
	}
	if len(steps) == 0 {
		return nil, errors.New("steps must not be empty")
	}
	prev := 0
	for _, s := range steps {
		if s <= prev || s > 100 {
			return nil, fmt.Errorf("steps must increase strictly within 1..100 (got %v)", steps)
		}
		prev = s
	}
	if prev != 100 {
		return nil, fmt.Errorf("the last step must be 100, which is promotion (got %d)", prev)
	}
	return steps, nil
}

// On insufficient data.
const (
	Extend = "extend"
	Pause  = "pause"
	Fail   = "fail"
)

// Analysis is a rollout's pass criteria, the canary_rollouts.analysis object.
type Analysis struct {
	// HoldS is how long each step serves before it is analysed.
	HoldS int `json:"hold_s"`
	// MinRequests is the canary requests a step needs before it can pass.
	MinRequests int `json:"min_requests"`
	// MaxErrorRateIncrease is how far the canary's error rate may exceed the
	// baseline's, as a fraction: 0.01 is one percentage point.
	MaxErrorRateIncrease float64 `json:"max_error_rate_increase"`
	// MaxP95Ratio bounds the canary's p95 latency as a multiple of the baseline's.
	MaxP95Ratio float64 `json:"max_p95_ratio"`
	// OnInsufficientData is extend, pause or fail.
	OnInsufficientData string `json:"on_insufficient_data"`
	// MaxExtensions bounds "extend": after this many extra hold windows the step
	// pauses for an operator rather than waiting forever.
	MaxExtensions int `json:"max_extensions"`
}

// Defaults (docs/architecture.md §7.3).
const (
	DefaultHoldS                = 120
	DefaultMinRequests          = 100
	DefaultMaxErrorRateIncrease = 0.01
	DefaultMaxP95Ratio          = 1.2
	DefaultMaxExtensions        = 3
)

// ParseAnalysis reads an analysis object, applies defaults and refuses values that
// would make the gate meaningless.
func ParseAnalysis(raw json.RawMessage) (Analysis, error) {
	var a Analysis
	if len(raw) > 0 && string(raw) != "null" {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&a); err != nil {
			return Analysis{}, fmt.Errorf("analysis: %w", err)
		}
	}
	if a.HoldS == 0 {
		a.HoldS = DefaultHoldS
	}
	if a.MinRequests == 0 {
		a.MinRequests = DefaultMinRequests
	}
	if a.MaxErrorRateIncrease == 0 {
		a.MaxErrorRateIncrease = DefaultMaxErrorRateIncrease
	}
	if a.MaxP95Ratio == 0 {
		a.MaxP95Ratio = DefaultMaxP95Ratio
	}
	if a.OnInsufficientData == "" {
		a.OnInsufficientData = Extend
	}
	if a.MaxExtensions == 0 {
		a.MaxExtensions = DefaultMaxExtensions
	}
	switch {
	case a.HoldS < 10 || a.HoldS > 86400:
		return Analysis{}, errors.New("analysis: hold_s must be between 10 and 86400")
	case a.MinRequests < 1:
		return Analysis{}, errors.New("analysis: min_requests must be at least 1")
	case a.MaxErrorRateIncrease < 0 || a.MaxErrorRateIncrease > 1:
		return Analysis{}, errors.New("analysis: max_error_rate_increase must be in [0, 1]")
	case a.MaxP95Ratio < 1:
		return Analysis{}, errors.New("analysis: max_p95_ratio must be at least 1: a canary may not be held faster than its baseline")
	case a.OnInsufficientData != Extend && a.OnInsufficientData != Pause && a.OnInsufficientData != Fail:
		return Analysis{}, errors.New("analysis: on_insufficient_data must be extend, pause or fail")
	case a.MaxExtensions < 0 || a.MaxExtensions > 100:
		return Analysis{}, errors.New("analysis: max_extensions must be in [0, 100]")
	}
	return a, nil
}

// Arm is one side's observations over the analysis window.
type Arm struct {
	Requests float64 `json:"requests"`
	Errors   float64 `json:"errors"`
	// P95MS is the p95 request duration; NaN when it could not be computed.
	P95MS float64 `json:"p95_ms"`
}

// MarshalJSON writes an unknown p95 as null: JSON has no NaN, and the verdict is
// stored in canary_rollouts.last_analysis.
func (a Arm) MarshalJSON() ([]byte, error) {
	var p95 *float64
	if known(a.P95MS) {
		v := round1(a.P95MS)
		p95 = &v
	}
	return json.Marshal(struct {
		Requests float64  `json:"requests"`
		Errors   float64  `json:"errors"`
		P95MS    *float64 `json:"p95_ms"`
	}{math.Round(a.Requests*100) / 100, math.Round(a.Errors*100) / 100, p95})
}

// ErrorRate is errors over requests; zero with no requests.
func (a Arm) ErrorRate() float64 {
	if a.Requests <= 0 {
		return 0
	}
	return a.Errors / a.Requests
}

// Outcomes.
const (
	Pass         = "pass"
	Breach       = "breach"
	Insufficient = "insufficient_data"
)

// Check is one criterion's result, kept in canary_rollouts.last_analysis so an
// abort is explainable from the row.
type Check struct {
	Name     string  `json:"name"`
	Baseline float64 `json:"baseline"`
	Canary   float64 `json:"canary"`
	Limit    float64 `json:"limit"`
	Passed   bool    `json:"passed"`
}

// Verdict is one analysis.
type Verdict struct {
	Outcome  string  `json:"outcome"`
	Reason   string  `json:"reason"`
	Baseline Arm     `json:"baseline"`
	Canary   Arm     `json:"canary"`
	Checks   []Check `json:"checks"`
}

// Evaluate compares the arms. Breaches are judged before sufficiency: a canary
// failing every request it got is aborted at once, not after it has failed enough
// requests to be "significant".
func Evaluate(a Analysis, baseline, canary Arm) Verdict {
	v := Verdict{Outcome: Pass, Baseline: baseline, Canary: canary}
	br, cr := round4(baseline.ErrorRate()), round4(canary.ErrorRate())
	errLimit := round4(br + a.MaxErrorRateIncrease)
	errCheck := Check{Name: "error_rate", Baseline: br, Canary: cr, Limit: errLimit, Passed: cr <= errLimit}
	v.Checks = append(v.Checks, errCheck)

	latency := Check{Name: "p95_ms", Baseline: round1(baseline.P95MS), Canary: round1(canary.P95MS), Passed: true}
	if known(baseline.P95MS) && known(canary.P95MS) && baseline.P95MS > 0 {
		latency.Limit = round1(baseline.P95MS * a.MaxP95Ratio)
		latency.Passed = canary.P95MS <= latency.Limit
	}
	v.Checks = append(v.Checks, latency)

	volume := Check{Name: "min_requests", Canary: math.Floor(canary.Requests), Limit: float64(a.MinRequests),
		Passed: canary.Requests >= float64(a.MinRequests)}
	v.Checks = append(v.Checks, volume)

	switch {
	case !errCheck.Passed && canary.Errors >= 1:
		v.Outcome = Breach
		v.Reason = fmt.Sprintf("canary error rate %.2f%% exceeds the baseline's %.2f%% + %.2f pp",
			cr*100, br*100, a.MaxErrorRateIncrease*100)
	case !latency.Passed:
		v.Outcome = Breach
		v.Reason = fmt.Sprintf("canary p95 %.0f ms exceeds the baseline's %.0f ms × %.2f",
			canary.P95MS, baseline.P95MS, a.MaxP95Ratio)
	case !volume.Passed:
		v.Outcome = Insufficient
		v.Reason = fmt.Sprintf("the canary served %.0f requests in the window; %d are needed",
			canary.Requests, a.MinRequests)
	default:
		v.Reason = fmt.Sprintf("canary error rate %.2f%% within %.2f%%; p95 %s; %.0f requests",
			cr*100, errLimit*100, p95Note(latency), canary.Requests)
	}
	return v
}

func p95Note(c Check) string {
	if c.Limit == 0 {
		return "not comparable"
	}
	return fmt.Sprintf("%.0f ms within %.0f ms", c.Canary, c.Limit)
}

func known(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func round4(v float64) float64 { return math.Round(v*1e4) / 1e4 }

func round1(v float64) float64 {
	if !known(v) {
		return 0
	}
	return math.Round(v*10) / 10
}

// Weights are the route's target weights for a canary step.
func Weights(canary int) (baselineWeight, canaryWeight int) { return 100 - canary, canary }
