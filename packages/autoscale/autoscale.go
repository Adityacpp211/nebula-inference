// Package autoscale decides a deployment's replica count (docs/architecture.md
// §7.2). It is pure: signals and a clock in, a decision out, with the per-
// deployment memory the guards need kept in a History the caller owns.
//
// The shape is the Kubernetes HPA's, so its behaviour is predictable to anyone who
// knows Kubernetes:
//
//	desired = ceil(current × max over signals of (observed / target))
//
// wrapped in guards that are all mandatory:
//
//   - a signal that is missing is not zero: with no usable signal the decision is
//     to hold, never to fall back to the minimum;
//   - a ratio within the tolerance of 1 changes nothing;
//   - stabilization windows: scale up only to what every recommendation in the
//     up-window supports, scale down only to what every recommendation in the
//     down-window allows, and act on neither until the window has been observed;
//   - a cooldown after any change;
//   - a maximum step: at most double up, at most 10% (or one replica) down;
//   - the deployment's [min, max] bounds;
//   - no scaling while a rollout is in progress.
//
// Every decision, including each suppressed one, carries the full signal vector
// and a sentence saying why, which is what an autoscaling_events row stores.
package autoscale

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

// Settings is a deployment's autoscaling configuration, the deployments.autoscaling
// object. Zero values take the defaults.
type Settings struct {
	Enabled bool `json:"enabled"`
	// TargetQueueWaitMS is the p95 worker queue wait to hold.
	TargetQueueWaitMS float64 `json:"target_queue_wait_ms,omitempty"`
	// TargetConcurrency is requests in flight or queued per replica. Zero uses the
	// worker's own parallel slots.
	TargetConcurrency float64 `json:"target_concurrency,omitempty"`
	// Windows and cooldown, in seconds.
	ScaleUpStabilizationS   int `json:"scale_up_stabilization_s,omitempty"`
	ScaleDownStabilizationS int `json:"scale_down_stabilization_s,omitempty"`
	CooldownS               int `json:"cooldown_s,omitempty"`
	// Tolerance is how far from 1 the ratio must be before anything changes.
	Tolerance float64 `json:"tolerance,omitempty"`
}

// Defaults (docs/architecture.md §7.2).
const (
	DefaultTargetQueueWaitMS       = 500
	DefaultScaleUpStabilizationS   = 30
	DefaultScaleDownStabilizationS = 300
	DefaultCooldownS               = 30
	DefaultTolerance               = 0.1
)

// ParseSettings reads a deployments.autoscaling object, applies defaults and
// refuses values that would make the guards meaningless.
func ParseSettings(raw json.RawMessage) (Settings, error) {
	var s Settings
	if len(raw) > 0 && string(raw) != "null" {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&s); err != nil {
			return Settings{}, fmt.Errorf("autoscaling: %w", err)
		}
	}
	if s.TargetQueueWaitMS == 0 {
		s.TargetQueueWaitMS = DefaultTargetQueueWaitMS
	}
	if s.ScaleUpStabilizationS == 0 {
		s.ScaleUpStabilizationS = DefaultScaleUpStabilizationS
	}
	if s.ScaleDownStabilizationS == 0 {
		s.ScaleDownStabilizationS = DefaultScaleDownStabilizationS
	}
	if s.CooldownS == 0 {
		s.CooldownS = DefaultCooldownS
	}
	if s.Tolerance == 0 {
		s.Tolerance = DefaultTolerance
	}
	switch {
	case s.TargetQueueWaitMS < 0 || s.TargetConcurrency < 0:
		return Settings{}, fmt.Errorf("autoscaling: targets must not be negative")
	case s.ScaleUpStabilizationS < 0 || s.ScaleDownStabilizationS < 0 || s.CooldownS < 0:
		return Settings{}, fmt.Errorf("autoscaling: windows and cooldown must not be negative")
	case s.ScaleDownStabilizationS > 3600 || s.ScaleUpStabilizationS > 3600 || s.CooldownS > 3600:
		return Settings{}, fmt.Errorf("autoscaling: windows and cooldown must be at most 3600 s")
	case s.Tolerance < 0 || s.Tolerance >= 1:
		return Settings{}, fmt.Errorf("autoscaling: tolerance must be in [0, 1)")
	}
	return s, nil
}

// Signal is one input, as stored in the decision's signal vector.
type Signal struct {
	Name     string  `json:"name"`
	Observed float64 `json:"observed"`
	Target   float64 `json:"target"`
	Ratio    float64 `json:"ratio"`
	Source   string  `json:"source"`
	// Stale means the signal could not be read; it is never used as zero.
	Stale    bool `json:"stale"`
	Selected bool `json:"selected"`
}

// Directions and decisions: the closed sets of autoscaling_events.
const (
	Up   = "up"
	Down = "down"
	Hold = "hold"

	Scaled                  = "scaled"
	SuppressedCooldown      = "suppressed_cooldown"
	SuppressedStabilization = "suppressed_stabilization"
	SuppressedRollout       = "suppressed_rollout"
	SuppressedNoSignal      = "suppressed_no_signal"
	ClampedMax              = "clamped_max"
	ClampedMin              = "clamped_min"
	Disabled                = "disabled"
)

// Input is everything one decision needs.
type Input struct {
	Now           time.Time
	Current       int32 // desired replicas now
	Min, Max      int32
	Settings      Settings
	Signals       []Signal
	RolloutActive bool
}

// Decision is the outcome, shaped like an autoscaling_events row.
type Decision struct {
	From      int32    `json:"from_replicas"`
	To        int32    `json:"to_replicas"`
	Direction string   `json:"direction"`
	Decision  string   `json:"decision"`
	Reason    string   `json:"reason"`
	Signals   []Signal `json:"signals"`
	// Recommended is the raw recommendation before the guards.
	Recommended int32 `json:"recommended"`
	// Record says the decision is worth a row: a change, or a suppression that
	// differs from the last one recorded. Steady state writes nothing.
	Record bool `json:"-"`
}

// Changed reports whether the decision changes the replica count.
func (d Decision) Changed() bool { return d.Decision == Scaled && d.To != d.From }

type recommendation struct {
	at       time.Time
	replicas int32
}

// History is one deployment's memory between decisions. The zero value is ready.
type History struct {
	firstSeen  time.Time
	recs       []recommendation
	lastChange time.Time
	last       string // the last recorded decision's identity, for de-duplication
}

// Decide makes one decision and updates the history.
func (h *History) Decide(in Input) Decision {
	s := in.Settings
	current := in.Current
	d := Decision{From: current, To: current, Direction: Hold, Signals: in.Signals}

	if !s.Enabled {
		d.Decision, d.Reason = Disabled, "autoscaling is disabled for this deployment"
		return h.finish(d)
	}
	if h.firstSeen.IsZero() {
		h.firstSeen = in.Now
	}

	// Ratios over usable signals; the largest wins.
	best := -1
	for i := range d.Signals {
		sig := &d.Signals[i]
		if sig.Stale || sig.Target <= 0 || math.IsNaN(sig.Observed) || math.IsInf(sig.Observed, 0) {
			sig.Stale = true
			continue
		}
		sig.Ratio = round(sig.Observed / sig.Target)
		if best < 0 || sig.Ratio > d.Signals[best].Ratio {
			best = i
		}
	}
	if best < 0 {
		d.Decision = SuppressedNoSignal
		d.Reason = "no usable signal: holding at the current replica count rather than guessing"
		return h.finish(d)
	}
	sel := &d.Signals[best]
	sel.Selected = true

	base := current
	if base < 1 {
		base = 1
	}
	raw := current
	if math.Abs(sel.Ratio-1) > s.Tolerance {
		raw = int32(math.Ceil(float64(base) * sel.Ratio))
	}
	d.Recommended = raw
	h.remember(in.Now, raw, s)
	why := fmt.Sprintf("%s %.4g vs target %.4g (ratio %.2f) recommends %d", sel.Name, sel.Observed,
		sel.Target, sel.Ratio, raw)

	if raw == current {
		d.Decision, d.Reason = Scaled, why+": no change needed"
		return h.finish(d) // steady state: nothing to record
	}
	direction := Up
	if raw < current {
		direction = Down
	}
	d.Direction = direction

	// Stabilization: act only on what the whole window supports, and only once the
	// window has been observed.
	window := time.Duration(s.ScaleUpStabilizationS) * time.Second
	if direction == Down {
		window = time.Duration(s.ScaleDownStabilizationS) * time.Second
	}
	stable := h.stabilized(in.Now, window, direction)
	unsupported := (direction == Up && stable <= current) || (direction == Down && stable >= current)
	if in.Now.Sub(h.firstSeen) < window || unsupported {
		d.Decision = SuppressedStabilization
		d.Reason = fmt.Sprintf("%s, but the %s stabilization window (%s) does not support a change yet",
			why, direction, window)
		return h.finish(d)
	}

	// Maximum step.
	target := stable
	if direction == Up {
		if limit := maxInt32(base*2, base+1); target > limit {
			target = limit
			why += fmt.Sprintf(", limited to a step of %d", limit-current)
		}
	} else {
		step := int32(math.Floor(float64(current) * 0.1))
		if step < 1 {
			step = 1
		}
		if limit := current - step; target < limit {
			target = limit
			why += fmt.Sprintf(", limited to a step of %d", step)
		}
	}

	// Bounds.
	switch {
	case target > in.Max:
		target = in.Max
		if target <= current {
			d.Decision = ClampedMax
			d.Reason = fmt.Sprintf("%s, but the deployment is at its maximum of %d", why, in.Max)
			return h.finish(d)
		}
		why += fmt.Sprintf(", clamped to the maximum of %d", in.Max)
	case target < in.Min:
		target = in.Min
		if target >= current {
			d.Decision = ClampedMin
			d.Reason = fmt.Sprintf("%s, but the deployment is at its minimum of %d", why, in.Min)
			return h.finish(d)
		}
		why += fmt.Sprintf(", clamped to the minimum of %d", in.Min)
	}

	if in.RolloutActive {
		d.Decision = SuppressedRollout
		d.Reason = why + ", but a rollout is in progress: replicas do not move under a canary's analysis"
		return h.finish(d)
	}
	if cool := time.Duration(s.CooldownS) * time.Second; !h.lastChange.IsZero() && in.Now.Sub(h.lastChange) < cool {
		d.Decision = SuppressedCooldown
		d.Reason = fmt.Sprintf("%s, but the last change was %s ago (cooldown %s)", why,
			in.Now.Sub(h.lastChange).Round(time.Second), cool)
		return h.finish(d)
	}

	d.To, d.Decision = target, Scaled
	d.Reason = fmt.Sprintf("%s: %d -> %d", why, current, target)
	h.lastChange = in.Now
	return h.finish(d)
}

// finish decides whether the decision is worth a row: every change, and a
// suppression only when it differs from the last one recorded, so a decision held
// for an hour is one row, not 240. Steady state and "disabled" write nothing.
func (h *History) finish(d Decision) Decision {
	switch {
	case d.Decision == Disabled || (d.Decision == Scaled && d.To == d.From):
		h.last = ""
	case d.Changed():
		d.Record, h.last = true, ""
	default:
		key := fmt.Sprintf("%s/%s/%d", d.Decision, d.Direction, d.To)
		d.Record = key != h.last
		h.last = key
	}
	return d
}

// remember adds a recommendation and forgets those no window can use.
func (h *History) remember(now time.Time, replicas int32, s Settings) {
	h.recs = append(h.recs, recommendation{at: now, replicas: replicas})
	keep := time.Duration(maxInt(s.ScaleUpStabilizationS, s.ScaleDownStabilizationS)) * time.Second
	i := 0
	for i < len(h.recs) && now.Sub(h.recs[i].at) > keep {
		i++
	}
	h.recs = h.recs[i:]
}

// stabilized is the replica count every recommendation in the window supports:
// the smallest for a scale-up, the largest for a scale-down.
func (h *History) stabilized(now time.Time, window time.Duration, direction string) int32 {
	var out int32
	first := true
	for _, r := range h.recs {
		if now.Sub(r.at) > window {
			continue
		}
		switch {
		case first:
			out, first = r.replicas, false
		case direction == Up && r.replicas < out:
			out = r.replicas
		case direction == Down && r.replicas > out:
			out = r.replicas
		}
	}
	return out
}

func round(v float64) float64 { return math.Round(v*100) / 100 }

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
