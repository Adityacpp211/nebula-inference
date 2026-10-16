// Package rolloutctrl advances canary rollouts (docs/architecture.md §7.3): it
// shifts a route's weights step by step, analyses both arms after each step's
// hold, and on a breach puts every request back on the baseline, with the reason
// on the row.
//
// The decision is packages/rollout; this package owns the clock, the Prometheus
// reads and the writes. Every write is a compare-and-set on the state and step
// the rollout was read in, so an operator's pause, abort or promote through the
// control plane always wins over a tick in flight.
package rolloutctrl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/promquery"
	"github.com/adityasatwar321/nebula/packages/rollout"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Store is what the reconciler needs from PostgreSQL.
type Store interface {
	ActiveRollouts(ctx context.Context) ([]store.Rollout, error)
	ApplyRollout(ctx context.Context, r store.Rollout, ch store.RolloutChange) error
}

// Analyzer observes both arms over a window.
type Analyzer interface {
	Arms(ctx context.Context, baseline, canary uuid.UUID, window time.Duration) (base, can rollout.Arm, err error)
}

// Reconciler advances every active rollout on each Tick.
type Reconciler struct {
	Store    Store
	Analyzer Analyzer
	Logger   *slog.Logger
	Now      func() time.Time
	// Metrics, when set, receives nebula_rollout_step and nebula_rollout_state.
	Metrics *telemetry.Metrics

	exported map[uuid.UUID][3]string // route, rollout, state of each exported series
}

// analysisRecord is canary_rollouts.last_analysis: the verdict, and which step
// and window it was for, so a window is analysed once.
type analysisRecord struct {
	Step   int32 `json:"step"`
	Window int   `json:"window"`
	rollout.Verdict
}

// Tick advances every active rollout once. An error means none could be listed.
func (r *Reconciler) Tick(ctx context.Context) error {
	rs, err := r.Store.ActiveRollouts(ctx)
	if err != nil {
		return err
	}
	r.export(rs)
	for _, ro := range rs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := r.advance(ctx, ro); err != nil {
			log := r.Logger.With(slog.String("rollout_id", ro.ID.String()), slog.String("route", ro.Route))
			if errors.Is(err, store.ErrRolloutChanged) {
				log.InfoContext(ctx, "rollout changed under the tick; reading it again next time")
				continue
			}
			log.ErrorContext(ctx, "advancing the rollout failed", slog.String("cause", err.Error()))
		}
	}
	return nil
}

// export publishes where each unfinished rollout stands, as read at the start of
// the tick, and removes the series of rollouts that have finished.
func (r *Reconciler) export(rs []store.Rollout) {
	if r.Metrics == nil {
		return
	}
	step, state := r.Metrics.Gauge("nebula_rollout_step"), r.Metrics.Gauge("nebula_rollout_state")
	seen := map[uuid.UUID][3]string{}
	for _, ro := range rs {
		labels := [3]string{ro.Route, ro.ID.String(), string(ro.State)}
		seen[ro.ID] = labels
		if prev, ok := r.exported[ro.ID]; ok && prev[2] != labels[2] {
			state.DeleteLabelValues(prev[:]...)
		}
		weight := 0
		if steps, err := rollout.Steps(ro.Steps); err == nil && ro.State != models.RolloutPending &&
			int(ro.CurrentStep) < len(steps) {
			weight = steps[ro.CurrentStep]
		}
		step.WithLabelValues(labels[0], labels[1]).Set(float64(weight))
		state.WithLabelValues(labels[:]...).Set(1)
	}
	for id, labels := range r.exported {
		if _, ok := seen[id]; !ok {
			step.DeleteLabelValues(labels[0], labels[1])
			state.DeleteLabelValues(labels[:]...)
		}
	}
	r.exported = seen
}

func (r *Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Reconciler) advance(ctx context.Context, ro store.Rollout) error {
	steps, err := rollout.Steps(ro.Steps)
	if err == nil && int(ro.CurrentStep) >= len(steps) {
		err = fmt.Errorf("current step %d is past the last of %d steps", ro.CurrentStep, len(steps))
	}
	var analysis rollout.Analysis
	if err == nil {
		analysis, err = rollout.ParseAnalysis(ro.Analysis)
	}
	if err != nil {
		// The control plane validates both on create, so this is a row written
		// around it. Failing restores the baseline, which is always safe.
		return r.end(ctx, ro, models.RolloutFailed, "invalid rollout configuration: "+err.Error(), nil, "rollout.failed")
	}

	switch ro.State {
	case models.RolloutPending:
		first := steps[0]
		if first == 100 {
			return r.promote(ctx, ro, 0, nil)
		}
		r.log(ctx, ro, "rollout started", slog.Int("canary_weight", first))
		return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: models.RolloutProgressing, Step: 0,
			CanaryWeight: first, Start: true, EnterStep: true, Action: "rollout.started"})

	case models.RolloutProgressing:
		return r.analyse(ctx, ro, steps, analysis)
	}
	// Paused: an operator resumes, promotes or aborts it through the control plane.
	return nil
}

func (r *Reconciler) analyse(ctx context.Context, ro store.Rollout, steps []int, a rollout.Analysis) error {
	if ro.StepEnteredAt == nil {
		return nil
	}
	hold := time.Duration(a.HoldS) * time.Second
	elapsed := r.now().Sub(*ro.StepEnteredAt)
	window := int(elapsed / hold) // completed hold windows at this step
	if window < 1 || analysedAlready(ro, window) {
		return nil
	}

	base, canary, err := r.Analyzer.Arms(ctx, ro.BaselineID, ro.CanaryID, elapsed)
	var v rollout.Verdict
	if err != nil {
		// Blind is never healthy: a step that cannot be measured does not pass.
		v = rollout.Verdict{Outcome: rollout.Insufficient, Reason: "metrics unavailable: " + err.Error(),
			Baseline: rollout.Arm{P95MS: math.NaN()}, Canary: rollout.Arm{P95MS: math.NaN()}}
	} else {
		v = rollout.Evaluate(a, base, canary)
	}
	record, _ := json.Marshal(analysisRecord{Step: ro.CurrentStep, Window: window, Verdict: v})

	switch v.Outcome {
	case rollout.Pass:
		next := ro.CurrentStep + 1
		if steps[next] == 100 {
			return r.promote(ctx, ro, next, record)
		}
		r.log(ctx, ro, "step passed; advancing", slog.Int("canary_weight", steps[next]), slog.String("analysis", v.Reason))
		return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: models.RolloutProgressing, Step: next,
			CanaryWeight: steps[next], EnterStep: true, LastAnalysis: record, Action: "rollout.advanced"})

	case rollout.Breach:
		return r.end(ctx, ro, models.RolloutRolledBack, v.Reason, record, "rollout.rolled_back")
	}

	// Insufficient data.
	extensions := window - 1
	choice := a.OnInsufficientData
	if choice == rollout.Extend && extensions >= a.MaxExtensions {
		choice = rollout.Pause
	}
	switch choice {
	case rollout.Fail:
		return r.end(ctx, ro, models.RolloutFailed, "insufficient data: "+v.Reason, record, "rollout.failed")
	case rollout.Pause:
		r.log(ctx, ro, "rollout paused: insufficient data", slog.String("analysis", v.Reason))
		return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: models.RolloutPaused, Step: ro.CurrentStep,
			CanaryWeight: -1, LastAnalysis: record, Action: "rollout.paused"})
	}
	r.log(ctx, ro, "insufficient data; extending the window", slog.Int("window", window), slog.String("analysis", v.Reason))
	return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: models.RolloutProgressing, Step: ro.CurrentStep,
		CanaryWeight: -1, LastAnalysis: record})
}

func (r *Reconciler) promote(ctx context.Context, ro store.Rollout, step int32, record json.RawMessage) error {
	r.log(ctx, ro, "rollout promoted: the canary serves every request")
	return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: models.RolloutPromoted, Step: step,
		CanaryWeight: 100, Promote: true, Finish: true, LastAnalysis: record, Action: "rollout.promoted"})
}

// end finishes a rollout that did not promote: every request back on the baseline.
func (r *Reconciler) end(ctx context.Context, ro store.Rollout, state models.RolloutState, reason string,
	record json.RawMessage, action string) error {
	r.Logger.WarnContext(ctx, "rollout ended; traffic restored to the baseline",
		slog.String("rollout_id", ro.ID.String()), slog.String("route", ro.Route), slog.String("state", string(state)),
		slog.String("reason", reason))
	return r.Store.ApplyRollout(ctx, ro, store.RolloutChange{State: state, Step: ro.CurrentStep, CanaryWeight: 0,
		Finish: true, AbortReason: reason, LastAnalysis: record, Action: action})
}

// analysedAlready reports whether last_analysis is this step's window.
func analysedAlready(ro store.Rollout, window int) bool {
	var last analysisRecord
	if len(ro.LastAnalysis) == 0 || json.Unmarshal(ro.LastAnalysis, &last) != nil {
		return false
	}
	return last.Step == ro.CurrentStep && last.Window >= window
}

func (r *Reconciler) log(ctx context.Context, ro store.Rollout, msg string, attrs ...any) {
	r.Logger.InfoContext(ctx, msg, append([]any{slog.String("rollout_id", ro.ID.String()),
		slog.String("route", ro.Route), slog.String("canary", ro.Canary), slog.Int("step", int(ro.CurrentStep))}, attrs...)...)
}

// Prometheus measures arms from the gateway's request metrics, which carry
// deployment_id (docs/observability.md §2.1).
type Prometheus struct {
	Client *promquery.Client
}

// Queries are exported for the documentation and the tests.
func (p *Prometheus) Queries(baseline, canary uuid.UUID, window time.Duration) map[string]string {
	w := promquery.Duration(window, 30*time.Second)
	sel := fmt.Sprintf(`deployment_id=~"%s|%s"`, baseline, canary)
	return map[string]string{
		"requests": `sum by (deployment_id) (increase(nebula_requests_total{` + sel + `}[` + w + `]))`,
		// A failure the client saw: a 5xx, or a stream that broke after its 200
		// (recorded as 2xx with an upstream, internal or timeout error class). The
		// two selectors are disjoint series, unioned before the sum.
		"errors": `sum by (deployment_id) (increase(nebula_requests_total{` + sel + `, status_class="5xx"}[` + w + `])` +
			` or increase(nebula_requests_total{` + sel + `, status_class!="5xx", error_class=~"upstream_error|internal_error|timeout_error"}[` + w + `]))`,
		"p95_ms": `1000 * histogram_quantile(0.95, sum by (deployment_id, le) (rate(nebula_request_duration_seconds_bucket{` +
			sel + `}[` + w + `])))`,
	}
}

// Arms reads both arms. A failed query fails the reading; an arm absent from a
// result served nothing, and its p95 is unknown.
func (p *Prometheus) Arms(ctx context.Context, baseline, canary uuid.UUID, window time.Duration) (base, can rollout.Arm, err error) {
	q := p.Queries(baseline, canary, window)
	got := map[string]map[string]float64{}
	for _, name := range []string{"requests", "errors", "p95_ms"} {
		m, err := p.Client.Vector(ctx, q[name], "deployment_id")
		if err != nil {
			return rollout.Arm{}, rollout.Arm{}, fmt.Errorf("%s: %w", name, err)
		}
		got[name] = m
	}
	arm := func(id uuid.UUID) rollout.Arm {
		k := strings.ToLower(id.String())
		a := rollout.Arm{Requests: got["requests"][k], Errors: got["errors"][k], P95MS: math.NaN()}
		if v, ok := got["p95_ms"][k]; ok {
			a.P95MS = v
		}
		return a
	}
	return arm(baseline), arm(canary), nil
}
