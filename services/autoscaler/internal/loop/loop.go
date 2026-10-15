// Package loop is the autoscaler's control loop: on every tick, read the signals
// once, decide every autoscaled deployment, and record what is worth a row.
//
// The decision itself is packages/autoscale, which is pure; this package owns the
// per-deployment History, the clock, the store and the metrics around it.
package loop

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/autoscale"
	"github.com/adityasatwar321/nebula/packages/telemetry"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/signals"
	"github.com/adityasatwar321/nebula/services/autoscaler/internal/store"
)

// Store is what the loop needs from PostgreSQL.
type Store interface {
	Targets(ctx context.Context) ([]store.Target, error)
	Record(ctx context.Context, t store.Target, d autoscale.Decision) error
}

// Reader reads one pass of signals.
type Reader interface {
	Read(ctx context.Context) signals.Reading
}

// Loop decides every autoscaled deployment on each Tick.
type Loop struct {
	Store   Store
	Signals Reader
	Metrics *telemetry.Metrics
	Logger  *slog.Logger
	Now     func() time.Time

	histories map[uuid.UUID]*tracked
	signalErr bool
}

type tracked struct {
	history autoscale.History
	name    string
	signals []string
}

// Tick makes one decision per deployment. An error means the deployments could
// not be listed, and nothing was decided.
func (l *Loop) Tick(ctx context.Context) error {
	if l.histories == nil {
		l.histories = map[uuid.UUID]*tracked{}
	}
	now := time.Now
	if l.Now != nil {
		now = l.Now
	}
	targets, err := l.Store.Targets(ctx)
	if err != nil {
		return err
	}
	reading := l.Signals.Read(ctx)
	if err := ctx.Err(); err != nil {
		// Shutting down: a query cut short is not a missing signal, and a
		// suppressed_no_signal row for it would be a false record.
		return err
	}
	switch {
	case reading.Err != nil && !l.signalErr:
		// Logged on the transition, not every tick; each deployment's
		// suppressed_no_signal row is the durable record.
		l.Logger.WarnContext(ctx, "signals unavailable: holding every deployment",
			slog.String("cause", reading.Err.Error()))
	case reading.Err == nil && l.signalErr:
		l.Logger.InfoContext(ctx, "signals available again")
	}
	l.signalErr = reading.Err != nil

	at := now()
	seen := make(map[uuid.UUID]bool, len(targets))
	for _, t := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		seen[t.ID] = true
		tr := l.histories[t.ID]
		if tr == nil {
			tr = &tracked{name: t.Name}
			l.histories[t.ID] = tr
		}
		l.decide(ctx, at, t, tr, reading)
	}
	// A deployment that is gone, stopped or no longer autoscaled starts afresh if it
	// returns: a stale window would let it act on load it never saw.
	for id, tr := range l.histories {
		if !seen[id] {
			l.forget(tr)
			delete(l.histories, id)
		}
	}
	return nil
}

func (l *Loop) decide(ctx context.Context, at time.Time, t store.Target, tr *tracked, reading signals.Reading) {
	log := l.Logger.With(slog.String("deployment_id", t.ID.String()), slog.String("org", t.OrgSlug),
		slog.String("deployment", t.Name))
	settings, err := autoscale.ParseSettings(t.Autoscaling)
	if err != nil {
		// The control plane validates the object on write, so this is a row written
		// around it. Holding is the safe reading of a configuration nobody can parse.
		log.WarnContext(ctx, "autoscaling settings are invalid: holding", slog.String("cause", err.Error()))
		return
	}
	d := tr.history.Decide(autoscale.Input{
		Now: at, Current: t.Desired, Min: t.Min, Max: t.Max, Settings: settings,
		Signals:       signals.For(reading, t.ID, t.Desired, settings),
		RolloutActive: t.RolloutActive,
	})
	l.observe(t, tr, d)
	if !d.Record {
		return
	}
	attrs := []any{slog.Int("from", int(d.From)), slog.Int("to", int(d.To)), slog.String("direction", d.Direction),
		slog.String("decision", d.Decision), slog.String("reason", d.Reason)}
	switch err := l.Store.Record(ctx, t, d); {
	case errors.Is(err, store.ErrConflict):
		log.InfoContext(ctx, "deployment changed under the decision; deciding again next tick", attrs...)
	case err != nil:
		log.ErrorContext(ctx, "recording the decision failed", append(attrs, slog.String("cause", err.Error()))...)
	case d.Changed():
		log.InfoContext(ctx, "scaled", attrs...)
	default:
		log.InfoContext(ctx, "decision recorded", attrs...)
	}
}

// observe exports the decision. A signal that is missing has no ratio series at
// all, rather than a zero that would read as "idle".
func (l *Loop) observe(t store.Target, tr *tracked, d autoscale.Decision) {
	if l.Metrics == nil {
		return
	}
	l.Metrics.Counter("nebula_autoscale_decisions_total").WithLabelValues(t.Name, d.Direction, d.Decision).Inc()
	ratio := l.Metrics.Gauge("nebula_autoscale_signal_ratio")
	tr.name = t.Name
	tr.signals = tr.signals[:0]
	for _, s := range d.Signals {
		if s.Stale {
			ratio.DeleteLabelValues(t.Name, s.Name)
			continue
		}
		ratio.WithLabelValues(t.Name, s.Name).Set(s.Ratio)
		tr.signals = append(tr.signals, s.Name)
	}
}

func (l *Loop) forget(tr *tracked) {
	if l.Metrics == nil {
		return
	}
	ratio := l.Metrics.Gauge("nebula_autoscale_signal_ratio")
	for _, s := range tr.signals {
		ratio.DeleteLabelValues(tr.name, s)
	}
}

// Run ticks until ctx ends, while lead reports this replica holds leadership.
func (l *Loop) Run(ctx context.Context, every time.Duration, lead func(context.Context) bool) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if lead(ctx) {
			if err := l.Tick(ctx); err != nil && ctx.Err() == nil {
				// No list, no decisions: holding is what a missing input means.
				l.Logger.WarnContext(ctx, "cannot list deployments: no decisions this tick",
					slog.String("cause", err.Error()))
			}
		} else if len(l.histories) > 0 {
			// A standby keeps no memory: when it leads, it observes its windows first.
			for id, tr := range l.histories {
				l.forget(tr)
				delete(l.histories, id)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
