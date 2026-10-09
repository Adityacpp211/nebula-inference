package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Tracing (docs/observability.md §3). Service code uses the OpenTelemetry API
// through Tracer and StartSpan; the SDK and the OTLP exporter are configured here
// and nowhere else, so the Collector stays the swap point (ADR-0020).
//
// Without an endpoint configured the global provider stays OpenTelemetry's no-op:
// spans cost nothing and the W3C trace context still flows, so correlation by
// trace_id in logs keeps working with no tracing backend at all.

// TracingOptions configure export.
type TracingOptions struct {
	// Endpoint is the Collector's OTLP/HTTP address, host:port. Empty disables
	// export.
	Endpoint string
	// SampleRatio is the head-sampling ratio for traces that start here; a sampled
	// parent is always followed. The Collector's tail sampling keeps the
	// interesting unsampled ones (errors, slow, retried).
	SampleRatio float64
	Service     string
	Version     string
	Instance    string
	// Exporter replaces the OTLP exporter, for tests.
	Exporter sdktrace.SpanExporter
}

// SetupTracing installs the global tracer provider. The returned function
// flushes and stops it; call it on shutdown.
func SetupTracing(ctx context.Context, o TracingOptions) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	exp := o.Exporter
	if exp == nil {
		if o.Endpoint == "" {
			return func(context.Context) error { return nil }, nil
		}
		var err error
		exp, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(o.Endpoint), otlptracehttp.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("tracing exporter: %w", err)
		}
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", o.Service),
		attribute.String("service.version", o.Version),
		attribute.String("service.instance.id", o.Instance),
	)
	ratio := o.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		// Bounded and asynchronous: telemetry never blocks the request path, and a
		// full queue drops spans rather than memory growing (§1, property 3).
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(4096), sdktrace.WithBatchTimeout(2*time.Second)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the named tracer from the global provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// StartRequestSpan starts a service's root server span, continuing the incoming
// W3C context when there is one, and returns the trace context NEBULA's logs and
// outgoing headers use: the span's own ids when tracing is on, the incoming
// context unchanged when it is off.
func StartRequestSpan(ctx context.Context, name string, parent TraceContext) (context.Context, trace.Span, TraceContext) {
	if parent.Valid() {
		if sc, ok := spanContextOf(parent); ok {
			ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		}
	}
	ctx, span := Tracer("nebula").Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer))
	tc := parent
	if sc := span.SpanContext(); sc.IsValid() && span.IsRecording() {
		tc = TraceContext{TraceID: sc.TraceID().String(), SpanID: sc.SpanID().String(),
			Sampled: sc.IsSampled(), TraceState: parent.TraceState}
	}
	return ctx, span, tc
}

// Traceparent renders the traceparent for an outgoing call made inside ctx's
// current span: the span's own id when it is recording, else a fresh child of
// fallback, so a worker always gets a valid parent.
func Traceparent(ctx context.Context, fallback TraceContext) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && trace.SpanFromContext(ctx).IsRecording() {
		carrier := propagation.MapCarrier{}
		propagation.TraceContext{}.Inject(ctx, carrier)
		return carrier.Get(HeaderTraceparent)
	}
	if child, err := fallback.Child(); err == nil {
		return child.Header()
	}
	return ""
}

func spanContextOf(tc TraceContext) (trace.SpanContext, bool) {
	tid, err := trace.TraceIDFromHex(tc.TraceID)
	if err != nil {
		return trace.SpanContext{}, false
	}
	sid, err := trace.SpanIDFromHex(tc.SpanID)
	if err != nil {
		return trace.SpanContext{}, false
	}
	var flags trace.TraceFlags
	if tc.Sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true}), true
}
