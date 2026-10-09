# 35. Observability wiring: a declared catalogue, OTLP/HTTP, a separate metrics port

Date: 2026-10-09

## Status

Accepted. Implemented in Phase 8 (`packages/telemetry`, `services/*/main.go`,
`workers/inference/nebula_worker/tracing.py`, `deploy/helm/nebula/templates/observability`).

## Context

docs/observability.md is the contract: the metric catalogue with its labels, the span tree,
the log schema, the dashboards and alerts, and the operator's path from a panel to a trace to
log lines. Building it raised five choices the document did not make.

## Decision

1. **Metrics are declared once, in code.** `telemetry.Catalog` lists every Go metric with its
   type, labels and buckets; a service asks for a metric by name and gets the declared vector,
   and an undeclared name panics at startup. A test parses the tables of observability.md and
   compares them with the catalogue in both directions, so a rename on either side fails CI
   instead of silently emptying a dashboard. Rows for metrics that arrive later are marked
   *(Phase N)* in the document and are allowed to be absent.
2. **/metrics on its own port** (`NEBULA_METRICS_ADDR`, `:9090` in the chart), on every service.
   The gateway's main port is the public edge; metrics are for in-cluster scraping, and a
   separate listener keeps them off it by construction rather than by a path filter.
3. **OTLP over HTTP (4318)**, not gRPC. The HTTP exporter is a smaller dependency in both Go and
   Python, and the Collector speaks both, so it remains the swap point (ADR-0020). Without an
   endpoint the OpenTelemetry API's no-op provider makes spans free while the W3C context still
   flows and logs keep their `trace_id`.
4. **The gateway's root span adopts the request's trace context**, and NEBULA's log correlation
   reads the span's ids; the worker continues the `dispatch.attempt` span's context. One trace
   therefore covers gateway and worker, and the `trace_id` in every log line is the trace's.
   Histograms carry that trace id as an exemplar.
5. **Prometheus discovers pods by annotation** (`prometheus.io/scrape`), not by the Prometheus
   Operator's ServiceMonitors: kind has no operator, and annotations are what both setups can
   read. A cluster with the operator adds ServiceMonitors for the same ports. Worker metrics get
   their `deployment` label from the pod label at scrape time, not from the worker
   (observability.md §2.3).

## Consequences

- The development chart runs Collector, Tempo, Loki, Promtail, Prometheus and Grafana in
  `nebula-observability`, single-replica with emptyDir storage. Production points the services
  at its own backends with `telemetry.otlpEndpoint` and its own scraping; that is a values
  change.
- Dashboards are generated (`deploy/grafana/generate.py`) into the chart, which is the only place
  Helm can package them from. CI regenerates them and fails on any difference, and the Phase 8
  kind demo evaluates every panel's query against the live Prometheus.
- Dashboards whose series do not exist yet — Rollouts & Experiments, Cost, Nodes & GPU — are not
  shipped; they arrive with their metrics.
- Tail sampling and Alertmanager routing are not configured in the development stack: sampling is
  100% there, and alerts are evaluated and visible in Prometheus.
