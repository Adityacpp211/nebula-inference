#!/usr/bin/env python3
"""Generate NEBULA's Grafana dashboards (docs/observability.md §6).

The JSON under deploy/helm/nebula/files/dashboards/ is committed and provisioned by
the chart (Helm can only package files inside the chart);
this script is how it is written, so every panel's query lives in one reviewable
place and `make dashboards` regenerates them byte-for-byte. tests/e2e/kind/
phase8_demo.py evaluates every query here against a live Prometheus, so a shipped
dashboard cannot contain a broken query or a panel with no series behind it.

Dashboards whose series do not exist yet are not shipped (Rollouts & Experiments
with Phase 12, Cost with Phase 13, Nodes & GPU with a GPU exporter): a dashboard of
empty panels is the "fake metrics" failure in another form.

Run: python deploy/grafana/generate.py
"""

from __future__ import annotations

import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "deploy" / "helm" / "nebula" / "files" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}

# Every query, by dashboard, so the demo can check them without parsing JSON.
QUERIES: dict[str, list[tuple[str, str]]] = {}


def panel(title: str, expr: str, *, unit: str = "short", kind: str = "timeseries",
          legend: str = "", w: int = 12, h: int = 8, exemplars: bool = False) -> dict:
    return {
        "type": kind,
        "title": title,
        "datasource": PROM,
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}} if kind == "timeseries" else {},
        "targets": [{"refId": "A", "datasource": PROM, "expr": expr, "legendFormat": legend,
                     "exemplar": exemplars}],
        "gridPos": {"w": w, "h": h},
    }


def dashboard(uid: str, title: str, panels: list[dict], *, deployment_var: bool = False) -> dict:
    x = y = 0
    for i, p in enumerate(panels):
        p["id"] = i + 1
        w, h = p["gridPos"]["w"], p["gridPos"]["h"]
        if x + w > 24:
            x, y = 0, y + h
        p["gridPos"].update({"x": x, "y": y})
        x += w
    QUERIES[uid] = [(p["title"], p["targets"][0]["expr"]) for p in panels]
    templating = []
    if deployment_var:
        templating.append({
            "name": "deployment", "type": "query", "datasource": PROM,
            "query": {"query": "label_values(nebula_requests_total, deployment)", "refId": "v"},
            "definition": "label_values(nebula_requests_total, deployment)",
            "includeAll": True, "multi": False, "allValue": ".*", "refresh": 2,
            "current": {"selected": True, "text": "All", "value": "$__all"},
        })
    return {
        "uid": uid, "title": title, "tags": ["nebula"], "schemaVersion": 39, "version": 1,
        "editable": False, "time": {"from": "now-1h", "to": "now"}, "refresh": "30s",
        "templating": {"list": templating}, "panels": panels,
    }


D = 'deployment=~"$deployment"'

fleet = dashboard("nebula-fleet", "NEBULA / Fleet Overview", [
    panel("Request rate", 'sum by (route) (rate(nebula_requests_total[5m]))', unit="reqps", legend="{{route}}"),
    panel("Error rate (5xx, excluding client cancellations)",
          # With traffic and no 5xx the ratio is a true zero; with no traffic at all
          # the denominator is absent and the panel says "no data".
          '(sum(rate(nebula_requests_total{status_class="5xx"}[5m])) or vector(0))'
          ' / sum(rate(nebula_requests_total[5m]))',
          unit="percentunit"),
    panel("p95 time to first token",
          'histogram_quantile(0.95, sum by (le, deployment) (rate(nebula_ttft_seconds_bucket[5m])))',
          unit="s", legend="{{deployment}}", exemplars=True),
    panel("Deployments by state", 'sum by (state) (nebula_deployment_state)', legend="{{state}}"),
    panel("Ready endpoints", 'sum by (deployment) (nebula_endpoints{state="ready"})', legend="{{deployment}}"),
    panel("Requests shed by the admission queue", 'sum by (reason) (rate(nebula_queue_drops_total[5m]))',
          unit="reqps", legend="{{reason}}"),
])

detail = dashboard("nebula-deployment", "NEBULA / Deployment Detail", [
    panel("Requests by status", f'sum by (status_class) (rate(nebula_requests_total{{{D}}}[5m]))',
          unit="reqps", legend="{{status_class}}"),
    panel("Request duration p95",
          f'histogram_quantile(0.95, sum by (le) (rate(nebula_request_duration_seconds_bucket{{{D}}}[5m])))',
          unit="s", legend="p95", exemplars=True),
    panel("Time to first token p95",
          f'histogram_quantile(0.95, sum by (le) (rate(nebula_ttft_seconds_bucket{{{D}}}[5m])))',
          unit="s", legend="p95", exemplars=True),
    panel("Tokens per second", f'sum by (direction) (rate(nebula_tokens_total{{{D}}}[5m]))',
          legend="{{direction}}"),
    panel("Endpoints by state", f'sum by (state) (nebula_endpoints{{{D}}})', legend="{{state}}"),
    panel("Replicas desired / ready", f'nebula_deployment_replicas{{{D}}}', legend="{{state}}"),
    panel("In flight", f'sum(nebula_inflight_requests{{{D}}})'),
    panel("Generation lag", f'max(nebula_generation_lag{{{D}}})'),
], deployment_var=True)

path = dashboard("nebula-request-path", "NEBULA / Request Path", [
    panel("Gateway queue wait p95",
          'histogram_quantile(0.95, sum by (le, deployment) (rate(nebula_queue_wait_seconds_bucket[5m])))',
          unit="s", legend="{{deployment}}", exemplars=True),
    panel("Time to first token p95 (worker-measured)",
          'histogram_quantile(0.95, sum by (le, deployment) (rate(nebula_ttft_seconds_bucket[5m])))',
          unit="s", legend="{{deployment}}", exemplars=True),
    panel("Total duration p95",
          'histogram_quantile(0.95, sum by (le, deployment) (rate(nebula_request_duration_seconds_bucket[5m])))',
          unit="s", legend="{{deployment}}", exemplars=True),
    panel("Dispatch attempts by outcome", 'sum by (outcome) (rate(nebula_request_attempts_total[5m]))',
          unit="reqps", legend="{{outcome}}"),
    panel("Routing decisions", 'sum by (outcome) (rate(nebula_route_decisions_total[5m]))',
          unit="reqps", legend="{{outcome}}"),
    panel("Open or half-open breakers", 'sum by (deployment) (nebula_breaker_state > 0) or vector(0)',
          legend="{{deployment}}"),
    panel("Heartbeat age at selection p95",
          'histogram_quantile(0.95, sum by (le) (rate(nebula_endpoint_staleness_seconds_bucket[5m])))', unit="s"),
    panel("Rate-limit refusals", 'sum by (scope, limit) (rate(nebula_ratelimit_rejections_total[5m])) or vector(0)',
          unit="reqps", legend="{{scope}} {{limit}}"),
])

queue = dashboard("nebula-queue", "NEBULA / Queue & Autoscaling", [
    panel("Queue depth", 'sum by (deployment, priority) (nebula_queue_depth)', legend="{{deployment}} {{priority}}"),
    panel("Oldest waiting request", 'max by (deployment) (nebula_queue_oldest_age_seconds)', unit="s",
          legend="{{deployment}}"),
    panel("Queue wait p95 vs 500 ms target",
          'histogram_quantile(0.95, sum by (le, deployment) (rate(nebula_queue_wait_seconds_bucket[5m])))',
          unit="s", legend="{{deployment}}", exemplars=True),
    panel("Drops by reason", 'sum by (deployment, reason) (rate(nebula_queue_drops_total[5m])) or vector(0)',
          unit="reqps", legend="{{deployment}} {{reason}}"),
    panel("Worker queue depth", 'sum by (pod) (nebula_worker_queue_depth)', legend="{{pod}}"),
    panel("Worker slots busy / total", 'sum(nebula_worker_slots_busy) / clamp_min(sum(nebula_worker_slots_total), 1)',
          unit="percentunit"),
])

if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for d in (fleet, detail, path, queue):
        (OUT / f"{d['uid']}.json").write_text(json.dumps(d, indent=2) + "\n", encoding="utf-8", newline="\n")
    (OUT.parent / "dashboard-queries.json").write_text(json.dumps(QUERIES, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {len(QUERIES)} dashboards, {sum(len(v) for v in QUERIES.values())} panels")
