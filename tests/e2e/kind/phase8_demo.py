#!/usr/bin/env python3
"""Phase 8 demo: one request followed from a metric to its trace to its logs.

Run after `make dev-up` (the dev values install the observability stack). Uses the
standard library and kubectl. The walk is the operator's path of
docs/observability.md §5:

  1. the stack is up: Prometheus scrapes every NEBULA service and worker, Grafana
     has its datasources and the committed dashboards;
  2. traffic, including one request with a known trace id and request id, and a
     burst that makes requests queue;
  3. every dashboard panel's query returns real series (no broken query, no panel
     with nothing behind it);
  4. a histogram exemplar names a trace id, and that trace is in Tempo;
  5. the known request's trace spans the gateway and the worker — gateway.request,
     router.select, dispatch.attempt, worker.generate, runtime.stream — in one trace;
  6. Loki holds the gateway's and the worker's log lines for its request_id, each
     carrying the same trace_id, and no prompt text;
  7. cleanup.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
    GATEWAY,
    api,
    deploy,
    fail,
    find_model,
    kubectl,
    ok,
    put_route,
    ready_version,
    register,
    remove_deployment,
    remove_route,
    seeded_key,
    status,
    step,
    wait,
)

OBS = "nebula-observability"
ROUTE, DEP = "obs-demo", "obs-demo"
GRAFANA = "http://127.0.0.1:3000"
SECRET = "the vault code is 7741-obs"
QUERIES = os.path.join(os.path.dirname(__file__), "..", "..", "..", "deploy", "helm", "nebula", "files",
                       "dashboard-queries.json")


def forward(svc: str, port: int) -> tuple[subprocess.Popen, str]:
    """kubectl port-forward on a free local port; returns the process and base URL."""
    p = subprocess.Popen(["kubectl", "-n", OBS, "port-forward", f"svc/{svc}", f"0:{port}"],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert p.stdout is not None
    deadline = time.time() + 30
    while time.time() < deadline:
        line = p.stdout.readline()
        m = re.search(r"127\.0\.0\.1:(\d+)", line)
        if m:
            return p, f"http://127.0.0.1:{m.group(1)}"
    p.kill()
    fail(f"port-forward to {svc} did not start")
    raise SystemExit(1)


def get(url: str, auth: str | None = None) -> dict:
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def prom(base: str, query: str) -> list:
    out = get(f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": query}))
    if out.get("status") != "success":
        fail(f"query failed: {query}: {out}")
    return out["data"]["result"]


def chat(key: str, content: str = "hi", max_tokens: int = 4, headers: dict | None = None,
         reject: bool = False) -> int:
    body = {"model": ROUTE, "messages": [{"role": "user", "content": content}], "max_tokens": max_tokens}
    if reject:
        body["nebula"] = {"queue": "reject"}
    req = urllib.request.Request(GATEWAY + "/v1/chat/completions", method="POST", data=json.dumps(body).encode())
    req.add_header("Authorization", f"Bearer {key}")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            r.read()
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    forwards: list[subprocess.Popen] = []
    try:
        step("the observability stack is up")
        for d in ("otel-collector", "tempo", "loki", "prometheus", "grafana"):
            kubectl("-n", OBS, "rollout", "status", f"deploy/{d}", "--timeout=300s")
        kubectl("-n", OBS, "rollout", "status", "ds/promtail", "--timeout=300s")
        p, PROM = forward("prometheus", 9090)
        forwards.append(p)
        p, TEMPO = forward("tempo", 3200)
        forwards.append(p)
        p, LOKI = forward("loki", 3100)
        forwards.append(p)
        auth = "admin:nebula-dev"
        sources = {d["uid"]: d for d in get(f"{GRAFANA}/api/datasources", auth)}
        for uid in ("prometheus", "tempo", "loki"):
            if uid not in sources:
                fail(f"Grafana has no {uid} datasource: {list(sources)}")
        boards = get(f"{GRAFANA}/api/search?tag=nebula", auth)
        ok(f"collector, Tempo, Loki, Promtail, Prometheus, Grafana ready; Grafana has 3 linked datasources "
           f"and {len(boards)} dashboards ({', '.join(b['title'].split(' / ')[-1] for b in boards)})")

        step("deploy, route, and traffic — one request with a known trace and request id")
        model = find_model(key, "mock-model")
        version = (ready_version(key, model, "v1") if model else None) or \
            register(key, "mock-model", "mock", "mock", b"mock weights phase8")["version"]
        code, dep = deploy(key, DEP, version["id"], 2)
        if code != 202:
            fail(f"{code} {dep}")
        wait("deployment ready", lambda: status(key, dep["id"])["status"]["state"] == "ready", timeout=300)
        put_route(key, ROUTE, [{"deployment": DEP, "weight": 100}])
        wait("the route serves", lambda: chat(key) == 200, timeout=60, every=1)

        trace_id = uuid.uuid4().hex
        request_id = str(uuid.uuid4())
        if chat(key, SECRET, headers={"traceparent": f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01",
                                      "X-Request-Id": request_id}) != 200:
            fail("the traced request failed")
        # A burst larger than the deployment's admission capacity, so requests queue;
        # half of it asks not to wait, so some are shed rather than queued.
        with ThreadPoolExecutor(max_workers=48) as pool:
            codes = list(pool.map(lambda i: chat(key, max_tokens=48, reject=i % 2 == 0), range(240)))
        ok(f"traced request {request_id} (trace {trace_id}); burst of 240: "
           f"{codes.count(200)} served, {sum(1 for c in codes if c == 429)} shed")

        step("Prometheus scrapes every NEBULA service and every worker")
        def targets() -> dict:
            up = prom(PROM, 'up{job="nebula"}')
            return {r["metric"].get("service") or r["metric"].get("pod", "?"): r["value"][1] for r in up}
        wait("gateway, control plane, controller and workers to be scraped",
             lambda: (lambda t: all(any(s.startswith(n) for s in t) for n in
                                    ("nebula-gateway", "nebula-controlplane", "nebula-controller")) and
                      all(v == "1" for v in t.values()))(targets()), timeout=180, every=5)
        # New worker pods appear at the next service-discovery refresh.
        workers = wait("both worker pods to be scraped", lambda: (lambda w: w if len(w) >= 2 and all(
            r["value"][1] == "1" for r in w) else None)(prom(PROM, 'up{job="nebula", namespace="nebula-workloads"}')),
            timeout=120, every=5)
        ok(f"{len(targets())} scrape targets up, including {len(workers)} worker pods")

        step("every dashboard panel is backed by a real series")
        time.sleep(35)  # rates need two scrapes inside their window
        with open(QUERIES, encoding="utf-8") as f:
            queries = json.load(f)
        empty = []
        total = 0
        for board, panels in queries.items():
            for title, expr in panels:
                total += 1
                if not prom(PROM, expr.replace("$deployment", ".*")):
                    empty.append(f"{board}: {title}")
        if empty:
            fail(f"{len(empty)} of {total} panels have no series: {empty}")
        ok(f"all {total} panels across {len(queries)} dashboards return series")

        step("a histogram exemplar names a trace, and Tempo has it")
        now = time.time()
        ex = get(f"{PROM}/api/v1/query_exemplars?" + urllib.parse.urlencode(
            {"query": "nebula_request_duration_seconds_bucket", "start": now - 600, "end": now}))
        ids = [e["labels"].get("trace_id") for s in ex.get("data", []) for e in s.get("exemplars", [])]
        ids = [i for i in ids if i]
        if not ids:
            fail(f"no exemplars on nebula_request_duration_seconds: {ex}")
        def tempo_has(tid: str) -> bool:
            try:
                return bool(get(f"{TEMPO}/api/traces/{tid}").get("batches"))
            except urllib.error.HTTPError:
                return False
        found = wait("an exemplar's trace in Tempo", lambda: next((i for i in ids if tempo_has(i)), None),
                     timeout=90, every=3)
        ok(f"{len(ids)} exemplars; trace {found} opens in Tempo")

        step("the known request's trace spans gateway and worker")
        def spans() -> dict:
            try:
                t = get(f"{TEMPO}/api/traces/{trace_id}")
            except urllib.error.HTTPError:
                return {}
            out = {}
            for b in t.get("batches", []):
                svc = next((a["value"].get("stringValue") for a in b["resource"]["attributes"]
                            if a["key"] == "service.name"), "?")
                for ss in b.get("scopeSpans", []):
                    for s in ss.get("spans", []):
                        out[s["name"]] = svc
            return out
        want = {"gateway.request": "nebula-gateway", "router.select": "nebula-gateway",
                "dispatch.attempt": "nebula-gateway", "worker.generate": "nebula-worker",
                "runtime.stream": "nebula-worker"}
        got = wait("the full trace", lambda: (lambda s: s if all(n in s for n in want) else None)(spans()),
                   timeout=90, every=3)
        for name, svc in want.items():
            if got[name] != svc:
                fail(f"{name} came from {got[name]}, want {svc}")
        ok(f"{len(got)} spans in trace {trace_id}: " + ", ".join(sorted(got)))

        step("Loki has both services' log lines for the request, with its trace id, and no prompt")
        def lines() -> list[dict]:
            q = '{namespace=~"nebula-system|nebula-workloads"} |= "' + request_id + '"'
            r = get(f"{LOKI}/loki/api/v1/query_range?" + urllib.parse.urlencode(
                {"query": q, "start": int((time.time() - 900) * 1e9), "limit": 200}))
            out = []
            for stream in r["data"]["result"]:
                for _, line in stream["values"]:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        out.append({"raw": line})
            return out
        logs = wait("the request's log lines", lambda: (lambda ls: ls if {l.get("service") for l in ls} >=
                                                     {"nebula-gateway", "nebula-worker"} else None)(lines()),
                    timeout=120, every=5)
        bad = [l for l in logs if l.get("trace_id") != trace_id]
        if bad:
            fail(f"log lines for the request with another trace id: {bad[:2]}")
        if any("7741-obs" in json.dumps(l) for l in logs):
            fail("prompt text reached Loki")
        all_lines = get(f"{LOKI}/loki/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": '{namespace=~"nebula-system|nebula-workloads"} |= "7741-obs"',
             "start": int((time.time() - 900) * 1e9)}))["data"]["result"]
        if all_lines:
            fail("prompt text found somewhere in Loki")
        ok(f"{len(logs)} lines from {sorted({l.get('service') for l in logs})}, all with trace_id {trace_id}; "
           "the prompt appears nowhere")
    finally:
        for p in forwards:
            p.kill()
        step("cleanup")
        remove_route(key, ROUTE)
        remove_deployment(key, DEP)
        ok("route and deployment removed")
    print("\nPHASE 8 DEMO: all steps passed")


if __name__ == "__main__":
    main()
