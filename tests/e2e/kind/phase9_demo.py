#!/usr/bin/env python3
"""Phase 9 demo: load raises replicas, idle lowers them, and every decision is a row.

Run after `make dev-up` (the dev values install Prometheus, which the autoscaler
reads). Uses the standard library and kubectl. The roadmap's exit criterion, walked:

  1. a deployment opts in to autoscaling: one replica, bounds [1, 4], a mock runtime
     slowed to 20 tokens/s with 2 parallel slots, so a handful of clients saturate it;
  2. sustained load: queue depth and queue wait grow, the autoscaler raises the
     desired replica count step by step (never more than double), the controller
     reconciles it, and the new pods come up;
  3. load stops: nothing happens inside the scale-down window, then replicas fall
     back to the minimum one step at a time;
  4. every change is an autoscaling_events row with the signal vector that caused
     it, readable through GET /v1/deployments/{id}/autoscaling-events, and the
     autoscaler's metrics are in Prometheus;
  5. cleanup.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
    GATEWAY,
    WL,
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
from phase8_demo import forward, prom  # noqa: E402

ROUTE = DEP = "autoscale-demo"
K8S = f"nebula-dev-{DEP}"
CLIENTS = 12
# Windows short enough for a demo, the same guards as production.
AUTOSCALING = {"enabled": True, "target_concurrency": 2, "scale_up_stabilization_s": 15,
               "scale_down_stabilization_s": 60, "cooldown_s": 15}


def chat(key: str) -> int:
    body = {"model": ROUTE, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 40}
    req = urllib.request.Request(GATEWAY + "/v1/chat/completions", method="POST", data=json.dumps(body).encode())
    req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            r.read()
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:  # noqa: BLE001 - a dropped connection is a client error here
        return 0


def desired(key: str, dep_id: str) -> int:
    return int(status(key, dep_id)["spec"]["desired_replicas"])


def events(key: str, dep_id: str) -> list[dict]:
    code, page = api("GET", f"/v1/deployments/{dep_id}/autoscaling-events?limit=200", key=key)
    if code != 200:
        fail(f"autoscaling events: {code} {page}")
    return list(reversed(page["data"]))  # oldest first


def show(e: dict) -> str:
    sel = next((s for s in e["signals"] if s.get("selected")), None)
    why = f" [{sel['name']} {sel['observed']} vs {sel['target']}]" if sel else ""
    return f"{e['from_replicas']}->{e['to_replicas']} {e['direction']}/{e['decision']}{why}"


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    p, prom_url = forward("prometheus", 9090)
    stop = threading.Event()
    codes: list[int] = []
    try:
        step("a deployment opts in to autoscaling: 1 replica, bounds [1, 4]")
        model = find_model(key, "mock-model")
        version = (ready_version(key, model, "v1") if model else None) or \
            register(key, "mock-model", "mock", "mock", b"mock weights phase9")["version"]
        code, dep = deploy(key, DEP, version["id"], 1, autoscaling=AUTOSCALING,
                           runtime_overrides={"tokens_per_second": 20, "parallel_slots": 2})
        if code != 202:
            fail(f"{code} {dep}")
        dep_id = dep["id"]
        wait("deployment ready", lambda: status(key, dep_id)["status"]["state"] == "ready", timeout=300)
        put_route(key, ROUTE, [{"deployment": DEP, "weight": 100}])
        wait("the route serves", lambda: chat(key) == 200, timeout=60, every=1)
        ok(f"{DEP} ready with 1 replica; autoscaling {json.dumps(AUTOSCALING)}")

        step(f"sustained load from {CLIENTS} clients: replicas rise, at most doubling per step")

        def client() -> None:
            while not stop.is_set():
                codes.append(chat(key))
        threads = [threading.Thread(target=client, daemon=True) for _ in range(CLIENTS)]
        for t in threads:
            t.start()
        started = time.time()
        wait("the autoscaler to reach the maximum of 4", lambda: desired(key, dep_id) == 4, timeout=300, every=5)
        ok(f"desired 4 after {time.time() - started:.0f}s of load")
        wait("Kubernetes to run 4 ready pods",
             lambda: kubectl("-n", WL, "get", "deploy", K8S, "-o", "jsonpath={.status.readyReplicas}",
                             check=False) == "4", timeout=240, every=5)
        ups = [e for e in events(key, dep_id) if e["decision"] == "scaled" and e["direction"] == "up"]
        if not ups:
            fail("no scale-up was recorded")
        for e in ups:
            if e["to_replicas"] > 2 * e["from_replicas"]:
                fail(f"a step more than doubled: {show(e)}")
            if not any(s.get("selected") and s["ratio"] > 1 for s in e["signals"]):
                fail(f"a scale-up without a signal above target: {e}")
        ok("scale-ups: " + ", ".join(show(e) for e in ups))
        ok(f"4 pods ready; requests so far: {codes.count(200)} served, "
           f"{sum(1 for c in codes if c != 200)} not (shed or queued out)")

        step("load stops: nothing inside the 60s down window, then back to 1 step by step")
        stop.set()
        for t in threads:
            t.join(timeout=130)
        idle_at = time.time()
        time.sleep(45)
        if desired(key, dep_id) != 4:
            fail(f"scaled down to {desired(key, dep_id)} inside the stabilization window")
        ok("still 4 replicas 45s into idle: the window is being observed")
        wait("replicas back to the minimum of 1", lambda: desired(key, dep_id) == 1, timeout=360, every=5)
        ok(f"desired 1 after {time.time() - idle_at:.0f}s idle")
        wait("Kubernetes to run 1 pod",
             lambda: kubectl("-n", WL, "get", "deploy", K8S, "-o", "jsonpath={.spec.replicas}",
                             check=False) == "1", timeout=120, every=5)

        step("every decision is explainable from a row")
        rows = events(key, dep_id)
        downs = [e for e in rows if e["decision"] == "scaled" and e["direction"] == "down"]
        if len(downs) < 1 or any(e["from_replicas"] - e["to_replicas"] != 1 for e in downs):
            fail("scale-down must move one replica at a time below 10 replicas: "
                 + ", ".join(show(e) for e in downs))
        held = [e for e in rows if e["decision"].startswith("suppressed_") or e["decision"].startswith("clamped_")]
        if not any(e["decision"] == "suppressed_stabilization" for e in held):
            fail("no suppressed_stabilization row: the window was not recorded")
        for e in rows:
            print(f"     {e['created_at'][11:19]} {show(e)}  {e['reason'][:110]}")
        ok(f"{len(rows)} rows: {len(ups)} up, {len(downs)} down, {len(held)} suppressed or clamped")

        decided = prom(prom_url, f'sum by (direction, decision) (nebula_autoscale_decisions_total{{deployment="{DEP}"}})')
        if not decided:
            fail("nebula_autoscale_decisions_total has no series for the deployment")
        ok("nebula_autoscale_decisions_total: " + ", ".join(
            f"{r['metric']['direction']}/{r['metric']['decision']}={float(r['value'][1]):.0f}" for r in decided))

        step("cleanup")
        remove_route(key, ROUTE)
        remove_deployment(key, DEP)
        ok("removed")
        print("\nPHASE 9 DEMO PASSED")
    finally:
        stop.set()
        p.kill()


if __name__ == "__main__":
    main()
