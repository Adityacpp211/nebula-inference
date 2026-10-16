#!/usr/bin/env python3
"""Phase 12 demo: canary rollouts that promote on health and roll back on a breach.

Run after `make dev-up` (the dev values install Prometheus, which the rollout
reconciler reads). Standard library and kubectl only. Clients keep streaming and
not streaming against one route for the whole walk:

  1. a route at 100% on a baseline deployment, and three candidates: a healthy one,
     one that fails every generation partway, and one that is six times slower;
  2. the healthy canary walks 10 -> 25 -> 50 -> 100 on its metrics, holding each
     step, and is promoted — with zero client errors;
  3. a second rollout on the same route is refused while one is active;
  4. the failing canary is aborted at its first analysis and every request goes
     back to the baseline automatically, with the error-rate breach recorded; after
     the rollback no client sees an error;
  5. the slow canary is aborted on its p95 latency, with that reason recorded;
  6. the audit trail holds every move; cleanup.
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
    api,
    deploy,
    fail,
    find_model,
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
from phase10_failure import Load  # noqa: E402

ROUTE = "canary-demo"
BASE, GOOD, BAD, SLOW = "cnry-base", "cnry-good", "cnry-bad", "cnry-slow"
ANALYSIS = {"hold_s": 30, "min_requests": 20, "on_insufficient_data": "extend", "max_extensions": 4}


def rollout(key: str, rid: str) -> dict:
    code, ro = api("GET", f"/v1/rollouts/{rid}", key=key)
    if code != 200:
        fail(f"rollout: {code} {ro}")
    return ro


def start(key: str, canary: str, steps: list[int]) -> dict:
    code, ro = api("POST", "/v1/rollouts", {"route": ROUTE, "canary_deployment": canary, "steps": steps,
                                             "analysis": ANALYSIS}, key)
    if code != 201:
        fail(f"start rollout to {canary}: {code} {ro}")
    return ro


def finished(key: str, rid: str, seen: list[int]) -> dict | None:
    ro = rollout(key, rid)
    if not seen or seen[-1] != ro["canary_weight"]:
        seen.append(ro["canary_weight"])
    return ro if ro["state"] not in ("pending", "progressing") else None


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    for d in (BASE, GOOD, BAD, SLOW):
        remove_deployment(key, d)
    load = Load(key, route=ROUTE)
    try:
        step("a route on a baseline, and three candidates: healthy, failing, slow")
        model = find_model(key, "mock-model")
        version = (ready_version(key, model, "v1") if model else None) or \
            register(key, "mock-model", "mock", "mock", b"mock weights phase12")["version"]
        fast = {"tokens_per_second": 80, "parallel_slots": 4}
        ids = {}
        for name, overrides in ((BASE, fast), (GOOD, fast), (BAD, {**fast, "fail_after_tokens": 3}),
                                (SLOW, {**fast, "stall_before_first_token_ms": 1500})):
            code, dep = deploy(key, name, version["id"], 2, runtime_overrides=overrides)
            if code != 202:
                fail(f"{name}: {code} {dep}")
            ids[name] = dep["id"]
        for name, dep_id in ids.items():
            wait(f"{name} ready", lambda d=dep_id: status(key, d)["status"]["state"] == "ready", timeout=300)
        put_route(key, ROUTE, [{"deployment": BASE, "weight": 100}])
        wait("the route serves", lambda: load.one(stream=False).ok, timeout=60, every=1)
        load.start()
        since = time.time()
        time.sleep(10)
        total, errors = load.window(since)
        if errors:
            fail(f"baseline: {len(errors)} errors")
        ok(f"baseline: {total} requests, 0 errors")

        step("a healthy canary walks 10 -> 25 -> 50 -> 100 on its metrics: zero client errors")
        since = time.time()
        ro = start(key, GOOD, [10, 25, 50, 100])
        seen: list[int] = []
        done = wait("the rollout to finish", lambda: finished(key, ro["id"], seen), timeout=600, every=2)
        total, errors = load.window(since)
        if done["state"] != "promoted":
            fail(f"healthy canary ended {done['state']}: {done.get('abort_reason')} {done.get('last_analysis')}")
        if [w for w in seen if w] != [10, 25, 50, 100]:
            fail(f"weights seen: {seen}")
        if errors:
            for e in errors[:5]:
                print(f"       stream={e.stream} {e.detail}")
            fail(f"{len(errors)} of {total} requests failed during a healthy rollout")
        last = done["last_analysis"] or {}
        ok(f"promoted after {time.time() - since:.0f}s through weights {seen}; {total} requests, 0 errors")
        ok(f"last analysis: {last.get('reason')}")

        step("a second rollout on the route is refused while one is active")
        ro_bad = start(key, BAD, [10, 50, 100])
        code, env = api("POST", "/v1/rollouts", {"route": ROUTE, "canary_deployment": SLOW}, key)
        if code != 409 or env["error"]["code"] != "rollout_active":
            fail(f"concurrent rollout: {code} {env}")
        ok("409 rollout_active, from the database's one-active-rollout-per-route index")

        step("a canary failing its generations is rolled back automatically, with the reason")
        since = time.time()
        done = wait("the rollback", lambda: finished(key, ro_bad["id"], []), timeout=300, every=2)
        if done["state"] != "rolled_back" or "error rate" not in (done.get("abort_reason") or ""):
            fail(f"failing canary: {done['state']} {done.get('abort_reason')}")
        if done["canary_weight"] != 0:
            fail(f"weights not restored: canary at {done['canary_weight']}")
        total, errors = load.window(since)
        ok(f"rolled back after {time.time() - since:.0f}s: {done['abort_reason']}")
        ok(f"{len(errors)} of {total} requests failed while the canary held 10% — the blast radius")
        time.sleep(15)  # gateways pick up the restored weights
        after = time.time()
        time.sleep(20)
        total, errors = load.window(after)
        if errors:
            fail(f"{len(errors)} of {total} requests failed after the rollback")
        ok(f"after the rollback: {total} requests, 0 errors")

        step("a slow canary is rolled back on its p95 latency")
        since = time.time()
        ro_slow = start(key, SLOW, [10, 50, 100])
        done = wait("the rollback", lambda: finished(key, ro_slow["id"], []), timeout=300, every=2)
        if done["state"] != "rolled_back" or "p95" not in (done.get("abort_reason") or ""):
            fail(f"slow canary: {done['state']} {done.get('abort_reason')}")
        ok(f"rolled back after {time.time() - since:.0f}s: {done['abort_reason']}")

        step("the audit trail holds every move")
        _, audit = api("GET", "/v1/audit-logs?resource_type=rollout&limit=100", key=key)
        actions = [a["action"] for a in reversed(audit.get("data", []))]
        for want in ("rollout.create", "rollout.started", "rollout.advanced", "rollout.promoted", "rollout.rolled_back"):
            if want not in actions:
                fail(f"no {want} in the audit trail: {actions}")
        ok(f"{len(actions)} rollout audit rows: " + ", ".join(sorted(set(actions))))

        total, errors = load.window(0)
        step("cleanup")
        load.stop.set()
        remove_route(key, ROUTE)
        for d in (BASE, GOOD, BAD, SLOW):
            remove_deployment(key, d)
        ok(f"removed; {total} requests over the whole walk")
        print("\nPHASE 12 DEMO PASSED")
    finally:
        load.stop.set()


if __name__ == "__main__":
    main()
