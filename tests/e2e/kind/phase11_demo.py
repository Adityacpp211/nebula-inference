#!/usr/bin/env python3
"""Phase 11 demo: model versions change under load, and roll back, losing nothing.

Run after `make dev-up`. Standard library and kubectl only. Clients keep streaming
and not streaming for the whole walk, and every step is judged by what they saw:

  1. three versions of one model: v1, v2, and a v3 whose model cannot load; a ready
     version cannot be changed through the API (the database half of that rule is a
     control-plane integration test);
  2. v1 -> v2 as a rolling update: zero client errors, and both versions serve
     while it rolls, never anything else;
  3. v2 -> v3: the new pods never load; the deployment fails with the worker's own
     reason, at once, while v2 keeps serving 100% of traffic with zero errors;
  4. rollback (to the previous revision, v2), then rollback to revision 1 (v1):
     zero client errors, each a new revision — history is appended, never rewritten
     — and the restored spec is exactly revision 1's;
  5. the audit trail holds every change;
  6. cleanup.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
    WL,
    api,
    deploy,
    fail,
    find_model,
    kubectl,
    ok,
    put_route,
    remove_deployment,
    remove_route,
    seeded_key,
    status,
    step,
    wait,
)
from phase10_failure import Load, k8s_rollout_done  # noqa: E402

MODEL = "versioned-model"
ROUTE = DEP = "versions-demo"
K8S = f"nebula-dev-{DEP}"
REPLICAS = 3


def version(key: str, name: str, payload: bytes, runtime_config: dict | None = None) -> dict:
    """A ready mock version of MODEL, registered once and reused by later runs."""
    model = find_model(key, MODEL)
    if model is None:
        code, model = api("POST", "/v1/models", {"name": MODEL, "task": "chat"}, key)
        if code != 201:
            fail(f"create model: {code} {model}")
    _, page = api("GET", f"/v1/models/{model['id']}/versions?limit=50", key=key)
    for v in page.get("data", []):
        if v["version"] == name and v["status"] == "ready":
            return v
    digest = hashlib.sha256(payload).hexdigest()
    code, ver = api("POST", f"/v1/models/{model['id']}/versions", {
        "version": name, "format": "mock", "runtime": "mock", "size_bytes": len(payload),
        "checksum_sha256": digest, "context_window": 4096, "runtime_config": runtime_config or {},
        "hardware_profile": {"min_ram_mib": 128, "min_cpu_milli": 100},
    }, key)
    if code != 201:
        fail(f"create version {name}: {code} {ver}")
    code, _ = api("PUT", "", raw=payload, url=ver["upload"]["url"])
    if code != 200:
        fail(f"upload {name}: {code}")
    code, fin = api("POST", f"/v1/model-versions/{ver['id']}/finalize", {"checksum_sha256": digest}, key)
    if code != 202:
        fail(f"finalize {name}: {code} {fin}")
    ready = wait(f"{name} verified", lambda: (lambda c, v: v if v.get("status") != "verifying" else None)(
        *api("GET", f"/v1/model-versions/{ver['id']}", key=key)))
    if ready["status"] != "ready":
        fail(f"{name} is {ready['status']}: {ready.get('failure_reason')}")
    return ready


def ref(v: dict) -> str:
    return f"{MODEL}:{v['version']}"


def change(key: str, dep_id: str, body: dict) -> None:
    d = status(key, dep_id)
    code, out = api("PATCH", f"/v1/deployments/{dep_id}", {"generation": d["generation"], **body}, key)
    if code >= 300:
        fail(f"update: {code} {out}")


def rolled_to(key: str, dep_id: str, load: Load, want: str) -> bool:
    """Every pod runs the new revision and the clients see only it."""
    if status(key, dep_id)["status"]["state"] != "ready" or not k8s_rollout_done(K8S, WL, REPLICAS):
        return False
    with load.lock:
        served = {r.version for r in load.results[-40:] if r.ok}
    return served == {want}


def judge_versions(load: Load, what: str, since: float, allowed: set[str]) -> None:
    total, errors = load.window(since)
    with load.lock:
        served = {r.version for r in load.results if r.at >= since and r.ok}
    if errors:
        for e in errors[:6]:
            print(f"       stream={e.stream} {e.detail}")
        fail(f"{what}: {len(errors)} of {total} requests failed")
    if not served <= allowed:
        fail(f"{what}: served {sorted(served)}, only {sorted(allowed)} may serve")
    ok(f"{what}: {total} requests, 0 errors; served by {', '.join(sorted(served))}")


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    load = Load(key, route=ROUTE)
    try:
        step("three versions: v1, v2, and v3 whose model cannot load")
        v1 = version(key, "v1", b"versioned weights one")
        v2 = version(key, "v2", b"versioned weights two")
        v3 = version(key, "v3-broken", b"versioned weights three", {"load_error": "insufficient_memory"})
        ok(f"{ref(v1)}, {ref(v2)}, {ref(v3)} ready")
        code, env = api("POST", f"/v1/model-versions/{v1['id']}/finalize", {"checksum_sha256": "0" * 64}, key)
        if code != 409 or env["error"]["code"] != "invalid_status":
            fail(f"re-finalizing a ready version: {code} {env}")
        code, env = api("POST", f"/v1/model-versions/{v1['id']}/fail", {"reason": "tampering"}, key)
        if code != 409:
            fail(f"failing a ready version: {code} {env}")
        ok("a ready version cannot be changed through the API (409 invalid_status)")

        code, dep = deploy(key, DEP, v1["id"], REPLICAS, runtime_overrides={"tokens_per_second": 60})
        if code != 202:
            fail(f"{code} {dep}")
        dep_id = dep["id"]
        wait("deployment ready", lambda: status(key, dep_id)["status"]["state"] == "ready", timeout=300)
        put_route(key, ROUTE, [{"deployment": DEP, "weight": 100}])
        wait("the route serves", lambda: load.one(stream=False).ok, timeout=60, every=1)
        load.start()
        since = time.time()
        time.sleep(10)
        judge_versions(load, "baseline on v1", since, {ref(v1)})

        step("v1 -> v2 as a rolling update: zero errors, only v1 and v2 serve")
        since = time.time()
        change(key, dep_id, {"model_version_id": v2["id"]})
        wait("every pod on v2", lambda: rolled_to(key, dep_id, load, ref(v2)), timeout=360, every=3)
        judge_versions(load, "v1 -> v2", since, {ref(v1), ref(v2)})

        step("v2 -> v3, which cannot load: the rollout fails with the reason, v2 keeps serving")
        since = time.time()
        change(key, dep_id, {"model_version_id": v3["id"]})
        failed = wait("the rollout to fail", lambda: (lambda s: s if s["state"] == "failed" else None)(
            status(key, dep_id)["status"]), timeout=240, every=3)
        if failed.get("state_reason") != "ModelLoadFailed" or "insufficient" not in (failed.get("state_message") or ""):
            fail(f"the failure must carry the worker's reason: {failed.get('state_reason')} "
                 f"{failed.get('state_message')}")
        ok(f"failed {time.time() - since:.0f}s after the change: {failed['state_reason']}: {failed['state_message']}")
        time.sleep(10)
        judge_versions(load, "v2 -> v3 (cannot load)", since, {ref(v2)})

        step("rollback to the previous revision (v2), then to revision 1 (v1): zero errors")
        since = time.time()
        code, out = api("POST", f"/v1/deployments/{dep_id}/rollback", {}, key)
        if code >= 300:
            fail(f"rollback: {code} {out}")
        wait("back on v2", lambda: rolled_to(key, dep_id, load, ref(v2)), timeout=360, every=3)
        judge_versions(load, "rollback to v2", since, {ref(v2)})
        since = time.time()
        code, out = api("POST", f"/v1/deployments/{dep_id}/rollback", {"revision": 1}, key)
        if code >= 300:
            fail(f"rollback to 1: {code} {out}")
        wait("back on v1", lambda: rolled_to(key, dep_id, load, ref(v1)), timeout=360, every=3)
        judge_versions(load, "rollback to v1", since, {ref(v1), ref(v2)})

        _, page = api("GET", f"/v1/deployments/{dep_id}/revisions?limit=50", key=key)
        revs = sorted(page["data"], key=lambda r: r["revision"])
        reasons = [r["reason"] for r in revs]
        if [r["revision"] for r in revs] != list(range(1, len(revs) + 1)) or reasons[-2:] != ["rollback", "rollback"]:
            fail(f"revisions: {[(r['revision'], r['reason']) for r in revs]}")
        if revs[-1]["model_version_id"] != v1["id"] or revs[-1]["spec_hash"] != revs[0]["spec_hash"]:
            fail(f"the restored spec is not revision 1's: {revs[-1]['spec_hash']} vs {revs[0]['spec_hash']}")
        ok("revisions: " + ", ".join(f"{r['revision']}:{r['reason']}" for r in revs) +
           "; the last is revision 1's spec exactly")

        step("the audit trail holds every change")
        _, audit = api("GET", f"/v1/audit-logs?resource_id={dep_id}&limit=50", key=key)
        actions = [a["action"] for a in reversed(audit.get("data", []))]
        for want in ("deployment.create", "deployment.update", "deployment.rollback"):
            if want not in actions:
                fail(f"no {want} in the audit trail: {actions}")
        ok("audit: " + ", ".join(actions))

        total, errors = load.window(0)
        step("cleanup")
        load.stop.set()
        remove_route(key, ROUTE)
        remove_deployment(key, DEP)
        ok(f"removed; {total} requests over the whole walk, {len(errors)} errors")
        if errors:
            fail("a request failed")
        print("\nPHASE 11 DEMO PASSED")
    finally:
        load.stop.set()


if __name__ == "__main__":
    main()
