#!/usr/bin/env python3
"""Phase 13 demo: 1 000 requests, reconciled from the client to the cost report.

Run after `make dev-up`. Standard library and kubectl only. The roadmap's exit
criterion, walked:

  1. the organization is priced by a profile of its own (the seeded default prices
     everything at zero, so no cost is ever invented);
  2. exactly 1 000 requests go through the gateway; the client keeps its own token
     totals from the responses;
  3. the gateway publishes a usage record per request to JetStream, the controller
     ingests them into `requests` and the hourly `usage_records` rollups;
  4. reconcile: the client's token totals = the sum of the `requests` rows = the
     rollups = GET /v1/usage, and the rows' costs = the rollups' = GET /v1/costs,
     which states its assumptions and the profile version it priced under;
  5. a price change is a new version: later requests are priced under it, and the
     cost of the first 1 000 is unchanged after their rollup is recomputed;
  6. an A/B experiment on the stored rows: a sticky 50/50 split by user against a
     slower variant, every user staying on one arm, compared per arm with bootstrap
     intervals and no claim of significance;
  7. cleanup.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
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

ROUTE = DEP = "cost-demo"
DEP_B = "cost-demo-b"
N = 1000


def psql(sql: str) -> list[str]:
    out = kubectl("-n", "nebula-data", "exec", "postgres-0", "--", "psql", "-U", "nebula", "-d", "nebula",
                  "-tAF", "|", "-c", sql)
    return [line for line in out.splitlines() if line.strip()]


def totals(dep_id: str) -> tuple[int, int, int, int]:
    """Requests, prompt and completion tokens, and cost, from the requests rows."""
    row = psql(f"SELECT count(*), coalesce(sum(prompt_tokens), 0), coalesce(sum(completion_tokens), 0), "
               f"coalesce(sum(estimated_cost_micros), 0) FROM requests WHERE deployment_id = '{dep_id}'")[0]
    a, b, c, d = (int(x) for x in row.split("|"))
    return a, b, c, d


def chat(key: str) -> tuple[int, int, int]:
    code, out = api("POST", "/v1/chat/completions",
                    {"model": ROUTE, "messages": [{"role": "user", "content": "count to four"}], "max_tokens": 8}, key)
    if code != 200:
        return code, 0, 0
    u = out.get("usage") or {}
    return code, int(u.get("prompt_tokens", 0)), int(u.get("completion_tokens", 0))


def usage_of(key: str, dep_id: str, since: str) -> dict:
    code, out = api("GET", f"/v1/usage?group_by=deployment&from={since}", key=key)
    if code != 200:
        fail(f"usage: {code} {out}")
    return next((r for r in out["data"] if r.get("deployment_id") == dep_id), {})


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    remove_deployment(key, DEP_B)
    since = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00Z")

    # Right after an upgrade the gateway may not yet verify keys; wait until it does.
    wait("the API to answer", lambda: api("GET", "/v1/models", key=key)[0] == 200, timeout=120, every=2)

    step("the organization is priced by its own profile")
    code, v1 = api("POST", "/v1/pricing-profiles", {"name": "demo-standard", "cpu_core_hour_micros": 3_600_000,
                                                    "memory_gib_hour_micros": 360_000}, key)
    if code != 201:
        fail(f"pricing profile: {code} {v1}")
    ok(f"demo-standard v{v1['version']}: 3 600 000 µ per core-hour, 360 000 µ per GiB-hour, in use")

    step(f"exactly {N} requests through the gateway")
    model = find_model(key, "mock-model")
    version = (ready_version(key, model, "v1") if model else None) or \
        register(key, "mock-model", "mock", "mock", b"mock weights phase13")["version"]
    code, dep = deploy(key, DEP, version["id"], 2, runtime_overrides={"tokens_per_second": 200, "parallel_slots": 4})
    if code != 202:
        fail(f"{code} {dep}")
    dep_id = dep["id"]
    wait("deployment ready", lambda: status(key, dep_id)["status"]["state"] == "ready", timeout=300)
    put_route(key, ROUTE, [{"deployment": DEP, "weight": 100}])
    wait("the route serves", lambda: api("POST", "/v1/chat/completions",
         {"model": ROUTE, "messages": [{"role": "user", "content": "x"}], "max_tokens": 1}, key)[0] == 200,
         timeout=60, every=1)
    # The readiness probe was a request too (perhaps several). Take a baseline once
    # its usage has been ingested, and reconcile the 1 000 as a difference from it.
    wait("the probe's usage ingested", lambda: usage_of(key, dep_id, since).get("requests", 0) >= 1, timeout=180)
    time.sleep(5)
    base = totals(dep_id)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: chat(key), range(N)))
    bad = [r for r in results if r[0] != 200]
    if bad:
        fail(f"{len(bad)} of {N} requests failed: {bad[0]}")
    client_prompt = sum(r[1] for r in results)
    client_completion = sum(r[2] for r in results)
    ok(f"{N} served; the client counted {client_prompt} prompt and {client_completion} completion tokens")

    step("the usage stream is ingested into requests and hourly rollups")
    wait(f"{N} more requests ingested", lambda: usage_of(key, dep_id, since).get("requests", 0) >= base[0] + N,
         timeout=180, every=2)
    time.sleep(3)
    ok("ingested")

    step("reconcile: client = requests rows = rollups = /v1/usage, and costs end to end")
    req_count, req_prompt, req_completion, req_cost = totals(dep_id)
    roll = psql(f"SELECT sum(request_count), sum(prompt_tokens), sum(completion_tokens), sum(estimated_cost_micros) "
                f"FROM usage_records WHERE deployment_id = '{dep_id}'")[0].split("|")
    roll_count, roll_prompt, roll_completion, roll_cost = (int(x) for x in roll)
    api_usage = usage_of(key, dep_id, since)
    _, costs = api("GET", f"/v1/costs?group_by=deployment&from={since}", key=key)
    api_cost = next(r["estimated_cost_micros"] for r in costs["data"] if r.get("deployment_id") == dep_id)

    checks = {
        "new request rows": (req_count - base[0], N),
        "client prompt tokens vs rows": (client_prompt, req_prompt - base[1]),
        "client completion tokens vs rows": (client_completion, req_completion - base[2]),
        "rows vs rollups (requests)": (req_count, roll_count),
        "rows vs rollups (prompt)": (req_prompt, roll_prompt),
        "rows vs rollups (completion)": (req_completion, roll_completion),
        "rollups vs /v1/usage": (roll_count, api_usage["requests"]),
        "rows vs rollups (cost)": (req_cost, roll_cost),
        "rollups vs /v1/costs": (roll_cost, api_cost),
    }
    for name, (a, b) in checks.items():
        if a != b:
            fail(f"{name}: {a} != {b}")
    if req_cost <= 0:
        fail("a priced profile produced no cost")
    ok(f"{req_count} rows; tokens {req_prompt} + {req_completion}; cost {req_cost} µ — "
       "identical in the rows, the rollups and both APIs")
    used = ", ".join(f"{p['name']} v{p['version']}" for p in costs["pricing_profiles"])
    ok(f"/v1/costs: {costs['total_micros']} µ {costs['currency']} in range, priced under {used}; it assumes:")
    for a in costs["assumptions"]:
        print(f"       - {a}")

    step("a price change is a new version; history is unchanged")
    change = datetime.now(timezone.utc)
    time.sleep(1)
    code, v2 = api("POST", "/v1/pricing-profiles", {"name": "demo-standard", "cpu_core_hour_micros": 10_800_000,
                                                    "memory_gib_hour_micros": 1_080_000}, key)
    if code != 201 or v2["version"] != v1["version"] + 1:
        fail(f"new version: {code} {v2}")
    with ThreadPoolExecutor(max_workers=8) as pool:
        later = list(pool.map(lambda _: chat(key), range(50)))
    if any(r[0] != 200 for r in later):
        fail("a request after the price change failed")
    wait("the later requests ingested", lambda: usage_of(key, dep_id, since).get("requests", 0) >= req_count + 50,
         timeout=180, every=2)
    time.sleep(3)
    stamp = change.strftime("%Y-%m-%d %H:%M:%S+00")
    before = int(psql(f"SELECT coalesce(sum(estimated_cost_micros), 0) FROM requests "
                      f"WHERE deployment_id = '{dep_id}' AND created_at < '{stamp}'")[0])
    after = int(psql(f"SELECT coalesce(sum(estimated_cost_micros), 0) FROM requests "
                     f"WHERE deployment_id = '{dep_id}' AND created_at >= '{stamp}'")[0])
    total = int(psql(f"SELECT sum(estimated_cost_micros) FROM usage_records WHERE deployment_id = '{dep_id}'")[0])
    if before != req_cost:
        fail(f"the first {req_count} requests' cost changed after the price change: {req_cost} -> {before}")
    if total != before + after:
        fail(f"the recomputed rollups ({total}) are not the sum of the rows ({before} + {after})")
    ok(f"the first {req_count} requests still cost {before} µ; the 50 after v{v2['version']} cost {after} µ; "
       f"the recomputed rollups hold {total} µ = {before} + {after}")

    step("an A/B experiment: a sticky 50/50 split against a slower variant, compared per arm")
    code, dep_b = deploy(key, DEP_B, version["id"], 2, runtime_overrides={"tokens_per_second": 40, "parallel_slots": 4})
    if code != 202:
        fail(f"{code} {dep_b}")
    wait("variant ready", lambda: status(key, dep_b["id"])["status"]["state"] == "ready", timeout=300)
    put_route(key, ROUTE, [{"deployment": DEP, "weight": 50, "label": "baseline", "is_baseline": True},
                           {"deployment": DEP_B, "weight": 50, "label": "slower"}])
    time.sleep(8)  # gateways pick up the split
    code, exp = api("POST", "/v1/experiments", {"name": f"speed-{int(time.time())}", "route": ROUTE,
                                                "hypothesis": "the slower variant is slower"}, key)
    if code != 201:
        fail(f"experiment: {code} {exp}")
    users = [f"user-{i}" for i in range(40)]
    seen: dict[str, set[str]] = {u: set() for u in users}

    def as_user(u: str) -> None:
        c, out = api("POST", "/v1/chat/completions", {"model": ROUTE, "user": u, "max_tokens": 8,
                                                      "messages": [{"role": "user", "content": "hi"}]}, key)
        if c == 200:
            seen[u].add(out["nebula"]["deployment"])
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(as_user, [u for u in users for _ in range(10)]))
    moved = [u for u, deps in seen.items() if len(deps) != 1]
    if moved:
        fail(f"users moved between arms: {moved[:3]}")
    arms_used = {next(iter(d)) for d in seen.values()}
    if arms_used != {DEP, DEP_B}:
        fail(f"40 users all landed on {arms_used}")
    ok(f"400 requests from 40 users: every user stayed on one arm; both arms used")
    wait("the experiment's requests ingested", lambda: sum(
        a["requests"] for a in api("GET", f"/v1/experiments/{exp['id']}/results", key=key)[1].get("arms", [])) >= 400,
        timeout=180, every=2)
    code, stopped = api("POST", f"/v1/experiments/{exp['id']}/stop", {"conclusion": "the variant is slower"}, key)
    if code != 200:
        fail(f"stop: {code} {stopped}")
    _, res = api("GET", f"/v1/experiments/{exp['id']}/results", key=key)
    for arm in res["arms"]:
        ok(f"arm {arm['label']}: {arm['requests']} requests, p50 {arm['p50_ms']:.0f} ms, "
           f"{arm['tokens_per_second']} tokens/s, error rate {arm['error_rate']}")
    cmp = res["comparison"][0]
    p50 = cmp.get("p50_ms") or {}
    if not p50 or p50["ci95"][0] <= 0:
        fail(f"the slower variant's p50 interval should exclude zero: {cmp}")
    if res["significance"]["claimed"]:
        fail("significance must never be claimed")
    ok(f"{cmp['variant']} vs {cmp['baseline']}: p50 +{p50['delta']:.0f} ms, 95% CI {p50['ci95']} ({p50['method']}); "
       f"significance claimed: {res['significance']['claimed']}")

    step("cleanup")
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    remove_deployment(key, DEP_B)
    ok("removed")
    print("\nPHASE 13 DEMO PASSED")


if __name__ == "__main__":
    main()
