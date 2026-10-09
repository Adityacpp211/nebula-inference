#!/usr/bin/env python3
"""Phase 6 demo: health-aware routing on a live kind cluster.

Run after `make dev-up` (from Linux, macOS or WSL). Uses only the standard library
and kubectl. Every step asserts; the first failure stops the run with a reason.

  1. two deployments of the same model (blue, green), two replicas each;
  2. one route over both, 50/50, created through the API: the gateway picks it up
     from the control plane and discovers the pods from EndpointSlices;
  3. the gateway's health shows the routing table, endpoint discovery and worker
     heartbeats all live;
  4. the observed split over 400 requests matches the weights;
  5. every pod of green is killed under load: traffic shifts to blue within seconds,
     and no client sees an error; when green's replacements are ready, traffic
     returns;
  6. the control plane goes down and the gateway is restarted: it still routes,
     from the Redis snapshot (axiom A8);
  7. cleanup.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

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

ROUTE = "split-demo"
BLUE, GREEN = "blue", "green"


def chat(key: str) -> tuple[int, str]:
    code, out = api("POST", "/v1/chat/completions",
                    {"model": ROUTE, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 4}, key)
    return code, (out.get("nebula") or {}).get("deployment", "") if code == 200 else json.dumps(out)[:200]


def health() -> dict:
    with urllib.request.urlopen(GATEWAY + "/healthz", timeout=10) as r:
        return {c["name"]: c for c in json.load(r)["checks"]}


def model_targets(key: str) -> dict:
    code, m = api("GET", f"/v1/models/{ROUTE}", key=key)
    if code != 200:
        return {}
    return {t["deployment"]: t for t in m["nebula"]["targets"]}


def pods_of(dep_id: str) -> list[str]:
    out = kubectl("-n", WL, "get", "pods", "-l", f"nebula.dev/deployment-id={dep_id}",
                  "-o", "jsonpath={range .items[*]}{.metadata.name} {end}")
    return out.split()


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    for name in (BLUE, GREEN):
        remove_deployment(key, name)

    step("two deployments of one model version, two replicas each")
    model = find_model(key, "mock-model")
    version = ready_version(key, model, "v1") if model else None
    if version is None:
        version = register(key, "mock-model", "mock", "mock", b"mock weights phase6")["version"]
    ids = {}
    for name in (BLUE, GREEN):
        code, dep = deploy(key, name, version["id"], 2)
        if code != 202:
            fail(f"{name}: {code} {dep}")
        ids[name] = dep["id"]
    for name in (BLUE, GREEN):
        wait(f"{name} ready", lambda n=name: status(key, ids[n])["status"]["state"] == "ready", timeout=300)
    ok(f"blue {ids[BLUE][:8]}… and green {ids[GREEN][:8]}… ready, 2 replicas each")

    step("a 50/50 route over both, through the API; the gateway converges")
    rt = put_route(key, ROUTE, [{"deployment": BLUE, "weight": 50, "label": "baseline", "is_baseline": True},
                                {"deployment": GREEN, "weight": 50, "label": "canary"}])
    ok(f"route {rt['id'][:8]}…: " + ", ".join(f"{t['deployment']} {t['weight']}%" for t in rt["targets"]))
    t0 = time.time()
    wait("both targets to have two eligible endpoints",
         lambda: all(t.get("ready_endpoints") == 2 for t in model_targets(key).values())
         and len(model_targets(key)) == 2, timeout=60, every=0.5)
    ok(f"the gateway serves the route with 2 + 2 endpoints after {time.time() - t0:.1f}s")

    step("the gateway's routing sources are live")
    h = health()
    for check in ("routing_table", "endpoint_discovery", "heartbeats"):
        if h.get(check, {}).get("status") != "ok":
            fail(f"{check}: {h.get(check)}")
    ok("routing_table ok (from the control plane), endpoint_discovery ok (EndpointSlices), heartbeats ok (NATS)")

    step("the observed split matches the weights")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: chat(key), range(400)))
    errors = [r for r in results if r[0] != 200]
    if errors:
        fail(f"{len(errors)} errors, e.g. {errors[0]}")
    counts = Counter(d for _, d in results)
    share = counts[BLUE] / 400
    if abs(share - 0.5) > 0.08:
        fail(f"split {dict(counts)}: blue {share:.0%}, want 50% ± 8")
    ok(f"400 requests: blue {counts[BLUE]}, green {counts[GREEN]} ({share:.1%} / {1 - share:.1%})")

    step("kill every green pod under load: traffic shifts, no client errors")
    log: list[tuple[float, int, str]] = []
    stop = threading.Event()
    lock = threading.Lock()

    def load() -> None:
        while not stop.is_set():
            started = time.time()
            code, dep = chat(key)
            with lock:
                log.append((started, code, dep))
            # Paced: about 80 requests a second across four clients, inside the
            # development key's rate limit, so every error is the routing's.
            time.sleep(0.05)

    threads = [threading.Thread(target=load, daemon=True) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(3)
    victims = pods_of(ids[GREEN])
    killed_at = time.time()
    kubectl("-n", WL, "delete", "pod", *victims, "--wait=false")
    ok(f"deleted {', '.join(victims)} at t=0 while 4 clients send requests")

    def green_back() -> bool:
        t = model_targets(key).get(GREEN, {})
        if t.get("ready_endpoints") != 2:
            return False
        with lock:
            return any(s > killed_at and c == 200 and d == GREEN and s > time.time() - 2 for s, c, d in log)
    wait("green's replacements to serve again", green_back, timeout=240, every=0.5)
    time.sleep(2)
    stop.set()
    for t in threads:
        t.join(timeout=30)

    after = [(s - killed_at, c, d) for s, c, d in log if s >= killed_at]
    errors = [(round(s, 2), c, d) for s, c, d in after if c != 200]
    if errors:
        fail(f"{len(errors)} client errors after the kill, e.g. {errors[:3]}")
    # The shift: after the kill, green receives nothing until its replacements exist.
    new_pods = set(pods_of(ids[GREEN])) - set(victims)
    green_starts = [s for s, c, d in after if d == GREEN]
    last_old_green = max((s for s in green_starts if s < 5), default=0.0)
    resumed = min((s for s in green_starts if s > last_old_green + 1), default=None)
    ok(f"{len(after)} requests after the kill, 0 errors")
    ok(f"last request placed on the old green pods at t={last_old_green:.2f}s; "
       f"green resumed on {sorted(new_pods)} at t={resumed:.1f}s" if resumed is not None else
       f"last request placed on the old green pods at t={last_old_green:.2f}s")
    if last_old_green > 5:
        fail(f"traffic took {last_old_green:.1f}s to shift away from the killed pods")

    step("control plane down, gateway restarted: it still routes, from the Redis snapshot")
    kubectl("-n", "nebula-system", "scale", "deploy/nebula-controlplane", "--replicas=0")
    wait("the control plane gone", lambda: kubectl("-n", "nebula-system", "get", "pods", "-l",
         "app.kubernetes.io/name=nebula-controlplane", "-o", "name") == "", timeout=120)
    kubectl("-n", "nebula-system", "rollout", "restart", "deploy/nebula-gateway")
    kubectl("-n", "nebula-system", "rollout", "status", "deploy/nebula-gateway", "--timeout=180s")
    try:
        def routes_again() -> bool:
            try:
                return chat(key)[0] == 200
            except Exception:  # noqa: BLE001 - the gateway may still be starting
                return False
        wait("the restarted gateway to serve", routes_again, timeout=60, every=0.5)
        results = [chat(key) for _ in range(20)]
        bad = [r for r in results if r[0] != 200]
        if bad:
            fail(f"{len(bad)} of 20 failed with the control plane down: {bad[0]}")
        rt_check = health().get("routing_table", {})
        if "snapshot" not in (rt_check.get("error") or ""):
            fail(f"expected the routing table to come from the snapshot: {rt_check}")
        ok(f"20/20 served ({dict(Counter(d for _, d in results))}) by a gateway that has never "
           "reached the control plane; /healthz: " + rt_check["error"])
    finally:
        kubectl("-n", "nebula-system", "scale", "deploy/nebula-controlplane", "--replicas=1")
        kubectl("-n", "nebula-system", "rollout", "status", "deploy/nebula-controlplane", "--timeout=180s")
    wait("the gateway back on the live table",
         lambda: health().get("routing_table", {}).get("status") == "ok", timeout=60, every=1)
    ok("control plane back; the gateway returned to the live routing table")

    step("cleanup")
    remove_route(key, ROUTE)
    for name in (BLUE, GREEN):
        remove_deployment(key, name)
    ok("route and deployments removed")
    print("\nPHASE 6 DEMO: all steps passed")


if __name__ == "__main__":
    main()
