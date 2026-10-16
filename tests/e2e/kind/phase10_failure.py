#!/usr/bin/env python3
"""Phase 10 failure suite (`make test-failure`): disruptions under live load.

Run after `make dev-up`. Standard library and kubectl only. A load generator keeps
a steady mix of streaming and non-streaming requests running for the whole suite;
every disruption is judged by what those clients saw:

  1. a rolling update of every worker, triggered through the API: zero client errors;
  2. a rolling restart of the gateway: zero client errors;
  3. a worker pod force-deleted, and a worker process killed with SIGKILL (a crash:
     no drain, no preStop): only requests in flight on it may fail, nothing fails
     afterwards, and the deployment recovers;
  4. dependency outages (docs/architecture.md §8.4), one at a time — the control
     plane, PostgreSQL, Redis and NATS: inference continues with zero client errors,
     and each dependency comes back;
  5. cleanup.

A streamed response counts as an error unless it ends with [DONE] and carries no
error event: a stream cut halfway is the failure this suite exists to catch.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase5_demo import (  # noqa: E402
    GATEWAY,
    SYS,
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

ROUTE = DEP = "failure-demo"
K8S = f"nebula-dev-{DEP}"
DATA = "nebula-data"
REPLICAS = 3
CLIENTS = 8
OUTAGE_S = 30


@dataclass
class Result:
    at: float
    stream: bool
    ok: bool
    detail: str = ""
    # The model version that served the request, as the gateway reported it.
    version: str = ""


@dataclass
class Load:
    key: str
    route: str = ROUTE
    results: list[Result] = field(default_factory=list)
    stop: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def one(self, stream: bool) -> Result:
        body = {"model": self.route, "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16,
                "stream": stream}
        req = urllib.request.Request(GATEWAY + "/v1/chat/completions", method="POST",
                                     data=json.dumps(body).encode())
        req.add_header("Authorization", "Bearer " + self.key)
        req.add_header("Content-Type", "application/json")
        started = time.time()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                version = r.headers.get("X-Nebula-Model-Version", "")
                if not stream:
                    out = json.load(r)
                    good = bool(out.get("choices"))
                    version = (out.get("nebula") or {}).get("model_version") or version
                    return Result(started, stream, good, "" if good else json.dumps(out)[:200], version)
                done = False
                for raw in r:
                    line = raw.decode().strip()
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        done = True
                        break
                    if '"error"' in data:
                        return Result(started, stream, False, "error event: " + data[:200])
                return Result(started, stream, done, "" if done else "stream ended without [DONE]", version)
        except urllib.error.HTTPError as e:
            return Result(started, stream, False, f"HTTP {e.code}: {e.read()[:200]!r}")
        except Exception as e:  # noqa: BLE001 - every client-visible failure is a result
            return Result(started, stream, False, f"{type(e).__name__}: {e}")

    def client(self, n: int) -> None:
        i = n
        while not self.stop.is_set():
            res = self.one(stream=i % 2 == 0)
            with self.lock:
                self.results.append(res)
            i += 1

    def start(self) -> None:
        for n in range(CLIENTS):
            threading.Thread(target=self.client, args=(n,), daemon=True).start()

    def window(self, since: float, until: float | None = None) -> tuple[int, list[Result]]:
        until = until or time.time()
        with self.lock:
            inside = [r for r in self.results if since <= r.at <= until]
        return len(inside), [r for r in inside if not r.ok]


BASELINE_RATE = 0.0


def judge(load: Load, what: str, since: float, allowed: int = 0, min_share: float = 0.0) -> None:
    """Errors, and optionally throughput: an outage that keeps every request alive
    but makes each one wait on a dead dependency is a failure too."""
    until = time.time()
    total, errors = load.window(since, until)
    if total == 0:
        fail(f"{what}: no requests completed during the disruption")
    rate = total / max(until - since, 1e-9)
    if min_share and rate < min_share * BASELINE_RATE:
        fail(f"{what}: {rate:.1f} req/s against a baseline of {BASELINE_RATE:.1f}: requests are waiting on "
             "the failed dependency")
    if len(errors) > allowed:
        for e in errors[:8]:
            print(f"       {time.strftime('%H:%M:%S', time.localtime(e.at))} stream={e.stream} {e.detail}")
        fail(f"{what}: {len(errors)} of {total} requests failed (allowed {allowed})")
    ok(f"{what}: {total} requests ({rate:.1f}/s), {len(errors)} errors")


def k8s_rollout_done(name: str, ns: str, replicas: int) -> bool:
    out = kubectl("-n", ns, "get", "deploy", name, "-o",
                  "jsonpath={.metadata.generation}/{.status.observedGeneration}/{.status.updatedReplicas}/"
                  "{.status.readyReplicas}/{.status.replicas}", check=False)
    parts = out.split("/")
    return len(parts) == 5 and parts[0] == parts[1] and parts[2:] == [str(replicas)] * 3


def pod_names(dep_id: str) -> list[str]:
    return kubectl("-n", WL, "get", "pods", "-l", f"nebula.dev/deployment-id={dep_id}",
                   "--field-selector=status.phase=Running",
                   "-o", "jsonpath={range .items[*]}{.metadata.name} {end}").split()


def container_of(pod: str) -> tuple[str, str, int]:
    """The worker container of a pod: its node, its runtime id and its restart count."""
    out = kubectl("-n", WL, "get", "pod", pod, "-o", "jsonpath={.spec.nodeName} "
                  "{.status.containerStatuses[?(@.name==\"worker\")].containerID} "
                  "{.status.containerStatuses[?(@.name==\"worker\")].restartCount}", check=False).split()
    if len(out) != 3:
        return "", "", -1
    return out[0], out[1].split("://", 1)[-1], int(out[2])


def scale(kind: str, name: str, ns: str, n: int) -> None:
    kubectl("-n", ns, "scale", f"{kind}/{name}", f"--replicas={n}")


def gone(selector: str, ns: str) -> bool:
    return kubectl("-n", ns, "get", "pods", "-l", selector, "-o", "name", check=False).strip() == ""


def ready(kind: str, name: str, ns: str) -> bool:
    return kubectl("-n", ns, "get", f"{kind}/{name}", "-o", "jsonpath={.status.readyReplicas}",
                   check=False) == "1"


def outage(load: Load, what: str, kind: str, name: str, ns: str, selector: str) -> None:
    scale(kind, name, ns, 0)
    wait(f"{what} to be down", lambda: gone(selector, ns), timeout=120, every=2)
    since = time.time()
    time.sleep(OUTAGE_S)
    judge(load, f"{what} down for {OUTAGE_S}s", since, min_share=0.5)
    scale(kind, name, ns, 1)
    wait(f"{what} to come back", lambda: ready(kind, name, ns), timeout=240, every=3)


def main() -> None:
    key = seeded_key()
    remove_route(key, ROUTE)
    remove_deployment(key, DEP)
    load = Load(key)
    try:
        step(f"a deployment with {REPLICAS} replicas behind a route, under steady mixed load")
        model = find_model(key, "mock-model")
        version = (ready_version(key, model, "v1") if model else None) or \
            register(key, "mock-model", "mock", "mock", b"mock weights phase10")["version"]
        code, dep = deploy(key, DEP, version["id"], REPLICAS,
                           runtime_overrides={"tokens_per_second": 60, "parallel_slots": 4})
        if code != 202:
            fail(f"{code} {dep}")
        dep_id = dep["id"]
        wait("deployment ready", lambda: status(key, dep_id)["status"]["state"] == "ready", timeout=300)
        put_route(key, ROUTE, [{"deployment": DEP, "weight": 100}])
        wait("the route serves", lambda: load.one(stream=False).ok, timeout=60, every=1)
        load.start()
        since = time.time()
        time.sleep(15)
        judge(load, "baseline", since)
        global BASELINE_RATE
        BASELINE_RATE = load.window(since)[0] / 15

        step("rolling update of every worker, through the API: zero client errors")
        before = set(pod_names(dep_id))
        d = status(key, dep_id)
        code, out = api("PATCH", f"/v1/deployments/{dep_id}", {
            "generation": d["generation"], "runtime_overrides": {"tokens_per_second": 61, "parallel_slots": 4}}, key)
        if code >= 300:
            fail(f"update: {code} {out}")
        since = time.time()
        wait("every pod replaced", lambda: k8s_rollout_done(K8S, WL, REPLICAS) and
             not (set(pod_names(dep_id)) & before) and len(pod_names(dep_id)) == REPLICAS, timeout=360, every=3)
        time.sleep(10)  # requests that started on the last old pod finish
        judge(load, f"rolling update ({len(before)} pods replaced)", since)

        step("rolling restart of the gateway: zero client errors")
        since = time.time()
        kubectl("-n", SYS, "rollout", "restart", "deploy/nebula-gateway")
        kubectl("-n", SYS, "rollout", "status", "deploy/nebula-gateway", "--timeout=300s")
        time.sleep(15)  # the old pod drains for its preStop and grace period
        judge(load, "gateway rolling restart", since)

        step("a worker pod force-deleted: only its in-flight requests may fail, then recovery")
        victim = pod_names(dep_id)[0]
        killed = time.time()
        kubectl("-n", WL, "delete", "pod", victim, "--grace-period=0", "--force", "--wait=false")
        wait(f"{REPLICAS} ready replicas again", lambda: k8s_rollout_done(K8S, WL, REPLICAS) and
             victim not in pod_names(dep_id), timeout=240, every=3)
        # A killed pod's in-flight requests: its slots times the gateway's overcommit.
        _, early = load.window(killed, killed + 15)
        if len(early) > 8:
            fail(f"{len(early)} failures after the kill: more than one pod's in-flight requests")
        ok(f"{len(early)} requests failed with the pod (at most its 8 in-flight), replacement ready")
        since = killed + 15
        time.sleep(max(0, since + 15 - time.time()))
        judge(load, "after the kill", since)

        step("a worker process killed with SIGKILL: a crash, with no drain and no preStop")
        victim = pod_names(dep_id)[0]
        node, cid, restarts = container_of(victim)
        crashed = time.time()
        # Through the node's container runtime: SIGKILL with no grace, exactly what an
        # OOM kill or a segfault looks like from outside.
        subprocess.run(["docker", "exec", node, "crictl", "stop", "--timeout", "0", cid],
                       check=True, capture_output=True)
        wait("the container restarted and the pod ready again", lambda: (lambda n, c, r: r > restarts and
             k8s_rollout_done(K8S, WL, REPLICAS))(*container_of(victim)), timeout=240, every=3)
        _, early = load.window(crashed, crashed + 15)
        if len(early) > 8:
            fail(f"{len(early)} failures after the crash: more than one pod's in-flight requests")
        ok(f"{len(early)} requests failed with the crashed process (at most its 8 in-flight); restarted in place")
        since = crashed + 15
        time.sleep(max(0, since + 15 - time.time()))
        judge(load, "after the crash", since)

        step("dependency outages: inference continues (docs/architecture.md §8.4)")
        outage(load, "control plane", "deploy", "nebula-controlplane", SYS,
               "app.kubernetes.io/name=nebula-controlplane")
        outage(load, "PostgreSQL", "statefulset", "postgres", DATA, "app.kubernetes.io/name=nebula-postgres")
        wait("the control plane ready again", lambda: ready("deploy", "nebula-controlplane", SYS), timeout=240)
        outage(load, "Redis", "deploy", "redis", DATA, "app.kubernetes.io/name=nebula-redis")
        outage(load, "NATS", "deploy", "nats", DATA, "app.kubernetes.io/name=nebula-nats")
        since = time.time()
        time.sleep(15)
        judge(load, "after every dependency returned", since)

        total, errors = load.window(0)
        step("cleanup")
        load.stop.set()
        remove_route(key, ROUTE)
        remove_deployment(key, DEP)
        ok(f"removed; {total} requests over the whole suite, {len(errors)} errors")
        print("\nPHASE 10 FAILURE SUITE PASSED")
    finally:
        load.stop.set()


if __name__ == "__main__":
    main()
