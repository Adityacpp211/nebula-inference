#!/usr/bin/env python3
"""Phase 5, demonstrated against a real kind cluster (scripts/dev-up.sh).

Standard library only, so it runs wherever kubectl does. Every step asserts; the
first failure stops the run with the evidence. Usage:

    python3 tests/e2e/kind/phase5_demo.py            # after scripts/dev-up.sh

It does, in order — the roadmap's Phase 5 exit criteria and tests:

  1. registers a model version by uploading bytes to a presigned URL, and waits
     for the control plane to verify them (verification: computed);
  2. creates a deployment with curl-equivalent calls through the gateway and
     waits for it to be ready, with real pods on kind;
  3. sends inference through the gateway to those pods;
  4. kills a pod and watches Kubernetes replace it;
  5. deletes the Deployment object and watches the controller recreate it;
  6. edits the Deployment by hand and watches the controller revert the drift;
  7. scales through the API and sees Kubernetes follow;
  8. asks for an impossible deployment and gets 422 before anything is created;
  9. asserts RBAC with `kubectl auth can-i`, including the negatives;
 10. proves a pod in the workload namespace cannot reach PostgreSQL, and can reach
     the artifact store (so the negative is not vacuous);
 11. tampers with a verified artifact in the store and sees the artifact puller
     refuse it, failing the deployment with ArtifactChecksumMismatch;
 12. stops and deletes the deployment and sees its objects removed.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

GATEWAY = os.environ.get("NEBULA_GATEWAY_URL", "http://127.0.0.1:8080")
WL = "nebula-workloads"
SYS = "nebula-system"
STEP = 0


def step(title: str) -> None:
    global STEP
    STEP += 1
    print(f"\n[{STEP:2d}] {title}", flush=True)


def ok(msg: str) -> None:
    print(f"     ok  {msg}", flush=True)


def fail(msg: str) -> None:
    print(f"     FAIL {msg}", flush=True)
    sys.exit(1)


def kubectl(*args: str, check: bool = True) -> str:
    r = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=180)
    if check and r.returncode != 0:
        fail(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    # `auth can-i` answers "no" on stdout with exit status 1, so prefer stdout.
    return r.stdout.strip() if r.returncode == 0 or r.stdout.strip() else r.stderr.strip()


def api(method: str, path: str, body: object | None = None, key: str | None = None,
        raw: bytes | None = None, url: str | None = None) -> tuple[int, dict]:
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(url or GATEWAY + path, data=data, method=method)
    if key:
        req.add_header("Authorization", f"Bearer {key}")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            text = resp.read().decode() or "{}"
            return resp.status, json.loads(text) if text.startswith("{") else {"raw": text}
    except urllib.error.HTTPError as e:
        text = e.read().decode() or "{}"
        return e.code, json.loads(text) if text.startswith("{") else {"raw": text}


def wait(what: str, cond, timeout: float = 180, every: float = 2):  # type: ignore[no-untyped-def]
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = cond()
        if last:
            return last
        time.sleep(every)
    fail(f"timed out waiting for {what} (last: {last})")


DEV_KEY = "nbk_devkey0NEBULAdevelopmentKeyDoNotUseInProduction000"


def seeded_key() -> str:
    """The development seed key, fixed in deploy/helm/nebula/values-dev.yaml."""
    return os.environ.get("NEBULA_API_KEY", DEV_KEY)


def gguf_header() -> bytes:
    """A minimal valid GGUF header: the registry parses it at verification."""
    def s(v: str) -> bytes:
        return struct.pack("<Q", len(v)) + v.encode()
    out = b"GGUF" + struct.pack("<IQQ", 3, 0, 2)
    out += s("general.architecture") + struct.pack("<I", 8) + s("llama")
    out += s("llama.context_length") + struct.pack("<II", 4, 4096)
    return out + b"\0" * 64


def find_model(key: str, name: str) -> dict | None:
    _, page = api("GET", "/v1/models/registry?limit=200", key=key)
    return next((m for m in page.get("data", []) if m["name"] == name), None)


def ready_version(key: str, model: dict, version: str) -> dict | None:
    _, page = api("GET", f"/v1/models/{model['id']}/versions?limit=200", key=key)
    return next((v for v in page.get("data", []) if v["version"] == version and v["status"] == "ready"), None)


def register(key: str, name: str, fmt: str, runtime: str, payload: bytes, version: str = "v1") -> dict:
    model = find_model(key, name)
    if model is None:
        code, model = api("POST", "/v1/models", {"name": name, "task": "chat"}, key)
        if code != 201:
            fail(f"create model: {code} {model}")
    digest = hashlib.sha256(payload).hexdigest()
    code, ver = api("POST", f"/v1/models/{model['id']}/versions", {
        "version": version, "format": fmt, "runtime": runtime, "size_bytes": len(payload),
        "checksum_sha256": digest, "context_window": 4096,
        "hardware_profile": {"min_ram_mib": 128, "min_cpu_milli": 100},
    }, key)
    if code != 201 or not ver.get("upload"):
        fail(f"create version: {code} {ver}")
    code, _ = api("PUT", "", raw=payload, url=ver["upload"]["url"])
    if code != 200:
        fail(f"upload to the presigned URL: {code}")
    code, fin = api("POST", f"/v1/model-versions/{ver['id']}/finalize", {"checksum_sha256": digest}, key)
    if code != 202:
        fail(f"finalize: {code} {fin}")
    ready = wait("verification", lambda: (lambda c, v: v if v.get("status") != "verifying" else None)(
        *api("GET", f"/v1/model-versions/{ver['id']}", key=key)))
    if ready["status"] != "ready":
        fail(f"version {ready['status']}: {ready.get('failure_reason')}")
    return {"model": model, "version": ready, "digest": digest}


def deploy(key: str, name: str, version_id: str, replicas: int, **extra) -> dict:  # type: ignore[no-untyped-def]
    # Bounds wide enough to scale within: replicas alone sets min = max = replicas.
    body = {"name": name, "model_version_id": version_id, "replicas": replicas,
            "min_replicas": 1, "max_replicas": 4,
            "resources": {"cpu_milli": 100, "memory_mib": 256}, **extra}
    return api("POST", "/v1/deployments", body, key)


def status(key: str, dep_id: str) -> dict:
    return api("GET", f"/v1/deployments/{dep_id}", key=key)[1]


def k8s_ready(name: str) -> str:
    return kubectl("-n", WL, "get", "deploy", name, "-o",
                   "jsonpath={.status.readyReplicas}/{.spec.replicas}", check=False)


def remove_route(key: str, model_name: str) -> None:
    """Delete a route by its model name, if it exists: a routed deployment cannot be deleted."""
    _, page = api("GET", "/v1/routes?limit=200", key=key)
    for r in page.get("data", []):
        if r["model_name"] == model_name:
            api("DELETE", f"/v1/routes/{r['id']}", key=key)


def put_route(key: str, model_name: str, targets: list[dict]) -> dict:
    """Create a route, or replace an existing one's targets."""
    _, page = api("GET", "/v1/routes?limit=200", key=key)
    for r in page.get("data", []):
        if r["model_name"] == model_name:
            code, out = api("PATCH", f"/v1/routes/{r['id']}", {"targets": targets}, key)
            break
    else:
        code, out = api("POST", "/v1/routes", {"model_name": model_name, "targets": targets}, key)
    if code >= 300:
        fail(f"route {model_name}: {code} {out}")
    return out


def remove_deployment(key: str, name: str) -> None:
    """Stop and delete a deployment left by an earlier run, so the demo is rerunnable."""
    _, page = api("GET", "/v1/deployments?limit=200", key=key)
    for d in page.get("data", []):
        if d["name"] != name:
            continue
        api("POST", f"/v1/deployments/{d['id']}/stop", {}, key)
        wait(f"{name} stopped", lambda: status(key, d["id"])["status"]["state"] in ("stopped", "failed"), timeout=180)
        api("DELETE", f"/v1/deployments/{d['id']}", key=key)
        wait(f"{name}'s objects removed", lambda: "NotFound" in kubectl(
            "-n", WL, "get", "deploy", f"nebula-dev-{name}", check=False))


def main() -> None:
    key = seeded_key()
    run = uuid.uuid4().hex[:6]
    dep_name = "mock-demo"
    k8s_name = f"nebula-dev-{dep_name}"
    remove_route(key, "nebula-mock")
    remove_deployment(key, dep_name)

    step("register a model: upload to a presigned URL, verified by reading the bytes")
    # The gateway's development route asserts mock-model:v1, so that exact version
    # is registered once and reused by later runs.
    model = find_model(key, "mock-model")
    existing = ready_version(key, model, "v1") if model else None
    if existing:
        reg = {"model": model, "version": existing, "digest": existing["checksum_sha256"]}
        ok(f"reusing ready version {existing['id']} from an earlier run")
    else:
        reg = register(key, "mock-model", "mock", "mock", b"mock weights " + run.encode())
        ok(f"version {reg['version']['id']} ready, sha256 {reg['digest'][:12]}…")

    step("create a deployment through the gateway (202, with a placement decision)")
    code, dep = deploy(key, dep_name, reg["version"]["id"], 2)
    if code != 202 or not dep.get("placement", {}).get("admitted"):
        fail(f"{code} {dep}")
    dep_id = dep["id"]
    ok(f"202 Accepted; placement: {dep['placement']['capacity_note']}")

    def is_ready():  # type: ignore[no-untyped-def]
        s = status(key, dep_id)["status"]
        return s if s["state"] == "ready" else None
    st = wait("state ready", is_ready, timeout=300)
    ok(f"state ready, {st['ready_replicas']} ready replicas, reconciled={st['reconciled']}")
    ok(f"kubectl: {k8s_name} {k8s_ready(k8s_name)} ready")
    pods = kubectl("-n", WL, "get", "pods", "-l", f"nebula.dev/deployment-id={dep_id}",
                   "-o", "jsonpath={range .items[*]}{.metadata.name}@{.spec.nodeName} {end}")
    ok(f"pods: {pods}")

    step("inference through the gateway, served by those pods")
    put_route(key, "nebula-mock", [{"deployment": dep_name, "weight": 100, "label": "baseline"}])

    def served():  # type: ignore[no-untyped-def]
        c, o = api("POST", "/v1/chat/completions",
                   {"model": "nebula-mock", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 8}, key)
        return (c, o) if c == 200 else None
    code, out = wait("the gateway to pick up the route", served, timeout=30, every=0.5)
    if code != 200 or out["nebula"]["deployment"] != "mock-demo" or out["nebula"]["runtime"] != "mock":
        fail(f"{code} {out}")
    ok(f"200: {out['usage']['completion_tokens']} tokens from runtime {out['nebula']['runtime']}")

    step("kill a pod: Kubernetes replaces it and the deployment returns to ready")
    victim = pods.split()[0].split("@")[0]
    kubectl("-n", WL, "delete", "pod", victim, "--wait=false")
    wait("a replacement pod", lambda: victim not in kubectl(
        "-n", WL, "get", "pods", "-l", f"nebula.dev/deployment-id={dep_id}", "-o", "name") and
        k8s_ready(k8s_name) == "2/2")
    ok(f"{victim} replaced; {k8s_ready(k8s_name)} ready")

    step("delete the Deployment object out from under NEBULA: the controller recreates it")
    uid_before = kubectl("-n", WL, "get", "deploy", k8s_name, "-o", "jsonpath={.metadata.uid}")
    kubectl("-n", WL, "delete", "deploy", k8s_name, "--wait=true")
    t0 = time.time()
    wait("the Deployment to come back", lambda: kubectl(
        "-n", WL, "get", "deploy", k8s_name, "-o", "jsonpath={.metadata.uid}", check=False) not in ("", uid_before)
        and "NotFound" not in kubectl("-n", WL, "get", "deploy", k8s_name, check=False), every=0.5)
    ok(f"recreated in {time.time() - t0:.1f}s with a new uid")
    wait("replicas ready again", lambda: k8s_ready(k8s_name) == "2/2", timeout=240)
    ok("2/2 ready again")

    step("edit the Deployment by hand: the controller reverts the drift")
    kubectl("-n", WL, "scale", "deploy", k8s_name, "--replicas=5")
    wait("replicas reverted to 2", lambda: kubectl(
        "-n", WL, "get", "deploy", k8s_name, "-o", "jsonpath={.spec.replicas}") == "2", every=0.5)
    ok("spec.replicas back to 2")

    step("scale through the API: Kubernetes follows")
    code, out = api("POST", f"/v1/deployments/{dep_id}/scale", {"replicas": 3}, key)
    if code >= 300:
        fail(f"{code} {out}")
    wait("3 replicas", lambda: k8s_ready(k8s_name) == "3/3", timeout=240)
    wait("state ready at 3", lambda: status(key, dep_id)["status"]["ready_replicas"] == 3)
    ok("3/3 ready")

    step("an impossible deployment is refused with 422 before anything is created")
    code, out = deploy(key, "too-big-" + run, reg["version"]["id"], 1,
                       resources={"cpu_milli": 100, "memory_mib": 10_000_000})
    if code != 422 or out["error"]["code"] != "insufficient_capacity":
        fail(f"{code} {out}")
    if "NotFound" not in kubectl("-n", WL, "get", "deploy", f"nebula-dev-too-big-{run}", check=False):
        fail("an object was created for a refused deployment")
    ok(f"422: {out['error']['message'][:110]}…")

    step("RBAC: the controller can do its job and nothing else")
    sa = "system:serviceaccount:nebula-system:nebula-controller"
    checks = [
        ("yes", ["create", "deployments.apps", "-n", WL]), ("yes", ["delete", "services", "-n", WL]),
        ("yes", ["create", "poddisruptionbudgets.policy", "-n", WL]), ("yes", ["list", "nodes"]),
        ("yes", ["update", "leases.coordination.k8s.io", "-n", SYS]),
        ("no", ["get", "secrets", "-n", WL]), ("no", ["create", "pods/exec", "-n", WL]),
        ("no", ["update", "deployments.apps", "-n", SYS]), ("no", ["create", "deployments.apps", "-n", "kube-system"]),
        ("no", ["create", "clusterroles.rbac.authorization.k8s.io"]), ("no", ["patch", "nodes"]),
        ("no", ["create", "rolebindings.rbac.authorization.k8s.io", "-n", WL]),
    ]
    for want, args in checks:
        got = kubectl("auth", "can-i", *args, "--as", sa, check=False).split("\n")[0]
        if got != want:
            fail(f"can-i {' '.join(args)}: got {got}, want {want}")
    ok(f"{len(checks)} can-i assertions ({sum(w == 'no' for w, _ in checks)} negative)")
    for sa_name in ("nebula-controlplane", "nebula-gateway"):
        got = kubectl("auth", "can-i", "list", "pods", "-n", WL, "--as",
                      f"system:serviceaccount:nebula-system:{sa_name}", check=False).split("\n")[0]
        if got != "no":
            fail(f"{sa_name} can list pods")
    gw = "system:serviceaccount:nebula-system:nebula-gateway"
    for verb, res, ns, want in (("watch", "endpointslices.discovery.k8s.io", WL, "yes"),
                                ("list", "endpointslices.discovery.k8s.io", "kube-system", "no"),
                                ("get", "secrets", WL, "no")):
        got = kubectl("auth", "can-i", verb, res, "-n", ns, "--as", gw, check=False).split("\n")[0]
        if got != want:
            fail(f"gateway can-i {verb} {res} -n {ns}: got {got}, want {want}")
    ok("control plane: no Kubernetes access; gateway: reads EndpointSlices in the workload namespace, nothing else")

    step("network policy: a workload pod cannot reach PostgreSQL, but can reach the artifact store")
    probe = f"netprobe-{run}"

    last = {"out": ""}

    def reach(host: str, port: int, attempt: int = 0) -> bool:
        out = kubectl("-n", WL, "run", probe + f"-{port}-{attempt}", "--rm", "-i", "--restart=Never", "--quiet",
                      "--image=busybox:1.37", "--overrides",
                      json.dumps({"spec": {"securityContext": {"runAsNonRoot": True, "runAsUser": 65532,
                                                               "seccompProfile": {"type": "RuntimeDefault"}},
                                           "containers": [{"name": "p", "image": "busybox:1.37",
                                                           "command": ["sh", "-c", f"nc -z -w 3 {host} {port} && echo OPEN || echo CLOSED"],
                                                           "securityContext": {"allowPrivilegeEscalation": False,
                                                                               "capabilities": {"drop": ["ALL"]}}}]}}),
                      check=False)
        last["out"] = out
        return "OPEN" in out
    if reach("postgres.nebula-data", 5432):
        fail("a pod in nebula-workloads reached PostgreSQL")
    # The positive control retries: a brand-new pod's first connection can race the
    # CNI programming its policy. Only the "allowed" direction retries, so a retry can
    # never turn a leak into a pass.
    if not any(reach("s3.nebula-data", 8333, i) for i in range(4)):
        fail(f"the positive control failed: a workload pod could not reach the artifact store: {last['out'][-300:]}")
    ok("postgres:5432 CLOSED from nebula-workloads; s3:8333 OPEN (the negative is not vacuous)")

    step("tamper with a verified artifact: the puller refuses it and the deployment fails, named")
    reg2 = register(key, "tamper-" + run, "gguf", "llamacpp", gguf_header())
    creds = kubectl("-n", WL, "get", "secret", "nebula-artifact-store", "-o", "json")
    _ = creds  # the store is overwritten from inside the cluster, with the same credentials
    evil = gguf_header()[:-1] + b"X"   # same size, different bytes
    digest = reg2["digest"]
    # Overwrite through a presigned URL issued for the same content address: this is
    # what a compromised store or a bit flip looks like to the node.
    code, ver = api("POST", f"/v1/models/{reg2['model']['id']}/versions", {
        "version": "v2", "format": "gguf", "runtime": "llamacpp", "size_bytes": len(evil),
        "checksum_sha256": digest, "context_window": 4096}, key)
    if code != 201:
        fail(f"presign for tamper: {code} {ver}")
    code, _ = api("PUT", "", raw=evil, url=ver["upload"]["url"])
    if code != 200:
        fail(f"tampering upload: {code}")
    code, d2 = deploy(key, "tampered-" + run, reg2["version"]["id"], 1)
    if code != 202:
        fail(f"{code} {d2}")

    def failed():  # type: ignore[no-untyped-def]
        s = status(key, d2["id"])["status"]
        return s if s["state"] == "failed" else None
    s2 = wait("the tampered deployment to fail", failed, timeout=240)
    if not (s2.get("state_reason") or "").startswith("ArtifactChecksumMismatch"):
        fail(f"reason {s2.get('state_reason')}: {s2.get('state_message')}")
    ok(f"failed: {s2['state_reason']} — {(s2.get('state_message') or '')[:90]}…")

    step("stop and delete: pods go, then the objects")
    for d in (dep_id, d2["id"]):
        api("POST", f"/v1/deployments/{d}/stop", {}, key)
    wait("stopped", lambda: status(key, dep_id)["status"]["state"] == "stopped", timeout=180)
    ok("mock-demo stopped, pods gone: " + (kubectl("-n", WL, "get", "pods", "-l",
       f"nebula.dev/deployment-id={dep_id}", "-o", "name") or "none"))
    remove_route(key, "nebula-mock")
    # Read the history before deleting: a deleted deployment is gone from the API.
    _, trans = api("GET", f"/v1/deployments/{dep_id}/transitions?limit=100", key=key)
    code, out = api("DELETE", f"/v1/deployments/{dep_id}", key=key)
    if code >= 300:
        fail(f"delete: {code} {out}")
    wait("objects removed", lambda: "NotFound" in kubectl("-n", WL, "get", "deploy", k8s_name, check=False))
    ok("Kubernetes objects removed")

    print("\nstate history of mock-demo (from the database, attributed):")
    rows = trans.get("data", [])
    if not rows:
        fail(f"no transitions recorded: {trans}")
    for t in rows:
        print(f"   {t.get('from_state') or '-':>12} -> {t['to_state']:<12} {t.get('reason', ''):<24} {t.get('actor_type', '')}")
    print("\nPHASE 5 DEMO: all steps passed")


if __name__ == "__main__":
    main()
