"""Auto Data Classifier setup wizard.

Single-page web UI on http://localhost:8002 that walks the user through
prerequisite checks → CC sign-in → environment selection. Saves selections
to repo-root `.env` and `flink-scanner/scan.env` so the existing demo
scripts (`flink-scanner/scripts/start_scan.sh`, `e2e/produce_test_data.py`)
can run without manual config editing.

Iteration 1 scope: cards 1-3 (prereqs / login / env-select). Card 4 is a
placeholder for the demo orchestration that arrives in iteration 2.

Endpoints:
  GET  /                      — HTML wizard page
  GET  /prereqs               — local-tool checks (confluent, mvn, ngrok, docker, python)
  GET  /cc/status             — {user, has_cloud_key}
  POST /cc/login              — {email, password} → `confluent login --save` + auto-mint key
  GET  /cc/environments       — `confluent environment list -o json`
  GET  /cc/env/{id}/details   — clusters + flink pools + SR for one env (one round-trip)
  POST /cc/env/select         — write .env + flink-scanner/scan.env from selection
  GET  /demo/status           — iteration-1 stub
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("setup-wizard")

REPO_ROOT      = Path(__file__).resolve().parent.parent
ENV_FILE       = REPO_ROOT / ".env"
SCAN_ENV_FILE  = REPO_ROOT / "flink-scanner" / "scan.env"
EXAMPLE_ENV    = REPO_ROOT / ".env.example"
STATIC_DIR     = Path(__file__).resolve().parent / "static"
INDEX_HTML     = STATIC_DIR / "index.html"


# ──────────────────────────────────────────────────────────────────────────────
# Confluent CLI helpers (ported from openlineage-confluent/web/server.py)
# ──────────────────────────────────────────────────────────────────────────────

def _run_confluent(args: list[str], env: dict | None = None,
                   timeout: int = 30) -> tuple[int, str, str]:
    """Run `confluent <args>` and return (rc, stdout, stderr)."""
    try:
        proc = subprocess.run(
            ["confluent", *args],
            env={**os.environ, **(env or {})},
            capture_output=True, text=True, timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except FileNotFoundError:
        return 127, "", "`confluent` CLI not found in PATH"
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout}s"


def _run_confluent_json(args: list[str], timeout: int = 30) -> tuple[bool, object, str]:
    """Run `confluent ... -o json` and parse the response."""
    rc, out, err = _run_confluent(args, timeout=timeout)
    if rc != 0:
        return False, None, (err or out).strip()[:300]
    try:
        return True, json.loads(out), ""
    except json.JSONDecodeError as exc:
        return False, None, f"invalid JSON from `confluent {' '.join(args[:3])}`: {exc}"


def _current_user() -> str:
    """Return the currently logged-in CC user email, or '' if not logged in."""
    rc, out, _ = _run_confluent(["context", "list", "-o", "json"], timeout=8)
    if rc != 0:
        return ""
    try:
        for ctx in json.loads(out):
            if ctx.get("is_current"):
                m = re.match(r"login-([^-]+@[^-]+)-", ctx.get("name", ""))
                if m:
                    return m.group(1)
                return ctx.get("name", "")
    except Exception:
        pass
    return ""


def _list_environments() -> list[dict]:
    rc, out, _ = _run_confluent(["environment", "list", "-o", "json"], timeout=15)
    if rc != 0:
        return []
    try:
        envs = json.loads(out)
    except json.JSONDecodeError:
        return []
    return [{"id": e.get("id", ""), "name": e.get("name", "")}
            for e in envs if e.get("id")]


def _list_clusters(env_id: str) -> tuple[list[dict], str]:
    ok, data, err = _run_confluent_json(
        ["kafka", "cluster", "list", "--environment", env_id, "-o", "json"]
    )
    if not ok or not isinstance(data, list):
        return [], err or "no clusters returned"
    return data, ""


def _list_flink_pools(env_id: str) -> tuple[list[dict], str]:
    ok, data, err = _run_confluent_json(
        ["flink", "compute-pool", "list", "--environment", env_id, "-o", "json"]
    )
    if not ok or not isinstance(data, list):
        return [], err or "flink pool list failed"
    return data, ""


def _describe_sr(env_id: str) -> tuple[dict, str]:
    """SR endpoint + lsrc-id for an env, normalised across CLI versions."""
    ok, data, err = _run_confluent_json(
        ["schema-registry", "cluster", "describe",
         "--environment", env_id, "-o", "json"]
    )
    if not ok:
        return {}, err or "SR cluster describe failed"
    if not isinstance(data, dict):
        return {}, "unexpected SR JSON shape"
    if data:
        data["id"]       = data.get("id") or data.get("cluster") or ""
        data["endpoint"] = data.get("endpoint") or data.get("endpoint_url") or ""
    return data, ""


def _probe_cloud_api_key(key: str, secret: str) -> bool:
    """Return True if (key, secret) successfully authenticates against the
    Cloud API. Hits /iam/v2/users — cheap, requires only Cloud-scope basic auth."""
    if not (key and secret):
        return False
    req = urllib.request.Request(
        "https://api.confluent.cloud/iam/v2/users?page_size=1",
        headers={
            "Authorization": "Basic " + base64.b64encode(
                f"{key}:{secret}".encode()
            ).decode(),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as exc:
        return exc.code == 200
    except Exception:    # noqa: BLE001 — network/DNS/timeout — treat as broken
        return False


def _create_cloud_api_key() -> tuple[dict, str]:
    """Mint a fresh cloud-scope API key under the logged-in user."""
    ok, data, err = _run_confluent_json(
        ["api-key", "create",
         "--resource", "cloud",
         "--description", "auto-data-classifier (auto-minted by wizard)",
         "-o", "json"],
        timeout=60,
    )
    if not ok or not isinstance(data, dict):
        return {}, err or "api-key create failed"
    key    = data.get("key")    or data.get("api_key")
    secret = data.get("secret") or data.get("api_secret")
    if not (key and secret):
        return {}, f"mint returned no key/secret: {data}"
    return {"api_key": key, "api_secret": secret}, ""


def _create_resource_api_key(resource_id: str, env_id: str,
                             description: str) -> tuple[dict, str]:
    """Mint a resource-scoped API key (Kafka cluster lkc-... or SR lsrc-...)."""
    ok, data, err = _run_confluent_json(
        ["api-key", "create",
         "--resource", resource_id,
         "--environment", env_id,
         "--description", description,
         "-o", "json"],
        timeout=60,
    )
    if not ok or not isinstance(data, dict):
        return {}, err or "api-key create failed"
    key    = data.get("key")    or data.get("api_key")
    secret = data.get("secret") or data.get("api_secret")
    if not (key and secret):
        return {}, f"mint returned no key/secret: {data}"
    return {"api_key": key, "api_secret": secret}, ""


# ──────────────────────────────────────────────────────────────────────────────
# .env file helpers (KEY=VALUE format)
# ──────────────────────────────────────────────────────────────────────────────

def _read_env_value(path: Path, key: str) -> str:
    """Return value for `key` in a KEY=VALUE file. '' if missing or file absent.

    IMPORTANT: uses `[ \\t]` rather than `\\s` around `=` because `\\s` matches
    newlines too — so for an empty value like `CONFLUENT_CLOUD_PROVIDER=` the
    `\\s*` after `=` would greedily consume the trailing newline AND the
    contents of the next line, returning the wrong value.
    """
    if not path.exists():
        return ""
    pat = re.compile(rf'^[ \t]*{re.escape(key)}[ \t]*=[ \t]*([^\n]*)$', re.M)
    m = pat.search(path.read_text())
    if not m:
        return ""
    val = m.group(1).strip()
    # Strip surrounding quotes if present
    if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
        val = val[1:-1]
    return val


def _upsert_env_values(path: Path, updates: dict[str, str], *,
                       header: str = "") -> None:
    """Set each KEY=VALUE in `path`, preserving every other line + comments.

    Existing keys get their value replaced in place. Missing keys are appended
    at the end. Bootstraps the file with `header` (a top-of-file comment) +
    nothing else if the file doesn't exist.
    """
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header)

    text = path.read_text()
    for key, raw_val in updates.items():
        # Quote any value containing whitespace or special chars
        val = raw_val if re.match(r'^[A-Za-z0-9_./:@-]*$', raw_val) else f'"{raw_val}"'
        new_line = f'{key}={val}'
        # Same `[ \t]` (not `\s`) reasoning as _read_env_value — avoid matching
        # across line boundaries when the existing value is empty.
        pat = re.compile(rf'^[ \t]*{re.escape(key)}[ \t]*=[^\n]*$', re.M)
        if pat.search(text):
            text = pat.sub(new_line, text)
        else:
            if not text.endswith("\n"):
                text += "\n"
            text += new_line + "\n"
    path.write_text(text)


# ──────────────────────────────────────────────────────────────────────────────
# Cloud key persistence — auto-mint on login, store in .env
# ──────────────────────────────────────────────────────────────────────────────

def _ensure_cloud_api_key() -> tuple[bool, str]:
    """Make sure .env has a working Cloud-scope API key.

    Probes the existing key (if any) against api.confluent.cloud. If 401 or
    no key, mints a fresh one under the logged-in user's account. The minted
    key inherits ALL the user's roles automatically — sign in as Org Admin
    and you get MetricsViewer + CloudClusterAdmin transitively.

    Idempotent: a working key is left untouched.
    """
    existing_key    = _read_env_value(ENV_FILE, "CONFLUENT_CLOUD_API_KEY")
    existing_secret = _read_env_value(ENV_FILE, "CONFLUENT_CLOUD_API_SECRET")
    if existing_key and existing_secret and _probe_cloud_api_key(existing_key, existing_secret):
        return True, f"existing Cloud API key {existing_key[:8]}… works"

    minted, err = _create_cloud_api_key()
    if not minted:
        return False, f"failed to mint Cloud API key: {err}"
    _upsert_env_values(ENV_FILE, {
        "CONFLUENT_CLOUD_API_KEY":    minted["api_key"],
        "CONFLUENT_CLOUD_API_SECRET": minted["api_secret"],
    })
    return True, f"minted fresh Cloud API key {minted['api_key'][:8]}…"


# ──────────────────────────────────────────────────────────────────────────────
# Prerequisite checks
# ──────────────────────────────────────────────────────────────────────────────

def _check_tool(cmd: list[str], name: str, *,
                blocking: bool = False) -> dict:
    """Run `cmd` and treat success as "tool present"."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
        if proc.returncode == 0:
            detail = (proc.stdout or proc.stderr).strip().splitlines()
            return {"name": name, "ok": True,
                    "blocking": blocking,
                    "detail": detail[0] if detail else "installed"}
        return {"name": name, "ok": False, "blocking": blocking,
                "detail": (proc.stderr or proc.stdout).strip()[:120]}
    except FileNotFoundError:
        return {"name": name, "ok": False, "blocking": blocking,
                "detail": "not found in PATH"}
    except subprocess.TimeoutExpired:
        return {"name": name, "ok": False, "blocking": blocking,
                "detail": "timeout"}


def _check_prereqs() -> list[dict]:
    """Check every external tool the wizard / demo will need."""
    checks: list[dict] = [
        _check_tool(["confluent", "version"], "Confluent CLI", blocking=True),
        _check_tool(["mvn",  "--version"],    "Maven (for JAR build)",       blocking=False),
        _check_tool(["ngrok", "version"],     "ngrok (to expose classifier)", blocking=False),
        _check_tool(["docker", "--version"],  "Docker (optional, for compose)", blocking=False),
    ]
    # Python venv check — peek at .venv/bin/python rather than spawning a process
    venv_py = REPO_ROOT / ".venv" / "bin" / "python"
    checks.append({
        "name":     "Project Python venv (.venv)",
        "ok":       venv_py.exists(),
        "blocking": True,
        "detail":   str(venv_py) if venv_py.exists() else f"missing — run: python3 -m venv {REPO_ROOT}/.venv",
    })
    return checks


# ──────────────────────────────────────────────────────────────────────────────
# Env-file writers — populate .env + flink-scanner/scan.env from a selection
# ──────────────────────────────────────────────────────────────────────────────

def _parse_kafka_region(bootstrap: str) -> tuple[str, str]:
    """Pull (cloud, region) out of a CC bootstrap host.

    `confluent kafka cluster describe` returns endpoints in either form:
        pkc-xxx.us-east-2.aws.confluent.cloud:9092
        SASL_SSL://pkc-xxx.us-east-2.aws.confluent.cloud:9092
    Strip the protocol prefix before splitting on `:` so the host parser
    sees the real hostname rather than `SASL_SSL`.
    """
    if not bootstrap:
        return "", ""
    if "://" in bootstrap:
        bootstrap = bootstrap.split("://", 1)[1]
    host = bootstrap.split(":")[0]
    parts = host.split(".")
    # ["pkc-xxx", "us-east-2", "aws", "confluent", "cloud"]
    if len(parts) >= 4 and parts[-2] == "confluent" and parts[-1] == "cloud":
        return parts[-3], parts[-4]
    return "", ""


def _write_demo_config(*, env_id: str, env_name: str,
                       cluster: dict, flink_pool: dict,
                       sr: dict, sr_key_secret: tuple[str, str],
                       kafka_key_secret: tuple[str, str],
                       source_topic: str) -> dict:
    """Populate both .env and flink-scanner/scan.env from one selection."""
    bootstrap   = cluster.get("endpoint", "") or cluster.get("bootstrap", "") or ""
    cloud, region = _parse_kafka_region(bootstrap)
    cluster_id  = cluster.get("id", "")
    pool_id     = flink_pool.get("id", "")
    sr_url      = sr.get("endpoint", "")
    sr_id       = sr.get("id", "")
    kafka_key, kafka_secret = kafka_key_secret
    sr_user_key, sr_user_secret = sr_key_secret

    # .env — read by docker-compose, kafka-pipeline/config.py, review-api
    _upsert_env_values(ENV_FILE, {
        "CONFLUENT_BOOTSTRAP_SERVERS": bootstrap,
        "CONFLUENT_API_KEY":           kafka_key,
        "CONFLUENT_API_SECRET":        kafka_secret,
        "CONFLUENT_SR_URL":            sr_url,
        "CONFLUENT_SR_API_KEY":        sr_user_key,
        "CONFLUENT_SR_API_SECRET":     sr_user_secret,
        "CONFLUENT_SR_CLUSTER_ID":     sr_id,
        "SOURCE_TOPIC":                source_topic,
        "REVIEW_API_URL":              "http://localhost:8001",
    }, header="# Written by setup-wizard. Edit MAX_LAYER / BATCH_SIZE / etc. by hand.\n")

    # flink-scanner/scan.env — read by flink-scanner/scripts/*.sh
    _upsert_env_values(SCAN_ENV_FILE, {
        "CONFLUENT_ENVIRONMENT":    env_id,
        "CONFLUENT_KAFKA_CLUSTER":  cluster_id,
        "CONFLUENT_COMPUTE_POOL":   pool_id,
        "CONFLUENT_CLOUD_PROVIDER": cloud,
        "CONFLUENT_CLOUD_REGION":   region,
        "SOURCE_TOPIC":             source_topic,
        "SR_URL":                   sr_url,
        "SR_KEY":                   sr_user_key,
        "SR_SECRET":                sr_user_secret,
        # Demo defaults — user can override after wizard run
        "CLASSIFIER_MAX_LAYER":     "3",
        "SCAN_INTERVAL_MINUTES":    "60",
        "SAMPLE_WINDOW_MINUTES":    "2",
    }, header="# Written by setup-wizard. CLASSIFIER_URL gets set when ngrok starts (iter 2).\n")

    return {
        "env_file":      str(ENV_FILE),
        "scan_env_file": str(SCAN_ENV_FILE),
        "summary": {
            "env_id":      env_id,
            "env_name":    env_name,
            "cluster_id":  cluster_id,
            "pool_id":     pool_id,
            "cloud":       cloud,
            "region":      region,
            "sr_id":       sr_id,
            "topic":       source_topic,
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI app
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="auto-data-classifier setup wizard",
    version="0.1.0",
)


@app.get("/", include_in_schema=False)
async def ui_root():
    return FileResponse(INDEX_HTML, media_type="text/html")


@app.get("/prereqs")
async def prereqs():
    return _check_prereqs()


@app.get("/cc/status")
async def cc_status():
    user = _current_user()
    has_key = bool(_read_env_value(ENV_FILE, "CONFLUENT_CLOUD_API_KEY"))
    return {"user": user, "has_cloud_key": has_key}


class CCLoginRequest(BaseModel):
    email:    str
    password: str


@app.post("/cc/login")
async def cc_login(req: CCLoginRequest):
    if not req.email or not req.password:
        raise HTTPException(status_code=400, detail="email + password required")
    rc, _, err = _run_confluent(
        ["login", "--save"],
        env={"CONFLUENT_CLOUD_EMAIL": req.email, "CONFLUENT_CLOUD_PASSWORD": req.password},
        timeout=30,
    )
    if rc != 0:
        msg = err.strip().splitlines()[0] if err.strip() else "login failed"
        raise HTTPException(status_code=401, detail=msg[:200])
    user = _current_user() or req.email
    ok, key_msg = _ensure_cloud_api_key()
    return {"user": user, "cloud_key": key_msg, "cloud_key_ok": ok}


@app.get("/cc/environments")
async def cc_environments():
    return _list_environments()


@app.get("/cc/env/{env_id}/details")
async def cc_env_details(env_id: str):
    """One-shot bundle: clusters + flink pools + SR endpoint for an env."""
    clusters, c_err = _list_clusters(env_id)
    pools,    p_err = _list_flink_pools(env_id)
    sr,       s_err = _describe_sr(env_id)
    return {
        "clusters":        clusters,
        "clusters_error":  c_err,
        "flink_pools":     pools,
        "pools_error":     p_err,
        "schema_registry": sr,
        "sr_error":        s_err,
    }


class EnvSelection(BaseModel):
    env_id:             str
    env_name:           Optional[str] = ""
    cluster_id:         str
    flink_compute_pool: str
    source_topic:       str = "raw-messages"


@app.post("/cc/env/select")
async def cc_env_select(sel: EnvSelection):
    """Mint Kafka + SR keys, write .env + flink-scanner/scan.env."""
    # Look up the cluster details for endpoint / cloud / region
    clusters, err = _list_clusters(sel.env_id)
    cluster = next((c for c in clusters if c.get("id") == sel.cluster_id), None)
    if cluster is None:
        raise HTTPException(status_code=400, detail=f"cluster {sel.cluster_id} not found in env {sel.env_id}")

    pools, _ = _list_flink_pools(sel.env_id)
    pool = next((p for p in pools if p.get("id") == sel.flink_compute_pool), None)
    if pool is None:
        raise HTTPException(status_code=400, detail=f"flink pool {sel.flink_compute_pool} not found in env {sel.env_id}")

    sr, sr_err = _describe_sr(sel.env_id)
    if not sr or not sr.get("id"):
        raise HTTPException(
            status_code=400,
            detail=f"Schema Registry not enabled in env {sel.env_id}: {sr_err}. "
                   "Enable SR via the Cloud UI before continuing.",
        )

    # Mint Kafka cluster + SR API keys
    kafka_keys, k_err = _create_resource_api_key(
        sel.cluster_id, sel.env_id, "auto-data-classifier (kafka)"
    )
    if k_err:
        raise HTTPException(status_code=502, detail=f"Kafka API key mint failed: {k_err}")

    sr_keys, s_err = _create_resource_api_key(
        sr["id"], sel.env_id, "auto-data-classifier (schema registry)"
    )
    if s_err:
        raise HTTPException(status_code=502, detail=f"SR API key mint failed: {s_err}")

    return _write_demo_config(
        env_id=sel.env_id,
        env_name=sel.env_name or "",
        cluster=cluster,
        flink_pool=pool,
        sr=sr,
        sr_key_secret=(sr_keys["api_key"], sr_keys["api_secret"]),
        kafka_key_secret=(kafka_keys["api_key"], kafka_keys["api_secret"]),
        source_topic=sel.source_topic or "raw-messages",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Demo orchestration (Card 4 — Start AI; Card 5 — Run test demo)
# ──────────────────────────────────────────────────────────────────────────────

# Source topic for the wizard demo. Hardcoded — keeps the demo
# self-contained and unique from any user-managed topics in the env.
DEMO_SOURCE_TOPIC = "customer-profiles-demo"

CLASSIFIER_DIR     = REPO_ROOT / "classifier-service"
FLINK_SCANNER_DIR  = REPO_ROOT / "flink-scanner"
FLINK_TARGET_GLOB  = "flink-scanner-udf-*.jar"

CLASSIFIER_PORT    = 8000
NGROK_API_URL      = "http://127.0.0.1:4040/api/tunnels"
HEALTH_POLL_TIMEOUT_S = 120


_demo_lock = threading.Lock()
_demo_state: dict = {
    "phase":             "idle",
    "current_card":      0,        # 4 or 5 while a worker is active; 0 otherwise
    "classifier_proc":   None,
    "ngrok_proc":        None,
    "bridge_proc":       None,
    "ngrok_url":         "",
    "flink_connection":  False,
    "jar_built":         False,
    "udfs_registered":   False,
    "topic_created":     False,
    "schema_id":         None,
    "messages_produced": 0,
    "scan_running":      False,
    # Each entry is {"card": int, "line": str}. Card 0 = global / unattributed
    # (pre-worker logs, /demo/stop, etc.).
    "log":               deque(maxlen=2000),
    "started_at":        None,
}


# ── Logging ──────────────────────────────────────────────────────────────────

def _demo_emit(line: str, *, card: int | None = None) -> None:
    """Append one line to the demo log ring buffer + stdlib logger.

    Lines are tagged with a card number so each card's UI panel can filter
    to only its own lines (no more duplication across panels). If `card` is
    not passed, falls back to whichever card the active worker has set.
    """
    ts = time.strftime("%H:%M:%S")
    with _demo_lock:
        c = card if card is not None else _demo_state["current_card"]
        _demo_state["log"].append({"card": c, "line": f"[{ts}] {line}"})
    log.info("demo[card%d]: %s", c, line)


def _demo_state_snapshot() -> dict:
    """Return a JSON-serialisable view of state (drops Popen handles)."""
    with _demo_lock:
        return {
            "phase":             _demo_state["phase"],
            "ngrok_url":         _demo_state["ngrok_url"],
            "flink_connection":  _demo_state["flink_connection"],
            "jar_built":         _demo_state["jar_built"],
            "udfs_registered":   _demo_state["udfs_registered"],
            "topic_created":     _demo_state["topic_created"],
            "schema_id":         _demo_state["schema_id"],
            "messages_produced": _demo_state["messages_produced"],
            "scan_running":      _demo_state["scan_running"],
            "started_at":        _demo_state["started_at"],
            "classifier_alive":  _proc_alive(_demo_state["classifier_proc"]),
            "ngrok_alive":       _proc_alive(_demo_state["ngrok_proc"]),
            "bridge_alive":      _proc_alive(_demo_state["bridge_proc"]),
        }


def _proc_alive(proc) -> bool:
    return proc is not None and proc.poll() is None


# ── Card 4 step 1: classifier-service ────────────────────────────────────────

def _classifier_health() -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{CLASSIFIER_PORT}/health", timeout=3) as r:
            return r.status == 200 and b'"ok"' in r.read()
    except Exception:    # noqa: BLE001
        return False


def _start_classifier_service() -> None:
    if _proc_alive(_demo_state["classifier_proc"]) or _classifier_health():
        _demo_emit(f"classifier-service already up at :{CLASSIFIER_PORT}")
        return

    venv_uvicorn = REPO_ROOT / ".venv" / "bin" / "uvicorn"
    if not venv_uvicorn.exists():
        raise RuntimeError(f".venv/bin/uvicorn not found — install with: "
                           f"{REPO_ROOT}/.venv/bin/pip install -r classifier-service/requirements.txt")

    _demo_emit("starting classifier-service on :8000 (GLiNER loads on first start, ~60s) …")
    log_path = REPO_ROOT / "scripts" / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [str(venv_uvicorn), "main:app", "--host", "0.0.0.0", "--port", str(CLASSIFIER_PORT)],
        cwd=str(CLASSIFIER_DIR),
        env={**os.environ, "PYTHONPATH": str(CLASSIFIER_DIR)},
        stdout=open(log_path / "classifier.log", "ab"),
        stderr=subprocess.STDOUT,
    )
    _demo_state["classifier_proc"] = proc

    # Poll /health
    deadline = time.time() + HEALTH_POLL_TIMEOUT_S
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"classifier-service exited (rc={proc.returncode}) — see scripts/logs/classifier.log")
        if _classifier_health():
            _demo_emit(f"classifier-service ready at http://localhost:{CLASSIFIER_PORT}")
            return
        time.sleep(3)
    raise RuntimeError("classifier-service did not become healthy in time — check scripts/logs/classifier.log")


# ── Card 4 step 2: ngrok tunnel ──────────────────────────────────────────────

def _ngrok_url_from_api() -> str:
    """Return the public https URL from ngrok's local API, or '' if none."""
    try:
        with urllib.request.urlopen(NGROK_API_URL, timeout=3) as r:
            data = json.loads(r.read())
        for t in data.get("tunnels", []):
            url = t.get("public_url", "")
            if url.startswith("https://") and "8000" in t.get("config", {}).get("addr", ""):
                return url
    except Exception:    # noqa: BLE001
        pass
    return ""


def _start_ngrok() -> None:
    # If something is already on the ngrok API and points at 8000, reuse it.
    existing = _ngrok_url_from_api()
    if existing:
        _demo_state["ngrok_url"] = existing
        _demo_emit(f"ngrok already running: {existing}")
        return

    if not shutil.which("ngrok"):
        raise RuntimeError("ngrok not in PATH — install with: brew install ngrok/ngrok/ngrok")

    _demo_emit("starting ngrok http 8000 …")
    log_path = REPO_ROOT / "scripts" / "logs" / "ngrok.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        ["ngrok", "http", str(CLASSIFIER_PORT), "--log=stdout"],
        stdout=open(log_path, "ab"),
        stderr=subprocess.STDOUT,
    )
    _demo_state["ngrok_proc"] = proc

    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"ngrok exited (rc={proc.returncode}) — see scripts/logs/ngrok.log")
        url = _ngrok_url_from_api()
        if url:
            _demo_state["ngrok_url"] = url
            _demo_emit(f"ngrok tunnel: {url}")
            return
        time.sleep(1)
    raise RuntimeError("ngrok started but no tunnel URL appeared — see scripts/logs/ngrok.log")


# ── Card 4 step 3: Flink connection ──────────────────────────────────────────

# Confluent Flink connections of type 'rest' REQUIRE auth credentials, even
# when the endpoint behind them doesn't enforce auth. Our demo classifier-
# service has no auth check, so the basic-auth header gets sent and ignored.
# Production deployments should put real auth on the classifier and use
# proper credentials here. Treat the ngrok URL itself as the only effective
# guard during the demo window.
_DEMO_CONNECTION_USER     = "demo"
_DEMO_CONNECTION_PASSWORD = "demo"


def _flink_connection_endpoint(env_id: str, cloud: str, region: str) -> str | None:
    """Return the connection's current endpoint, or None if it doesn't exist.

    The describe response shape varies across CLI versions; we look for the
    endpoint under common field names rather than asserting a single one.
    """
    rc, out, _ = _run_confluent(
        ["flink", "connection", "describe", "classifier-service",
         "--environment", env_id, "--cloud", cloud, "--region", region, "-o", "json"],
        timeout=15,
    )
    if rc != 0:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return ""
    if isinstance(data, list):
        data = data[0] if data else {}
    return data.get("endpoint") or data.get("Endpoint") or ""


def _ensure_flink_connection(env_id: str, cloud: str, region: str, url: str) -> None:
    """Make sure the 'classifier-service' Flink REST connection points at `url`.

    `confluent flink connection update` does NOT accept `--endpoint` — only
    secrets can be updated. So when the ngrok URL changes between sessions
    (the free tier rotates the subdomain), we delete + recreate.
    """
    if not (cloud and region):
        raise RuntimeError("cloud/region not set — re-run Card 3 to populate scan.env")

    existing = _flink_connection_endpoint(env_id, cloud, region)
    if existing == url:
        _demo_emit(f"Flink connection 'classifier-service' already points at {url}")
        _demo_state["flink_connection"] = True
        return
    if existing is not None:
        _demo_emit(f"Flink connection endpoint differs ({existing!r} → {url!r}); deleting + recreating")
        rc, _, err = _run_confluent(
            ["flink", "connection", "delete", "classifier-service",
             "--environment", env_id, "--cloud", cloud, "--region", region, "--force"],
            timeout=20,
        )
        if rc != 0:
            raise RuntimeError(f"flink connection delete failed: {err.strip()[:300]}")

    _demo_emit(f"creating Flink connection 'classifier-service' → {url}")
    rc, _, err = _run_confluent(
        ["flink", "connection", "create", "classifier-service",
         "--type", "rest", "--endpoint", url,
         "--username", _DEMO_CONNECTION_USER,
         "--password", _DEMO_CONNECTION_PASSWORD,
         "--environment", env_id, "--cloud", cloud, "--region", region],
        timeout=30,
    )
    if rc != 0:
        raise RuntimeError(f"flink connection create failed: {err.strip()[:300]}")
    _demo_state["flink_connection"] = True


# ── Card 4 step 4: JAR build ─────────────────────────────────────────────────

def _jar_present() -> bool:
    target = FLINK_SCANNER_DIR / "target"
    if not target.exists():
        return False
    return any(target.glob(FLINK_TARGET_GLOB))


def _build_jar() -> None:
    if _jar_present():
        _demo_emit("JAR already built — reusing")
        _demo_state["jar_built"] = True
        return
    _demo_emit("building flink-scanner JAR (this takes ~30-60s) …")
    rc = _exec_streaming(["bash", str(FLINK_SCANNER_DIR / "scripts" / "build.sh")],
                         cwd=str(FLINK_SCANNER_DIR))
    if rc != 0:
        raise RuntimeError(f"build.sh exited rc={rc}")
    _demo_state["jar_built"] = True
    _demo_emit("JAR built")


# ── Card 4 step 5: UDF registration ──────────────────────────────────────────

def _udfs_registered(env_id: str, cloud: str, region: str) -> bool:
    """Return True iff classify_fields/schema_watcher/apply_tag exist in this env."""
    rc, out, _ = _run_confluent(
        ["flink", "statement", "list",
         "--environment", env_id, "--cloud", cloud, "--region", region, "-o", "json"],
        timeout=20,
    )
    if rc != 0:
        return False
    try:
        statements = json.loads(out)
    except json.JSONDecodeError:
        return False
    needed = {"classify_fields", "schema_watcher", "apply_tag"}
    for stmt in statements:
        sql = (stmt.get("statement") or "").lower()
        for fn in list(needed):
            if f"function {fn}" in sql or f"function `{fn}`" in sql:
                needed.discard(fn)
    return not needed


def _register_udfs(env_id: str, cloud: str, region: str) -> None:
    if _udfs_registered(env_id, cloud, region):
        _demo_emit("UDFs already registered — reusing")
        _demo_state["udfs_registered"] = True
        return
    _demo_emit("registering UDFs (uploads JAR + creates 3 functions) …")
    rc = _exec_streaming(
        ["bash", str(FLINK_SCANNER_DIR / "scripts" / "register.sh")],
        cwd=str(FLINK_SCANNER_DIR),
        # register.sh reads env vars — load them from scan.env file
        env_overrides=_load_scan_env(),
    )
    if rc != 0:
        raise RuntimeError(f"register.sh exited rc={rc}")
    _demo_state["udfs_registered"] = True
    _demo_emit("UDFs registered")


# ── Card 5 step 1: clean-slate reset of topic + SR subject ───────────────────
#
# Each demo run starts fresh: any prior topic is deleted (so messages with
# now-stale schema_ids don't pollute the scanner) and the SR subject is
# permanently deleted (so the next register starts at version 1 with a
# brand-new schema_id). Both are idempotent — first-run is a no-op delete.

def _delete_demo_subject() -> None:
    """Permanently delete the SR subject for the demo topic (if it exists).

    Two-step (soft → hard) per Confluent SR docs:
      DELETE /subjects/{subject}                   → soft delete (versions hidden)
      DELETE /subjects/{subject}?permanent=true    → hard delete (frees subject)
    A 404 on either step means the subject was already gone — that's fine.
    """
    sr_url    = _read_env_value(ENV_FILE, "CONFLUENT_SR_URL")
    sr_key    = _read_env_value(ENV_FILE, "CONFLUENT_SR_API_KEY")
    sr_secret = _read_env_value(ENV_FILE, "CONFLUENT_SR_API_SECRET")
    if not (sr_url and sr_key and sr_secret):
        raise RuntimeError("Schema Registry creds missing in .env — re-run Card 3")

    subject = f"{DEMO_SOURCE_TOPIC}-value"
    base = sr_url.rstrip("/")
    auth_b64 = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    headers = {"Authorization": f"Basic {auth_b64}"}

    def _delete(url: str) -> tuple[int, str]:
        try:
            req = urllib.request.Request(url, headers=headers, method="DELETE")
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode()[:200]

    # Soft delete first.
    s_code, s_body = _delete(f"{base}/subjects/{subject}")
    if s_code == 404:
        _demo_emit(f"SR subject {subject} not present (clean start)")
        return
    if s_code >= 400:
        raise RuntimeError(f"SR soft-delete failed for {subject}: HTTP {s_code}: {s_body}")

    # Hard delete (frees the subject namespace).
    h_code, h_body = _delete(f"{base}/subjects/{subject}?permanent=true")
    if h_code >= 400 and h_code != 404:
        raise RuntimeError(f"SR hard-delete failed for {subject}: HTTP {h_code}: {h_body}")
    _demo_emit(f"SR subject {subject} deleted (soft+hard)")


def _wipe_review_recommendations() -> None:
    """Best-effort: delete prior recommendations for the demo topic from
    review-api so the UI matches the freshly-built schema.

    Idempotent — review-api may not be running yet (wizard launches it
    elsewhere); a connection failure is logged but doesn't fail the demo.
    """
    review_url = os.environ.get("REVIEW_API_URL", "http://localhost:8001")
    try:
        req = urllib.request.Request(
            f"{review_url.rstrip('/')}/recommendations?topic={DEMO_SOURCE_TOPIC}",
            method="DELETE",
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        _demo_emit(f"review-api: wiped {data.get('deleted', 0)} prior rec(s) for {DEMO_SOURCE_TOPIC}")
    except urllib.error.URLError as exc:
        _demo_emit(f"warn: could not reach review-api at {review_url} ({exc}) — continuing")
    except Exception as exc:    # noqa: BLE001
        _demo_emit(f"warn: review-api wipe failed: {exc} — continuing")


def _create_demo_topic() -> None:
    """Drop + recreate the demo Kafka topic for a clean run.

    Why drop: if a prior run produced messages bound to a now-deleted
    schema_id, the Flink scanner can't decode them. Starting with an empty
    topic (and a fresh schema) keeps the scanner unambiguous.
    """
    cluster_id = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_KAFKA_CLUSTER")
    env_id     = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_ENVIRONMENT")

    # Step 0: drop prior recommendations for this topic from review-api so
    # the review UI matches the schema we're about to build (no stale rows
    # carrying field names from a previous run's random schema pick).
    _wipe_review_recommendations()

    # Step 1: SR subject. Do this FIRST so we don't briefly have a topic
    # without a schema while creating downstream consumers.
    _delete_demo_subject()

    # Step 2: delete topic (idempotent — ignore "not found").
    rc_del, _, err_del = _run_confluent(
        ["kafka", "topic", "delete", DEMO_SOURCE_TOPIC, "--force",
         "--cluster", cluster_id, "--environment", env_id],
        timeout=20,
    )
    if rc_del == 0:
        _demo_emit(f"topic {DEMO_SOURCE_TOPIC} deleted")
        # Brokers need a moment to free the topic before recreate succeeds.
        time.sleep(3)
    elif "not found" in err_del.lower() or "does not exist" in err_del.lower():
        _demo_emit(f"topic {DEMO_SOURCE_TOPIC} not present (clean start)")
    else:
        # Don't fail hard — try to create anyway.
        _demo_emit(f"warn: topic delete returned {err_del.strip()[:200]} — continuing")

    # Step 3: create fresh topic.
    rc, _, err = _run_confluent(
        ["kafka", "topic", "create", DEMO_SOURCE_TOPIC,
         "--partitions", "6",
         "--cluster", cluster_id, "--environment", env_id],
        timeout=20,
    )
    combined = err.lower()
    if rc == 0 or "already exists" in combined:
        _demo_state["topic_created"] = True
        _demo_emit(f"topic {DEMO_SOURCE_TOPIC} ready")
        return
    raise RuntimeError(f"topic create failed: {err.strip()[:300]}")


# ── Card 5 step 2: schema register ───────────────────────────────────────────

def _register_demo_schema() -> int:
    """Build a fresh demo schema, register it to SR, fetch the canonical
    version BACK from SR by id, and stash everything needed downstream.

    SR is the source of truth: after this returns, every other step
    (producer, scanner, classifier) MUST refer to SR for the schema
    body — never to the locally-built dict.

    The number of fields is governed by FIELD_COUNT (env var, default
    DEFAULT_FIELD_COUNT). Set by startup.sh's interactive prompt.
    """
    from setup_wizard.schema_builder import (
        DEFAULT_FIELD_COUNT, MAX_FIELDS, MIN_FIELDS, build_demo_schema,
    )

    sr_url    = _read_env_value(ENV_FILE, "CONFLUENT_SR_URL")
    sr_key    = _read_env_value(ENV_FILE, "CONFLUENT_SR_API_KEY")
    sr_secret = _read_env_value(ENV_FILE, "CONFLUENT_SR_API_SECRET")
    if not (sr_url and sr_key and sr_secret):
        raise RuntimeError("Schema Registry creds missing in .env — re-run Card 3")

    # Pick N from the field pool, build the Avro schema dict.
    # Source of truth for the field count: the value the user entered in the
    # Card 5 UI input, captured by /demo/start-test into _demo_state. Fall
    # back to FIELD_COUNT env var (legacy / non-UI invocations), then default.
    try:
        n_fields = int(_demo_state.get("field_count")
                       or os.environ.get("FIELD_COUNT")
                       or DEFAULT_FIELD_COUNT)
    except (TypeError, ValueError):
        n_fields = DEFAULT_FIELD_COUNT
    n_fields = max(MIN_FIELDS, min(MAX_FIELDS, n_fields))
    schema_dict, fields = build_demo_schema(n_fields)
    _demo_emit(f"built demo schema with {len(fields)} fields ({', '.join(f.name for f in fields[:6])}{'…' if len(fields) > 6 else ''})")

    subject = f"{DEMO_SOURCE_TOPIC}-value"
    base = sr_url.rstrip('/')
    auth_b64 = base64.b64encode(f"{sr_key}:{sr_secret}".encode()).decode()
    headers_post = {
        "Content-Type":  "application/vnd.schemaregistry.v1+json",
        "Authorization": f"Basic {auth_b64}",
    }
    headers_get = {"Authorization": f"Basic {auth_b64}"}

    # POST → register
    req = urllib.request.Request(
        f"{base}/subjects/{subject}/versions",
        data=json.dumps({"schema": json.dumps(schema_dict)}).encode(),
        headers=headers_post,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"SR register failed: HTTP {exc.code}: {exc.read().decode()[:300]}") from exc

    sid = int(data.get("id"))

    # GET → fetch the canonical schema BACK from SR by id. From here on we
    # use this value, not the local schema_dict — proves SR is authoritative.
    try:
        with urllib.request.urlopen(
            urllib.request.Request(f"{base}/schemas/ids/{sid}", headers=headers_get),
            timeout=15,
        ) as r:
            sr_canonical = json.loads(r.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"SR canonical fetch failed: HTTP {exc.code}: {exc.read().decode()[:300]}") from exc

    canonical_schema_str = sr_canonical.get("schema")
    if not canonical_schema_str:
        raise RuntimeError(f"SR returned id={sid} but no 'schema' field — cannot proceed")

    _demo_state["schema_id"]      = sid
    _demo_state["schema_str"]     = canonical_schema_str
    _demo_state["field_specs"]    = fields  # generators only — schema body comes from SR
    _demo_emit(f"schema {subject} registered (id={sid}); fetched canonical body back from SR")
    return sid


# ── Card 5 step 3: produce test data ─────────────────────────────────────────

def _produce_test_messages(count: int = 50) -> None:
    """Produce N test messages using the SR-canonical schema (NOT a local copy).

    The encoder schema comes from `_demo_state["schema_str"]` — populated by
    `_register_demo_schema()` with the body fetched back from SR after
    registration. The local schema_dict that was POSTed to SR is discarded.

    Sample data comes from the per-field `sample(rng)` callables stored in
    `_demo_state["field_specs"]` — those are generators, not schema.
    """
    try:
        import fastavro
        from confluent_kafka import Producer
    except ImportError:
        raise RuntimeError("confluent-kafka or fastavro not installed — "
                           ".venv/bin/pip install confluent-kafka fastavro")

    bootstrap     = _read_env_value(ENV_FILE, "CONFLUENT_BOOTSTRAP_SERVERS")
    kafka_key     = _read_env_value(ENV_FILE, "CONFLUENT_API_KEY")
    kafka_secret  = _read_env_value(ENV_FILE, "CONFLUENT_API_SECRET")
    sid           = _demo_state.get("schema_id")
    schema_str    = _demo_state.get("schema_str")
    field_specs   = _demo_state.get("field_specs")
    if not sid or not schema_str or not field_specs:
        raise RuntimeError("schema_id / canonical schema / field specs missing — "
                           "register the schema first")

    # Parse the SR-canonical Avro schema for the writer. Anything that comes
    # out of fastavro from here on reflects SR's view of the schema.
    parsed = fastavro.parse_schema(json.loads(schema_str))

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     kafka_key,
        "sasl.password":     kafka_secret,
    })

    import random
    rng = random.Random()
    _demo_emit(f"producing {count} test messages using SR-canonical schema (id={sid}) …")

    # Keep records sparse — leave roughly half the fields null per row so the
    # classifier sees a mix of present/absent fields, like real traffic.
    for i in range(count):
        rec = {}
        for spec in field_specs:
            if rng.random() < 0.5:
                rec[spec.name] = spec.sample(rng)
            else:
                rec[spec.name] = None
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", sid))
        fastavro.schemaless_writer(buf, parsed, rec)
        producer.produce(DEMO_SOURCE_TOPIC, buf.getvalue())
        if (i + 1) % 10 == 0:
            producer.poll(0)
    producer.flush(timeout=15)
    # Accumulate across multiple produce waves in one demo run (e.g. the
    # wave1+trigger+wave2 flow), not overwrite. Reset happens at demo start
    # via _create_demo_topic() (topic is dropped + recreated → 0 msgs).
    _demo_state["messages_produced"] = (
        int(_demo_state.get("messages_produced") or 0) + count
    )
    _demo_emit(f"produced {count} messages")


# ── Card 5 step 4-6: scan + bridge ───────────────────────────────────────────

def _start_scan() -> None:
    # start_scan.sh sources scan.env, which clobbers any env vars we set on
    # the subprocess. Pin SOURCE_TOPIC to the wizard's demo topic by writing
    # it into scan.env first. (Card 3's "source topic" input lives in .env
    # for the streaming kafka-pipeline use case — different scenario.)
    _upsert_env_values(SCAN_ENV_FILE, {"SOURCE_TOPIC": DEMO_SOURCE_TOPIC})

    # Same clean-slate principle as topic+subject: each demo run gets fresh
    # scan statements built from the CURRENT SR schema. Old statements from
    # a prior run reference field names that may no longer exist (the schema
    # is rebuilt per run), so they FAIL — and submit_statement skips re-create
    # when the name still exists. Stop them first so start creates fresh ones.
    rc_stop = _exec_streaming(
        ["bash", str(FLINK_SCANNER_DIR / "scripts" / "start_scan.sh"), "--stop"],
        cwd=str(FLINK_SCANNER_DIR),
        env_overrides=_load_scan_env(),
    )
    if rc_stop != 0:
        _demo_emit(f"warn: scan --stop exited rc={rc_stop} — continuing")
    # Brief pause so deletes propagate before the create attempts.
    time.sleep(3)

    # start_scan.sh defaults to "start" mode with no args; passing "start"
    # as a literal arg is rejected by its case statement.
    rc = _exec_streaming(
        ["bash", str(FLINK_SCANNER_DIR / "scripts" / "start_scan.sh")],
        cwd=str(FLINK_SCANNER_DIR),
        env_overrides=_load_scan_env(),
    )
    if rc != 0:
        raise RuntimeError(f"start_scan.sh exited rc={rc}")
    _demo_state["scan_running"] = True
    _demo_emit("Flink scan statements running")


def _wait_for_scan_driver_running(timeout_s: int = 180) -> bool:
    """Poll the scan-driver Flink statement until it transitions to RUNNING.

    Confluent Cloud Flink statements typically take 30–60 s to leave PENDING.
    Firing the manual trigger before then means the trigger row is consumed
    by a not-yet-active driver — the interval-join window misses it and
    nothing flows to scan-results.

    Returns True once RUNNING; False on timeout or terminal failure.
    """
    cluster_id = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_KAFKA_CLUSTER")  # noqa: F841 — used implicitly via env
    env_id   = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_ENVIRONMENT")
    cloud    = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_PROVIDER") or "aws"
    region   = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_REGION") or "us-east-1"
    name = f"{DEMO_SOURCE_TOPIC}-scan-driver"

    deadline = time.time() + timeout_s
    last_status = ""
    while time.time() < deadline:
        rc, out, _ = _run_confluent(
            ["flink", "statement", "describe", name,
             "--environment", env_id, "--cloud", cloud, "--region", region,
             "--output", "json"],
            timeout=20,
        )
        if rc == 0:
            try:
                status = json.loads(out).get("status", "")
            except (ValueError, TypeError):
                status = ""
            if status != last_status:
                _demo_emit(f"scan-driver status: {status or 'unknown'}")
                last_status = status
            if status == "RUNNING":
                return True
            if status in ("FAILED", "STOPPED", "DEGRADED"):
                _demo_emit(f"scan-driver entered terminal state {status} — aborting wait")
                return False
        time.sleep(5)
    _demo_emit(f"timed out waiting for scan-driver RUNNING after {timeout_s}s")
    return False


def _fire_manual_scan() -> None:
    """Trigger a one-shot scan immediately so the user gets results in ~30s."""
    rc = _exec_streaming(
        ["bash", str(FLINK_SCANNER_DIR / "scripts" / "start_scan.sh"), "--now"],
        cwd=str(FLINK_SCANNER_DIR),
        env_overrides=_load_scan_env(),
    )
    if rc != 0:
        _demo_emit(f"WARNING: manual scan trigger failed (rc={rc}) — scheduled trigger will fire on its interval")
        return
    _demo_emit("manual scan triggered — recommendations will appear in ~30s")


def _start_results_bridge() -> None:
    if _proc_alive(_demo_state["bridge_proc"]):
        _demo_emit("results bridge already running")
        return
    venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    log_path = REPO_ROOT / "scripts" / "logs" / "results-bridge.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [str(venv_python), "-m", "setup_wizard.results_bridge",
         "--topic", DEMO_SOURCE_TOPIC],
        cwd=str(REPO_ROOT),
        stdout=open(log_path, "ab"),
        stderr=subprocess.STDOUT,
    )
    _demo_state["bridge_proc"] = proc
    _demo_emit(f"results bridge started (pid {proc.pid}, log scripts/logs/results-bridge.log)")


# ── Subprocess + scan-env helpers ────────────────────────────────────────────

def _load_scan_env() -> dict[str, str]:
    """Parse flink-scanner/scan.env into a dict for subprocess env."""
    if not SCAN_ENV_FILE.exists():
        return {}
    out = {}
    for line in SCAN_ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
            v = v[1:-1]
        out[k.strip()] = v
    return out


def _exec_streaming(cmd: list[str], *, cwd: str | None = None,
                    env_overrides: dict[str, str] | None = None) -> int:
    """Run a subprocess and tee its stdout/stderr into the demo log."""
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env={**os.environ, **(env_overrides or {})},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if proc.stdout is not None:
        for line in proc.stdout:
            _demo_emit(line.rstrip())
    proc.wait()
    return proc.returncode


# ── Background workers (Cards 4 + 5) ─────────────────────────────────────────

def _ai_worker():
    """Card 4: classifier + ngrok + flink connection + jar + UDFs."""
    with _demo_lock:
        _demo_state["phase"] = "starting_ai"
        _demo_state["current_card"] = 4
        _demo_state["started_at"] = time.time()
    try:
        env_id = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_ENVIRONMENT")
        cloud  = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_PROVIDER")
        region = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_REGION")
        if not (env_id and cloud and region):
            raise RuntimeError("scan.env not populated — complete Card 3 first")

        _start_classifier_service()
        _start_ngrok()
        _ensure_flink_connection(env_id, cloud, region, _demo_state["ngrok_url"])
        _build_jar()
        _register_udfs(env_id, cloud, region)

        with _demo_lock:
            _demo_state["phase"] = "ai_ready"
        _demo_emit("✓ Card 4 complete — AI model + Flink UDFs ready")
    except Exception as exc:    # noqa: BLE001
        with _demo_lock:
            _demo_state["phase"] = "error"
        _demo_emit(f"ERROR (Card 4): {exc}")
    finally:
        with _demo_lock:
            _demo_state["current_card"] = 0


def _demo_worker():
    """Card 5: topic + schema + test data + scan + bridge."""
    with _demo_lock:
        if _demo_state["phase"] not in ("ai_ready", "demo_running"):
            _demo_emit("ERROR: Card 4 must complete first", card=5)
            return
        _demo_state["phase"] = "starting_demo"
        _demo_state["current_card"] = 5
    try:
        # Reset per-run counters that accumulate across waves.
        _demo_state["messages_produced"] = 0
        _create_demo_topic()
        _register_demo_schema()
        # Submit scan statements against the (empty) topic FIRST, then wait
        # for scan-driver to be RUNNING, THEN produce messages, THEN trigger.
        # If we produce first, the wait can take 60+s and our messages fall
        # outside the 2-minute interval-join window when the trigger fires.
        _start_scan()
        _start_results_bridge()
        if not _wait_for_scan_driver_running():
            _demo_emit("warn: firing manual trigger anyway — results may be empty")
        # Produce in two waves around the trigger:
        #   wave 1 → primary batch INSIDE the [T-2min, T] interval-join window
        #   trigger
        #   wave 2 → small follow-on batch with rowtime > T to advance the
        #            source watermark past T, which is what Flink needs before
        #            it can emit the join result. Without wave 2 the join
        #            stays blocked until something else writes to the source
        #            topic — and the user sees 0 recommendations forever.
        _produce_test_messages(count=40)
        time.sleep(3)
        _fire_manual_scan()
        time.sleep(2)
        _produce_test_messages(count=10)

        with _demo_lock:
            _demo_state["phase"] = "demo_running"
        _demo_emit("✓ Card 5 complete — demo running. Open Review UI to approve.")
    except Exception as exc:    # noqa: BLE001
        with _demo_lock:
            _demo_state["phase"] = "error"
        _demo_emit(f"ERROR (Card 5): {exc}")
    finally:
        with _demo_lock:
            _demo_state["current_card"] = 0


def _stop_all() -> dict:
    """Kill every wizard-spawned subprocess + best-effort delete scan statements."""
    killed: list[str] = []
    for name in ("classifier_proc", "ngrok_proc", "bridge_proc"):
        proc = _demo_state.get(name)
        if _proc_alive(proc):
            try:
                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=8)
                killed.append(name)
            except Exception as exc:    # noqa: BLE001
                _demo_emit(f"warning: failed to stop {name}: {exc}")
        _demo_state[name] = None

    # Best-effort: stop the 3 long-running Flink scan statements
    env_id = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_ENVIRONMENT")
    cloud  = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_PROVIDER")
    region = _read_env_value(SCAN_ENV_FILE, "CONFLUENT_CLOUD_REGION")
    if env_id and cloud and region:
        for stmt in (
            f"{DEMO_SOURCE_TOPIC}-scan-trigger-scheduled",
            f"{DEMO_SOURCE_TOPIC}-scan-trigger-schema",
            f"{DEMO_SOURCE_TOPIC}-scan-driver",
        ):
            _run_confluent(
                ["flink", "statement", "delete", stmt,
                 "--environment", env_id, "--cloud", cloud, "--region", region, "--force"],
                timeout=15,
            )

    with _demo_lock:
        _demo_state["phase"] = "idle"
        _demo_state["ngrok_url"] = ""
        _demo_state["scan_running"] = False
    _demo_emit(f"stopped: {', '.join(killed) or '(nothing was running)'}")
    return {"killed": killed}


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/demo/start-ai")
async def demo_start_ai():
    with _demo_lock:
        if _demo_state["phase"] in ("starting_ai", "starting_demo"):
            raise HTTPException(status_code=409, detail=f"already in phase {_demo_state['phase']}")
    threading.Thread(target=_ai_worker, daemon=True).start()
    return {"status": "starting"}


@app.post("/demo/start-test")
async def demo_start_test(field_count: int = 20):
    """Kick off Card 5. The field_count picked in the UI determines how many
    fields the dynamic demo schema will have (range enforced 5..MAX_FIELDS)."""
    from setup_wizard.schema_builder import MAX_FIELDS, MIN_FIELDS
    if not (MIN_FIELDS <= field_count <= MAX_FIELDS):
        raise HTTPException(
            status_code=400,
            detail=f"field_count must be between {MIN_FIELDS} and {MAX_FIELDS}, got {field_count}",
        )
    with _demo_lock:
        if _demo_state["phase"] in ("starting_ai", "starting_demo"):
            raise HTTPException(status_code=409, detail=f"already in phase {_demo_state['phase']}")
        if _demo_state["phase"] not in ("ai_ready", "demo_running"):
            raise HTTPException(status_code=400, detail="run /demo/start-ai first (Card 4)")
        _demo_state["field_count"] = field_count
    threading.Thread(target=_demo_worker, daemon=True).start()
    return {"status": "starting", "field_count": field_count}


@app.post("/demo/stop")
async def demo_stop():
    return _stop_all()


@app.get("/demo/status")
async def demo_status():
    return _demo_state_snapshot()


@app.get("/demo/log")
async def demo_log():
    """Return the current full log buffer as structured entries.
    Each entry: {card: int, line: str}. card=0 means unattributed/global."""
    with _demo_lock:
        return {"entries": list(_demo_state["log"])}


@app.get("/demo/stream")
async def demo_stream():
    """SSE — emits each new log entry as `data: <card>|<line>`."""
    async def gen():
        last_idx = 0
        while True:
            with _demo_lock:
                cur = list(_demo_state["log"])
            if len(cur) < last_idx:
                last_idx = 0
            new_entries = cur[last_idx:]
            last_idx = len(cur)
            for entry in new_entries:
                # `card|line` — single pipe separator, line itself never
                # contains a leading "N|" pattern in practice.
                yield f"data: {entry['card']}|{entry['line']}\n\n"
            await asyncio.sleep(0.5)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/demo/recommendations-count")
async def demo_recommendations_count():
    """Proxy to review-api so the wizard UI doesn't need CORS to poll across ports."""
    review_url = _read_env_value(ENV_FILE, "REVIEW_API_URL") or "http://localhost:8001"
    try:
        with urllib.request.urlopen(f"{review_url}/recommendations?status=PENDING", timeout=5) as r:
            data = json.loads(r.read())
        return {"count": len(data) if isinstance(data, list) else 0,
                "review_url": review_url}
    except Exception as exc:    # noqa: BLE001
        return {"count": 0, "review_url": review_url, "error": str(exc)[:120]}
