"""Smoke tests for the setup-wizard FastAPI app.

The wizard shells out to `confluent` and reads/writes files in the repo root
(`.env`, `flink-scanner/scan.env`). Tests mock `subprocess.run` so we never
touch the real CLI, and patch the file paths to tmp_path so we never clobber
the user's real config.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def wizard_module(tmp_path):
    """Import setup_wizard.main fresh per test, with file paths redirected."""
    repo = Path(__file__).resolve().parent.parent
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    sys.modules.pop("setup_wizard.main", None)
    sys.modules.pop("setup_wizard", None)
    mod = importlib.import_module("setup_wizard.main")
    # Redirect file paths so the real .env / scan.env are never touched
    mod.ENV_FILE       = tmp_path / ".env"
    mod.SCAN_ENV_FILE  = tmp_path / "flink-scanner" / "scan.env"
    return mod


@pytest.fixture()
def client(wizard_module):
    from fastapi.testclient import TestClient
    return TestClient(wizard_module.app)


def _fake_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    p = MagicMock()
    p.stdout, p.stderr, p.returncode = stdout, stderr, returncode
    return p


def test_root_serves_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Auto Data Classifier" in resp.text


def test_prereqs_returns_per_tool_status(wizard_module, client):
    """`confluent` present, `mvn`/`ngrok` missing — each returns a row."""
    def fake_run(cmd, **kw):
        if cmd[0] == "confluent":
            return _fake_completed(stdout="Version:     v3.85.0", returncode=0)
        # Every other tool: not found
        raise FileNotFoundError(cmd[0])

    with patch.object(wizard_module.subprocess, "run", side_effect=fake_run):
        resp = client.get("/prereqs")
    assert resp.status_code == 200
    rows = resp.json()
    assert isinstance(rows, list) and len(rows) >= 4
    by_name = {r["name"]: r for r in rows}
    assert by_name["Confluent CLI"]["ok"] is True
    assert by_name["Confluent CLI"]["blocking"] is True
    assert by_name["Maven (for JAR build)"]["ok"] is False
    assert by_name["ngrok (to expose classifier)"]["ok"] is False


def test_env_select_writes_both_files(wizard_module, client):
    """Happy-path POST writes the expected keys to .env and scan.env."""
    cluster_payload = [{
        "id": "lkc-abc123",
        "name": "demo-cluster",
        "endpoint": "pkc-test.us-east-2.aws.confluent.cloud:9092",
    }]
    pool_payload = [{"id": "lfcp-xyz789", "name": "demo-pool"}]
    sr_payload = {"id": "lsrc-srid", "endpoint": "https://psrc-test.us-east-2.aws.confluent.cloud"}
    kafka_key_payload = {"key": "KAFKA_KEY_NEW", "secret": "kafka-secret-new"}
    sr_key_payload = {"key": "SR_KEY_NEW", "secret": "sr-secret-new"}

    def fake_run(cmd, **kw):
        if cmd[:3] == ["confluent", "kafka", "cluster"]:
            return _fake_completed(stdout=json.dumps(cluster_payload), returncode=0)
        if cmd[:3] == ["confluent", "flink", "compute-pool"]:
            return _fake_completed(stdout=json.dumps(pool_payload), returncode=0)
        if cmd[:3] == ["confluent", "schema-registry", "cluster"]:
            return _fake_completed(stdout=json.dumps(sr_payload), returncode=0)
        if cmd[:3] == ["confluent", "api-key", "create"]:
            # Two calls — first for Kafka cluster, second for SR. Differentiate by --resource value.
            res_idx = cmd.index("--resource") + 1
            payload = sr_key_payload if cmd[res_idx].startswith("lsrc") else kafka_key_payload
            return _fake_completed(stdout=json.dumps(payload), returncode=0)
        return _fake_completed(returncode=1, stderr=f"unexpected cmd: {cmd!r}")

    with patch.object(wizard_module.subprocess, "run", side_effect=fake_run):
        resp = client.post("/cc/env/select", json={
            "env_id":             "env-test123",
            "env_name":           "test-env",
            "cluster_id":         "lkc-abc123",
            "flink_compute_pool": "lfcp-xyz789",
            "source_topic":       "raw-messages",
        })

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["summary"]["cloud"]  == "aws"
    assert body["summary"]["region"] == "us-east-2"

    env_text = wizard_module.ENV_FILE.read_text()
    assert "CONFLUENT_BOOTSTRAP_SERVERS=pkc-test.us-east-2.aws.confluent.cloud:9092" in env_text
    assert "CONFLUENT_API_KEY=KAFKA_KEY_NEW" in env_text
    assert "CONFLUENT_SR_API_KEY=SR_KEY_NEW" in env_text
    assert "CONFLUENT_SR_CLUSTER_ID=lsrc-srid" in env_text

    scan_text = wizard_module.SCAN_ENV_FILE.read_text()
    assert "CONFLUENT_ENVIRONMENT=env-test123" in scan_text
    assert "CONFLUENT_KAFKA_CLUSTER=lkc-abc123" in scan_text
    assert "CONFLUENT_COMPUTE_POOL=lfcp-xyz789" in scan_text
    assert "CONFLUENT_CLOUD_PROVIDER=aws" in scan_text
    assert "CONFLUENT_CLOUD_REGION=us-east-2" in scan_text


def test_upsert_env_values_preserves_existing_keys(wizard_module, tmp_path):
    """Writing one key shouldn't remove unrelated keys the user added by hand."""
    env = tmp_path / ".env"
    env.write_text("MAX_LAYER=2\nCUSTOM_FLAG=true\n")
    wizard_module._upsert_env_values(env, {"CONFLUENT_API_KEY": "MYKEY"})
    text = env.read_text()
    assert "MAX_LAYER=2" in text
    assert "CUSTOM_FLAG=true" in text
    assert "CONFLUENT_API_KEY=MYKEY" in text


def test_upsert_env_values_replaces_in_place(wizard_module, tmp_path):
    """Re-running the wizard should overwrite a key, not duplicate it."""
    env = tmp_path / ".env"
    env.write_text("CONFLUENT_API_KEY=OLD\nMAX_LAYER=3\n")
    wizard_module._upsert_env_values(env, {"CONFLUENT_API_KEY": "NEW"})
    text = env.read_text()
    assert "CONFLUENT_API_KEY=NEW" in text
    assert "OLD" not in text
    # Should still appear exactly once
    assert text.count("CONFLUENT_API_KEY=") == 1


# ── Iteration 2 — demo orchestration ────────────────────────────────────────

def test_demo_status_starts_idle(client):
    resp = client.get("/demo/status")
    assert resp.status_code == 200
    assert resp.json()["phase"] == "idle"


def test_demo_schema_built_at_default_size():
    """The wizard's dynamic schema builder defaults to DEFAULT_FIELD_COUNT."""
    import sys
    repo = Path(__file__).resolve().parent.parent
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from setup_wizard.schema_builder import DEFAULT_FIELD_COUNT, build_demo_schema
    schema, fields = build_demo_schema(DEFAULT_FIELD_COUNT)
    assert len(schema["fields"]) == DEFAULT_FIELD_COUNT
    assert len(fields) == DEFAULT_FIELD_COUNT
    # Field names must be unique within a single demo schema.
    names = [f.name for f in fields]
    assert len(names) == len(set(names))


def test_demo_schema_field_types_are_nullable_primitives():
    """Every field in a built schema is nullable string/int — anything else
    means somebody added a pool entry the producer won't tolerate."""
    import sys
    repo = Path(__file__).resolve().parent.parent
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from setup_wizard.schema_builder import build_demo_schema, MAX_FIELDS
    valid_inner = {"string", "int", "long", "double", "boolean"}
    schema, _ = build_demo_schema(MAX_FIELDS)  # exercise EVERY pool entry
    for f in schema["fields"]:
        t = f["type"]
        assert isinstance(t, list) and len(t) == 2 and t[0] == "null", f
        assert t[1] in valid_inner, f


def test_parse_kafka_region_handles_protocol_prefix(wizard_module):
    """`confluent kafka cluster describe` returns bootstraps with or without
    a SASL_SSL:// prefix depending on CLI version. The parser must handle both."""
    cases = [
        ("pkc-xxx.us-east-2.aws.confluent.cloud:9092",          ("aws", "us-east-2")),
        ("SASL_SSL://pkc-xxx.us-east-2.aws.confluent.cloud:9092", ("aws", "us-east-2")),
        ("pkc-yyy.eu-central-1.aws.confluent.cloud:9092",       ("aws", "eu-central-1")),
        ("",                                                     ("", "")),
        ("not-a-confluent-host:9092",                            ("", "")),
    ]
    for inp, expected in cases:
        assert wizard_module._parse_kafka_region(inp) == expected, inp


def test_field_pool_generators_return_strings_or_ints():
    """Every FieldSpec in the pool must have a sample(rng) callable that
    returns a value matching its declared Avro inner type."""
    import random
    import sys
    repo = Path(__file__).resolve().parent.parent
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from setup_wizard.field_pool import FIELD_POOL
    rng = random.Random(0)    # deterministic
    for spec in FIELD_POOL:
        v = spec.sample(rng)
        inner = spec.avro_type[1]  # ["null", "<inner>"]
        if inner == "string":
            assert isinstance(v, str), f"{spec.name} returned non-string: {v!r}"
            assert v, f"{spec.name} returned empty string"
        elif inner in ("int", "long"):
            assert isinstance(v, int), f"{spec.name} returned non-int: {v!r}"
        else:
            raise AssertionError(f"unexpected inner type for {spec.name}: {inner}")
