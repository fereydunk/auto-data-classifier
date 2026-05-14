"""Tests for flink-scanner/scripts/generate_json_object.py.

The script fetches an Avro schema from SR and emits the per-field
KEY/VALUE pairs that get substituted into JSON_OBJECT(...) inside scan.sql.
We only test the pure functions (top_level_field_names + render_json_object) —
SR HTTP is the script's only side-effecting part and isn't exercised here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Add the script's parent dir so we can import it as a module.
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "flink-scanner" / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import generate_json_object as gen  # noqa: E402


def _body(schema_dict, schema_type="AVRO"):
    return {"schema": json.dumps(schema_dict), "schemaType": schema_type}


class TestTopLevelFieldNames:
    def test_flat_record(self):
        body = _body({
            "type": "record", "name": "F",
            "fields": [
                {"name": "ssn",   "type": "string"},
                {"name": "email", "type": "string"},
            ],
        })
        assert gen.top_level_field_names(body) == ["ssn", "email"]

    def test_preserves_field_order(self):
        body = _body({
            "type": "record", "name": "F",
            "fields": [{"name": f"f{i}", "type": "string"} for i in range(10)],
        })
        assert gen.top_level_field_names(body) == [f"f{i}" for i in range(10)]

    def test_nested_record_only_returns_top_level(self):
        body = _body({
            "type": "record", "name": "Top",
            "fields": [
                {"name": "outer1", "type": "string"},
                {"name": "outer2", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [{"name": "inner_a", "type": "string"}],
                }},
            ],
        })
        # top-level only — JSON_OBJECT is built from `p.<col>` references
        # and Flink columns reflect TOP-LEVEL Avro fields.
        assert gen.top_level_field_names(body) == ["outer1", "outer2"]

    def test_non_avro_raises(self):
        body = _body({"type": "object"}, schema_type="JSON")
        with pytest.raises(SystemExit, match="schemaType=JSON"):
            gen.top_level_field_names(body)

    def test_missing_schema_raises(self):
        with pytest.raises(SystemExit, match="missing"):
            gen.top_level_field_names({"schemaType": "AVRO"})

    def test_non_record_top_raises(self):
        body = _body({"type": "array", "items": "string"})
        with pytest.raises(SystemExit, match="not a record"):
            gen.top_level_field_names(body)

    def test_empty_fields_raises(self):
        body = _body({"type": "record", "name": "Empty", "fields": []})
        with pytest.raises(SystemExit, match="zero fields"):
            gen.top_level_field_names(body)


class TestRenderJsonObject:
    def test_single_field(self):
        out = gen.render_json_object(["ssn"])
        assert out == "                KEY 'ssn' VALUE p.`ssn`"

    def test_multiple_fields_comma_separated(self):
        out = gen.render_json_object(["a", "b", "c"])
        lines = out.split("\n")
        assert len(lines) == 3
        assert lines[0].endswith(",")
        assert lines[1].endswith(",")
        assert not lines[2].endswith(",")
        assert "KEY 'a' VALUE p.`a`" in lines[0]
        assert "KEY 'c' VALUE p.`c`" in lines[2]

    def test_indentation_matches_scan_sql(self):
        # 16 spaces — same indent the surrounding JSON_OBJECT(...) uses.
        out = gen.render_json_object(["x"])
        assert out.startswith(" " * 16 + "KEY")

    def test_quotes_field_name_with_underscores(self):
        out = gen.render_json_object(["customer_id", "billing_postal_code"])
        assert "KEY 'customer_id' VALUE p.`customer_id`" in out
        assert "KEY 'billing_postal_code' VALUE p.`billing_postal_code`" in out
