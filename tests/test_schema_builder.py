"""Tests for setup_wizard.schema_builder — the dynamic Avro schema generator.

The wizard composes a fresh Avro schema at every demo run by sampling N
fields from FIELD_POOL. These tests prove:
  - the builder respects the requested field count
  - it rejects invalid counts
  - the resulting Avro JSON is well-formed (parseable by fastavro)
  - field names are unique within a single schema
  - priority categories are covered when N >= len(_CATEGORY_PRIORITY)
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

# setup_wizard isn't on sys.path by default in tests/conftest.py — add the
# repo root so `import setup_wizard.schema_builder` works.
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from setup_wizard.schema_builder import (  # noqa: E402
    DEFAULT_FIELD_COUNT, MAX_FIELDS, MIN_FIELDS, build_demo_schema, select_fields,
)
from setup_wizard.field_pool import FIELD_POOL, CATEGORIES  # noqa: E402


class TestSelectFields:
    def test_default_count(self):
        chosen = select_fields(DEFAULT_FIELD_COUNT, random.Random(0))
        assert len(chosen) == DEFAULT_FIELD_COUNT

    def test_min_count_works(self):
        chosen = select_fields(MIN_FIELDS, random.Random(0))
        assert len(chosen) == MIN_FIELDS

    def test_max_count_works(self):
        chosen = select_fields(MAX_FIELDS, random.Random(0))
        assert len(chosen) == MAX_FIELDS
        # MAX_FIELDS == every entry in the pool — names match exactly.
        assert {f.name for f in chosen} == {f.name for f in FIELD_POOL}

    def test_below_min_raises(self):
        with pytest.raises(ValueError):
            select_fields(MIN_FIELDS - 1)

    def test_above_max_raises(self):
        with pytest.raises(ValueError):
            select_fields(MAX_FIELDS + 1)

    def test_field_names_unique(self):
        chosen = select_fields(20, random.Random(42))
        names = [f.name for f in chosen]
        assert len(names) == len(set(names))

    def test_priority_categories_covered_when_n_large_enough(self):
        # Picking 8+ fields should ALWAYS hit each of the 8 priority categories
        # (PII, GOVERNMENT_ID, PCI, FINANCIAL, CREDENTIALS, LOCATION, PHI, FREE_TEXT).
        chosen = select_fields(8, random.Random(0))
        cats = {f.category for f in chosen}
        for required in ("PII", "GOVERNMENT_ID", "PCI", "FINANCIAL",
                         "CREDENTIALS", "LOCATION", "PHI", "FREE_TEXT"):
            assert required in cats, f"missing required category: {required}"

    def test_random_seed_is_deterministic(self):
        a = [f.name for f in select_fields(15, random.Random(123))]
        b = [f.name for f in select_fields(15, random.Random(123))]
        assert a == b


class TestBuildDemoSchema:
    def test_returns_schema_and_fields(self):
        schema, fields = build_demo_schema(10, random.Random(0))
        assert schema["type"] == "record"
        assert "fields" in schema
        assert len(schema["fields"]) == 10
        assert len(fields) == 10

    def test_schema_is_valid_avro(self):
        """fastavro.parse_schema raises on invalid Avro — best regression test."""
        import fastavro
        schema, _ = build_demo_schema(MAX_FIELDS, random.Random(0))
        parsed = fastavro.parse_schema(schema)
        assert parsed is not None

    def test_every_field_is_nullable(self):
        schema, _ = build_demo_schema(MAX_FIELDS, random.Random(0))
        for f in schema["fields"]:
            t = f["type"]
            assert isinstance(t, list) and t[0] == "null", f
            assert f["default"] is None

    def test_namespace_is_set(self):
        schema, _ = build_demo_schema(5, random.Random(0))
        assert schema["namespace"] == "io.confluent.scanner.demo"

    def test_doc_includes_field_count(self):
        schema, _ = build_demo_schema(7, random.Random(0))
        assert "7" in schema["doc"]

    def test_field_specs_carry_generators(self):
        """The returned FieldSpec list is what the producer uses to make
        sample data — every spec must have a callable .sample."""
        rng = random.Random(0)
        _, specs = build_demo_schema(15, rng)
        for spec in specs:
            v = spec.sample(rng)
            assert v is not None


class TestPoolHealth:
    """Sanity checks on FIELD_POOL itself — not the builder, but cheap to bundle here."""

    def test_pool_is_non_trivial(self):
        assert len(FIELD_POOL) >= 30, "pool too small to give a meaningful demo"

    def test_no_duplicate_field_names_in_pool(self):
        names = [f.name for f in FIELD_POOL]
        assert len(names) == len(set(names)), "duplicate field name in FIELD_POOL"

    def test_priority_categories_have_entries(self):
        from setup_wizard.field_pool import by_category
        grouped = by_category()
        for cat in ("PII", "GOVERNMENT_ID", "PCI", "FINANCIAL",
                    "CREDENTIALS", "LOCATION", "PHI", "FREE_TEXT"):
            assert grouped.get(cat), f"no entries for required category {cat}"

    def test_categories_cover_every_classifier_tag(self):
        # The classifier emits these 11 tags; the pool can lack a 1:1 mapping
        # for some (e.g. NPI/MINOR are weakly demonstrated) but it should at
        # least include the major ones.
        major_tags = {"PII", "PCI", "PHI", "CREDENTIALS", "FINANCIAL",
                      "GOVERNMENT_ID", "LOCATION"}
        assert major_tags.issubset(set(CATEGORIES))
