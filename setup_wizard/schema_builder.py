"""Build a fresh Avro demo schema at wizard runtime.

Pulls N field specs from setup_wizard.field_pool.FIELD_POOL with a balanced
spread across categories so every classifier layer sees something to detect.
The resulting schema is registered to SR; downstream code (producer, scanner,
classifier) MUST then refetch the canonical schema from SR by id and never
look at any local copy.
"""
from __future__ import annotations

import random
from typing import List

from setup_wizard.field_pool import FIELD_POOL, FieldSpec, by_category

DEFAULT_FIELD_COUNT = 20
MIN_FIELDS = 5
MAX_FIELDS = len(FIELD_POOL)

# When we have to pick N from the pool, prefer at least one field from each
# category in this list (in this order) so the demo always exercises the
# major recognizers. Anything beyond the priority quota is filled at random.
_CATEGORY_PRIORITY = [
    "PII", "GOVERNMENT_ID", "PCI", "FINANCIAL",
    "CREDENTIALS", "LOCATION", "PHI", "FREE_TEXT",
]


def select_fields(n_fields: int, rng: random.Random | None = None) -> List[FieldSpec]:
    """Pick `n_fields` distinct fields from FIELD_POOL.

    Strategy: round-robin one from each priority category until we hit n,
    then top up with random picks from whatever's left. Guarantees coverage
    when n >= len(_CATEGORY_PRIORITY); below that we still touch the most
    important categories first.
    """
    if n_fields < MIN_FIELDS or n_fields > MAX_FIELDS:
        raise ValueError(
            f"n_fields={n_fields} out of range [{MIN_FIELDS}, {MAX_FIELDS}]"
        )
    rng = rng or random.Random()

    grouped = by_category()
    chosen: list[FieldSpec] = []
    seen_names: set[str] = set()

    # Round 1: one from each priority category until we've placed n_fields.
    for cat in _CATEGORY_PRIORITY:
        if len(chosen) >= n_fields:
            break
        bucket = [f for f in grouped.get(cat, []) if f.name not in seen_names]
        if bucket:
            pick = rng.choice(bucket)
            chosen.append(pick)
            seen_names.add(pick.name)

    # Round 2: top up at random from the remaining pool.
    remaining = [f for f in FIELD_POOL if f.name not in seen_names]
    rng.shuffle(remaining)
    while len(chosen) < n_fields and remaining:
        chosen.append(remaining.pop())

    return chosen


def build_schema(fields: List[FieldSpec]) -> dict:
    """Wrap a list of field specs into a complete Avro record schema dict."""
    return {
        "type":      "record",
        "name":      "DemoRecord",
        "namespace": "io.confluent.scanner.demo",
        "doc":       f"Auto-generated demo schema with {len(fields)} fields.",
        "fields": [
            {"name": f.name, "type": f.avro_type, "default": None}
            for f in fields
        ],
    }


def build_demo_schema(n_fields: int, rng: random.Random | None = None) -> tuple[dict, List[FieldSpec]]:
    """Convenience: select N fields, build the Avro schema dict, return both.

    Returns (schema_dict, fields) so the caller can register the schema AND
    keep the spec list around to drive sample-data generation.
    """
    fields = select_fields(n_fields, rng)
    return build_schema(fields), fields
