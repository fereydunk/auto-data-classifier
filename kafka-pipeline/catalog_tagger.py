"""Confluent wire-format helpers used by kafka-pipeline.

The previous CatalogTagger class in this module duplicated the Stream Catalog
tagging logic that now lives (canonically) in review-api/catalog_client.py.
The pipeline POSTs detections to review-api as recommendations; the human
reviewer pushes them to the catalog. The legacy in-pipeline tagger was
unreachable from pipeline.py and was removed.

Kept here: extract_schema_id_from_wire — the only function pipeline.py + the
test suite still import from this module.
"""
import struct
from typing import Optional


def extract_schema_id_from_wire(raw: bytes) -> Optional[int]:
    """Extract the schema ID from a Confluent wire-format message.

    Wire format: 0x00 (magic) | 4-byte big-endian schema ID | payload.
    Returns None if the message is not in Confluent wire format.
    """
    if len(raw) < 5 or raw[0] != 0x00:
        return None
    return struct.unpack(">I", raw[1:5])[0]
