"""
Tests for Confluent wire-format detection (magic byte + schema ID extraction).
No Kafka or Schema Registry connection required.
"""
import struct
import pytest
from catalog_tagger import extract_schema_id_from_wire


def _make_wire_message(schema_id: int, payload: bytes = b"{}") -> bytes:
    """Build a minimal Confluent wire-format message."""
    header = struct.pack(">bI", 0x00, schema_id)
    return header + payload


class TestExtractSchemaId:
    def test_valid_wire_format(self):
        msg = _make_wire_message(schema_id=42)
        assert extract_schema_id_from_wire(msg) == 42

    def test_large_schema_id(self):
        msg = _make_wire_message(schema_id=100_001)
        assert extract_schema_id_from_wire(msg) == 100_001

    def test_plain_json_returns_none(self):
        plain = b'{"customer": "John"}'
        assert extract_schema_id_from_wire(plain) is None

    def test_wrong_magic_byte_returns_none(self):
        bad_magic = struct.pack(">bI", 0x01, 99) + b"{}"
        assert extract_schema_id_from_wire(bad_magic) is None

    def test_too_short_returns_none(self):
        assert extract_schema_id_from_wire(b"\x00\x00") is None

    def test_empty_bytes_returns_none(self):
        assert extract_schema_id_from_wire(b"") is None

    def test_wire_format_with_schema_id_zero(self):
        msg = _make_wire_message(schema_id=0)
        # schema_id=0 is technically valid wire format
        assert extract_schema_id_from_wire(msg) == 0
