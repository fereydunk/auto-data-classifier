"""
Tests for sr_schema.extract_field_paths — the Avro field-path extractor.

The extractor walks an SR /subjects/{s}/versions/{v} response body and
returns the set of dotted paths matching the convention used by
catalog_client._field_qualified_name (after [→.] / strip(".")).
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx

from sr_schema import (
    extract_field_paths,
    fetch_schema_body,
    fetch_field_paths_cached,
    _clear_cache_for_tests,
)


def _body(schema_dict, schema_type="AVRO"):
    """Wrap an Avro schema dict in the SR /subjects/.../versions/v response shape."""
    return {
        "subject": "test-value",
        "version": 1,
        "id": 100,
        "schema": json.dumps(schema_dict),
        "schemaType": schema_type,
    }


class TestExtractFieldPaths:
    def test_empty_body_returns_empty(self):
        assert extract_field_paths(None) == set()
        assert extract_field_paths({}) == set()

    def test_flat_record(self):
        schema = {
            "type": "record",
            "name": "Foo",
            "fields": [
                {"name": "ssn",   "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "age",   "type": "int"},
            ],
        }
        assert extract_field_paths(_body(schema)) == {"ssn", "email", "age"}

    def test_nested_record_two_levels(self):
        schema = {
            "type": "record", "name": "Order", "fields": [
                {"name": "customer", "type": {
                    "type": "record", "name": "Customer", "fields": [
                        {"name": "email", "type": "string"},
                        {"name": "ssn",   "type": "string"},
                    ],
                }},
                {"name": "ordertime", "type": "long"},
            ],
        }
        assert extract_field_paths(_body(schema)) == {
            "customer", "customer.email", "customer.ssn", "ordertime",
        }

    def test_three_level_nesting(self):
        schema = {
            "type": "record", "name": "A", "fields": [
                {"name": "b", "type": {"type": "record", "name": "B", "fields": [
                    {"name": "c", "type": {"type": "record", "name": "C", "fields": [
                        {"name": "d", "type": "string"},
                    ]}},
                ]}},
            ],
        }
        paths = extract_field_paths(_body(schema))
        assert "b.c.d" in paths
        assert "b.c"   in paths
        assert "b"     in paths

    def test_nullable_union(self):
        # Avro nullable: ["null", record-type]
        schema = {
            "type": "record", "name": "U", "fields": [
                {"name": "address", "type": ["null", {
                    "type": "record", "name": "Address", "fields": [
                        {"name": "city",    "type": "string"},
                        {"name": "zipcode", "type": "string"},
                    ],
                }]},
            ],
        }
        paths = extract_field_paths(_body(schema))
        assert paths == {"address", "address.city", "address.zipcode"}

    def test_array_of_record(self):
        schema = {
            "type": "record", "name": "Cart", "fields": [
                {"name": "items", "type": {
                    "type": "array", "items": {
                        "type": "record", "name": "Item", "fields": [
                            {"name": "sku", "type": "string"},
                            {"name": "qty", "type": "int"},
                        ],
                    },
                }},
            ],
        }
        paths = extract_field_paths(_body(schema))
        assert paths == {"items", "items.sku", "items.qty"}

    def test_array_of_primitive(self):
        schema = {
            "type": "record", "name": "Tags", "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
            ],
        }
        assert extract_field_paths(_body(schema)) == {"tags"}

    def test_map_of_record(self):
        schema = {
            "type": "record", "name": "M", "fields": [
                {"name": "attrs", "type": {
                    "type": "map", "values": {
                        "type": "record", "name": "Attr", "fields": [
                            {"name": "v", "type": "string"},
                        ],
                    },
                }},
            ],
        }
        paths = extract_field_paths(_body(schema))
        assert paths == {"attrs", "attrs.v"}

    def test_enum_and_fixed_are_leaves(self):
        schema = {
            "type": "record", "name": "E", "fields": [
                {"name": "color",  "type": {"type": "enum",  "name": "Color",  "symbols": ["RED", "GREEN"]}},
                {"name": "digest", "type": {"type": "fixed", "name": "Digest", "size": 16}},
            ],
        }
        # Just the leaf names — no children.
        assert extract_field_paths(_body(schema)) == {"color", "digest"}

    def test_malformed_schema_returns_empty(self, caplog):
        body = {"schema": "{ this is not json", "schemaType": "AVRO"}
        with caplog.at_level("WARNING"):
            assert extract_field_paths(body) == set()
        assert any("malformed" in r.message.lower() for r in caplog.records)

    def test_missing_schema_field_returns_empty(self, caplog):
        with caplog.at_level("WARNING"):
            assert extract_field_paths({"subject": "x", "version": 1}) == set()
        assert any("missing" in r.message.lower() for r in caplog.records)

    def test_unsupported_schema_type_returns_empty(self, caplog):
        body = _body({"type": "object"}, schema_type="JSON")
        with caplog.at_level("WARNING"):
            assert extract_field_paths(body) == set()
        assert any("not supported" in r.message.lower() for r in caplog.records)

    def test_protobuf_schema_type_returns_empty(self):
        body = _body({"type": "record", "fields": []}, schema_type="PROTOBUF")
        assert extract_field_paths(body) == set()


class TestFetchSchemaBody:
    @pytest.mark.asyncio
    async def test_returns_parsed_body_on_200(self):
        client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"schema": "{}", "schemaType": "AVRO"}
        client.get = AsyncMock(return_value=resp)

        body = await fetch_schema_body(client, "https://sr", ("k", "s"), "subj", 1)
        assert body == {"schema": "{}", "schemaType": "AVRO"}

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self, caplog):
        client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 404
        client.get = AsyncMock(return_value=resp)

        with caplog.at_level("WARNING"):
            body = await fetch_schema_body(client, "https://sr", ("k", "s"), "subj", 1)
        assert body is None
        assert any("404" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_returns_none_on_http_error(self, caplog):
        client = AsyncMock()
        client.get = AsyncMock(side_effect=httpx.ConnectError("boom"))
        with caplog.at_level("WARNING"):
            body = await fetch_schema_body(client, "https://sr", ("k", "s"), "subj", 1)
        assert body is None


class TestFetchFieldPathsCached:
    @pytest.mark.asyncio
    async def test_caches_within_ttl(self):
        _clear_cache_for_tests()
        client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _body({
            "type": "record", "name": "F",
            "fields": [{"name": "a", "type": "string"}],
        })
        client.get = AsyncMock(return_value=resp)

        a = await fetch_field_paths_cached(client, "https://sr", ("k", "s"), "subj", 7)
        b = await fetch_field_paths_cached(client, "https://sr", ("k", "s"), "subj", 7)
        assert a == b == {"a"}
        assert client.get.call_count == 1  # second call hit the cache

    @pytest.mark.asyncio
    async def test_different_versions_are_separate_keys(self):
        _clear_cache_for_tests()
        client = AsyncMock()

        def _make_resp(call_url, **_kw):
            r = MagicMock()
            r.status_code = 200
            # version 1 has field "a", version 2 has field "b"
            v = "1" if call_url.endswith("/1") else "2"
            r.json.return_value = _body({
                "type": "record", "name": "F",
                "fields": [{"name": "a" if v == "1" else "b", "type": "string"}],
            })
            return r

        client.get = AsyncMock(side_effect=_make_resp)
        v1 = await fetch_field_paths_cached(client, "https://sr", ("k", "s"), "subj", 1)
        v2 = await fetch_field_paths_cached(client, "https://sr", ("k", "s"), "subj", 2)
        assert v1 == {"a"}
        assert v2 == {"b"}
        assert client.get.call_count == 2
