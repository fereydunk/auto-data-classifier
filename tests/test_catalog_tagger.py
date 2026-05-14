"""
Tests for CatalogTagger — all HTTP calls are mocked.
No Confluent Cloud connection required.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from catalog_tagger import CatalogTagger, _field_qualified_name, _highest_category, TAG_DEFINITIONS
from sr_schema import _clear_cache_for_tests


SR_URL = "https://psrc-test.confluent.cloud"
SR_CLUSTER_ID = "lsrc-test01"

ALL_TAG_NAMES = [t["name"] for t in TAG_DEFINITIONS]


@pytest.fixture(autouse=True)
def _clear_sr_cache():
    """Each test gets a fresh sr_schema cache."""
    _clear_cache_for_tests()
    yield
    _clear_cache_for_tests()


def _avro_schema_for_paths(paths):
    """Build a minimal Avro record JSON that contains every dotted path in `paths`.

    Top-level fields are made type=string; dotted children become nested
    records. Used by the SR mock so the new field-validation gate sees the
    test's field as "present in the live schema" instead of failing closed.
    """
    tree = {}
    for p in paths:
        parts = p.split(".")
        node = tree
        for part in parts:
            node = node.setdefault(part, {})

    def _to_record(d, name):
        fields = []
        for fname, children in d.items():
            if children:
                fields.append({"name": fname, "type": _to_record(children, fname.title())})
            else:
                fields.append({"name": fname, "type": "string"})
        return {"type": "record", "name": name, "fields": fields}

    return json.dumps(_to_record(tree, "Root"))


def make_tagger() -> CatalogTagger:
    return CatalogTagger(
        sr_url=SR_URL,
        sr_api_key="key",
        sr_api_secret="secret",
        sr_cluster_id=SR_CLUSTER_ID,
    )


def mock_client(
    tagdefs_get_status=200,
    tagdefs_get_body=None,
    tagdefs_post_status=200,
    versions_status=200,
    versions_body=None,
    version_detail_body=None,
    schema_field_paths=None,
    tag_post_status=200,
):
    """schema_field_paths: iterable of dotted paths to embed in the SR schema.
    Defaults to a permissive schema that contains every common test field path
    so existing tests keep passing without explicit listing."""
    client = AsyncMock()

    if schema_field_paths is None:
        schema_field_paths = [
            "customer.email", "customer.ssn", "patient.diagnosis",
            "db.password", "notes", "account.number",
        ]
    schema_str = _avro_schema_for_paths(schema_field_paths)
    detail_default = {
        "id": 99, "version": 1, "schema": schema_str, "schemaType": "AVRO",
        "subject": "test-value",
    }
    detail = {**detail_default, **(version_detail_body or {})}

    def _resp(status, body=None):
        r = MagicMock()
        r.status_code = status
        r.json.return_value = body or {}
        r.raise_for_status = MagicMock()
        if status >= 400:
            import httpx
            r.raise_for_status.side_effect = httpx.HTTPStatusError(
                "error", request=MagicMock(), response=r
            )
        return r

    client.get = AsyncMock(side_effect=lambda url, **kw: (
        _resp(tagdefs_get_status, tagdefs_get_body or [])
        if "tagdefs" in url
        else _resp(versions_status, versions_body or [1])
        if "/versions" in url and url.endswith("/versions")
        else _resp(200, detail)
    ))
    client.post = AsyncMock(return_value=_resp(tagdefs_post_status))
    return client


class TestQualifiedName:
    def test_format(self):
        # New Atlas-compatible format: {cluster}:.:{schema_id}:{namespace.record}.{field}
        qn = _field_qualified_name("lsrc-abc", 100014, "io.confluent.demo.Order", "customer.ssn")
        assert qn == "lsrc-abc:.:100014:io.confluent.demo.Order.customer.ssn"

    def test_nested_field(self):
        qn = _field_qualified_name("lsrc-abc", 5, "ns.Topic", "address.city")
        assert "address.city" in qn
        assert ":5:" in qn


class TestHighestTag:
    def test_phi_beats_pii(self):
        assert _highest_category(["PII", "PHI"]) == "PHI"

    def test_credentials_beats_pci(self):
        assert _highest_category(["PCI", "CREDENTIALS"]) == "CREDENTIALS"

    def test_phi_beats_credentials(self):
        assert _highest_category(["CREDENTIALS", "PHI"]) == "PHI"

    def test_pci_beats_financial(self):
        assert _highest_category(["FINANCIAL", "PCI"]) == "PCI"

    def test_financial_beats_pii(self):
        assert _highest_category(["PII", "FINANCIAL"]) == "FINANCIAL"

    def test_government_id_beats_location(self):
        assert _highest_category(["LOCATION", "GOVERNMENT_ID"]) == "GOVERNMENT_ID"

    def test_single_tag_returned(self):
        assert _highest_category(["MINOR"]) == "MINOR"

    def test_empty_defaults_to_pii(self):
        assert _highest_category([]) == "PII"


class TestTagDefinitions:
    def test_all_11_tags_defined(self):
        assert len(TAG_DEFINITIONS) == 11

    def test_tag_names(self):
        expected = {
            "PII", "PHI", "PCI", "CREDENTIALS", "FINANCIAL",
            "GOVERNMENT_ID", "BIOMETRIC", "GENETIC", "NPI", "LOCATION", "MINOR",
        }
        assert set(ALL_TAG_NAMES) == expected


class TestEnsureTagDefinitions:
    @pytest.mark.asyncio
    async def test_creates_missing_tags(self):
        tagger = make_tagger()
        client = mock_client(tagdefs_get_body=[{"name": "PII"}])
        await tagger.ensure_tag_definitions(client)
        assert client.post.called

    @pytest.mark.asyncio
    async def test_skips_if_all_exist(self):
        tagger = make_tagger()
        client = mock_client(tagdefs_get_body=[{"name": n} for n in ALL_TAG_NAMES])
        await tagger.ensure_tag_definitions(client)
        assert not client.post.called

    @pytest.mark.asyncio
    async def test_bootstrapped_flag_prevents_duplicate_calls(self):
        tagger = make_tagger()
        client = mock_client(tagdefs_get_body=[])
        await tagger.ensure_tag_definitions(client)
        await tagger.ensure_tag_definitions(client)
        assert client.get.call_count == 1


class TestApplyClassifications:
    @pytest.mark.asyncio
    async def test_phi_field_tagged_as_phi(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 10, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="patients",
            detected_entities={
                "patient.diagnosis": [
                    {"entity_type": "MEDICAL_CONDITION", "tag": "PHI", "score": 0.91}
                ]
            },
            schema_id=10,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["typeName"] == "PHI"
        assert call_payload[0]["entityType"] == "sr_field"
        # New format: {cluster}:.:{schema_id}:{namespace.record}.{field}
        assert ":Root.patient.diagnosis" in call_payload[0]["entityName"]

    @pytest.mark.asyncio
    async def test_pii_field_tagged_as_pii(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 5, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="orders",
            detected_entities={
                "customer.email": [
                    {"entity_type": "EMAIL_ADDRESS", "tag": "PII", "score": 0.95}
                ]
            },
            schema_id=5,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["typeName"] == "PII"

    @pytest.mark.asyncio
    async def test_credentials_field_tagged_as_credentials(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 3, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="configs",
            detected_entities={
                "db.password": [
                    {"entity_type": "PASSWORD", "tag": "CREDENTIALS", "score": 0.99}
                ]
            },
            schema_id=3,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["typeName"] == "CREDENTIALS"

    @pytest.mark.asyncio
    async def test_government_id_field_tagged_correctly(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 8, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="users",
            detected_entities={
                "customer.ssn": [
                    {"entity_type": "US_SSN", "tag": "GOVERNMENT_ID", "score": 0.97}
                ]
            },
            schema_id=8,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["typeName"] == "GOVERNMENT_ID"

    @pytest.mark.asyncio
    async def test_phi_wins_over_pii_on_same_field(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 7, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="records",
            detected_entities={
                "notes": [
                    {"entity_type": "PERSON",            "tag": "PII", "score": 0.90},
                    {"entity_type": "MEDICAL_CONDITION", "tag": "PHI", "score": 0.88},
                ]
            },
            schema_id=7,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["typeName"] == "PHI"
        assert call_payload[0]["entityType"] == "sr_field"

    @pytest.mark.asyncio
    async def test_no_api_call_for_empty_entities(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client()
        await tagger.apply_classifications(client=client, topic="orders", detected_entities={}, schema_id=1)
        assert not client.post.called

    @pytest.mark.asyncio
    async def test_in_process_cache_prevents_duplicate_tags(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 7, "version": 1})
        entities = {"customer.ssn": [{"entity_type": "US_SSN", "tag": "GOVERNMENT_ID", "score": 0.97}]}

        await tagger.apply_classifications(client=client, topic="payments", detected_entities=entities, schema_id=7)
        first_count = client.post.call_count

        await tagger.apply_classifications(client=client, topic="payments", detected_entities=entities, schema_id=7)
        assert client.post.call_count == first_count

    @pytest.mark.asyncio
    async def test_409_treated_as_success(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        r = MagicMock()
        r.status_code = 409
        client = mock_client(versions_body=[1], version_detail_body={"id": 3, "version": 1})
        client.post = AsyncMock(return_value=r)

        await tagger.apply_classifications(
            client=client, topic="payments",
            detected_entities={"account.number": [{"entity_type": "BANK_ACCOUNT", "tag": "FINANCIAL", "score": 0.8}]},
            schema_id=3,
        )
        assert len(tagger._tagged) == 1


class TestSRFieldValidation:
    """SR-as-source-of-truth: field_path must exist in the live schema body
    or the tag is dropped without POSTing. Mirrors the same gate in
    review-api/catalog_client.py.
    """

    @pytest.mark.asyncio
    async def test_field_in_schema_is_posted(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        # Schema explicitly contains the field we're tagging.
        client = mock_client(
            versions_body=[1],
            version_detail_body={"id": 50, "version": 1},
            schema_field_paths=["customer.email"],
        )
        await tagger.apply_classifications(
            client=client, topic="orders",
            detected_entities={"customer.email": [
                {"entity_type": "EMAIL_ADDRESS", "tag": "PII", "score": 0.95},
            ]},
            schema_id=50,
        )
        assert client.post.called, "should POST when field exists in SR schema"

    @pytest.mark.asyncio
    async def test_field_not_in_schema_is_dropped(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        # Schema only has 'ordertime', NOT 'customer.email' (mirrors today's
        # production bug: orders-value schema has no customer.email field).
        client = mock_client(
            versions_body=[1],
            version_detail_body={"id": 51, "version": 1},
            schema_field_paths=["ordertime"],
        )
        await tagger.apply_classifications(
            client=client, topic="orders",
            detected_entities={"customer.email": [
                {"entity_type": "EMAIL_ADDRESS", "tag": "PII", "score": 0.95},
            ]},
            schema_id=51,
        )
        assert not client.post.called, "should NOT POST when field is missing from SR schema"
        assert len(tagger._tagged) == 0, "dropped items must not be cached as tagged"

    @pytest.mark.asyncio
    async def test_unfetchable_schema_drops_tag(self):
        """Fail closed: if SR returns a body without a 'schema' field
        (or it's malformed), we must not POST blindly."""
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        # Empty schema_field_paths → empty record. extract_field_paths returns
        # set() because the record has no fields. Validation fails closed.
        client = mock_client(
            versions_body=[1],
            version_detail_body={"id": 52, "version": 1},
            schema_field_paths=[],
        )
        await tagger.apply_classifications(
            client=client, topic="orders",
            detected_entities={"x": [
                {"entity_type": "FOO", "tag": "PII", "score": 0.9},
            ]},
            schema_id=52,
        )
        assert not client.post.called
