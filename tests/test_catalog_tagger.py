"""
Tests for CatalogTagger — all HTTP calls are mocked.
No Confluent Cloud connection required.
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from catalog_tagger import CatalogTagger, _field_qualified_name


SR_URL = "https://psrc-test.confluent.cloud"
SR_CLUSTER_ID = "lsrc-test01"
SUBJECT = "payments-value"


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
    tag_post_status=200,
):
    """Build a mock httpx.AsyncClient with configurable responses."""
    client = AsyncMock()

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

    # GET /catalog/v1/types/tagdefs
    client.get = AsyncMock(side_effect=lambda url, **kw: (
        _resp(tagdefs_get_status, tagdefs_get_body or [])
        if "tagdefs" in url
        else _resp(versions_status, versions_body or [1])
        if "/versions" in url and not url.endswith("/1")
        else _resp(200, version_detail_body or {"id": 99, "version": 1})
    ))

    client.post = AsyncMock(return_value=_resp(tagdefs_post_status))

    return client


class TestQualifiedName:
    def test_format(self):
        qn = _field_qualified_name("lsrc-abc", "orders-value", 3, "customer.ssn")
        assert qn == "lsrc-abc:.:orders-value.v3.customer.ssn"

    def test_nested_field(self):
        qn = _field_qualified_name("lsrc-abc", "topic-value", 1, "address.city")
        assert "address.city" in qn


class TestEnsureTagDefinitions:
    @pytest.mark.asyncio
    async def test_creates_missing_tags(self):
        tagger = make_tagger()
        client = mock_client(tagdefs_get_body=[{"name": "PII"}])  # PII exists, others don't
        await tagger.ensure_tag_definitions(client)
        # POST should have been called to create the missing tags
        assert client.post.called

    @pytest.mark.asyncio
    async def test_skips_if_all_exist(self):
        tagger = make_tagger()
        existing = [{"name": "PII"}, {"name": "SENSITIVE"}, {"name": "INTERNAL"}]
        client = mock_client(tagdefs_get_body=existing)
        await tagger.ensure_tag_definitions(client)
        assert not client.post.called

    @pytest.mark.asyncio
    async def test_bootstrapped_flag_prevents_duplicate_calls(self):
        tagger = make_tagger()
        client = mock_client(tagdefs_get_body=[])
        await tagger.ensure_tag_definitions(client)
        await tagger.ensure_tag_definitions(client)  # second call
        assert client.get.call_count == 1  # only called once


class TestApplyClassifications:
    @pytest.mark.asyncio
    async def test_tags_high_sensitivity_field_as_pii(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 10, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="payments",
            detected_entities={
                "customer.ssn": [{"entity_type": "US_SSN", "score": 0.97}]
            },
            schema_id=10,
        )

        assert client.post.called
        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["classifications"][0]["typeName"] == "PII"

    @pytest.mark.asyncio
    async def test_tags_medium_sensitivity_field_as_sensitive(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 5, "version": 1})

        await tagger.apply_classifications(
            client=client,
            topic="payments",
            detected_entities={
                "customer.name": [{"entity_type": "PERSON", "score": 0.91}]
            },
            schema_id=5,
        )

        call_payload = client.post.call_args[1]["json"]
        assert call_payload[0]["classifications"][0]["typeName"] == "SENSITIVE"

    @pytest.mark.asyncio
    async def test_no_api_call_for_empty_entities(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client()

        await tagger.apply_classifications(
            client=client,
            topic="payments",
            detected_entities={},
            schema_id=1,
        )

        assert not client.post.called

    @pytest.mark.asyncio
    async def test_in_process_cache_prevents_duplicate_tags(self):
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(versions_body=[1], version_detail_body={"id": 7, "version": 1})

        entities = {"customer.ssn": [{"entity_type": "US_SSN", "score": 0.97}]}

        await tagger.apply_classifications(client=client, topic="payments",
                                           detected_entities=entities, schema_id=7)
        first_call_count = client.post.call_count

        await tagger.apply_classifications(client=client, topic="payments",
                                           detected_entities=entities, schema_id=7)

        # No additional POST — cached
        assert client.post.call_count == first_call_count

    @pytest.mark.asyncio
    async def test_409_conflict_treated_as_success(self):
        """If the catalog already has the tag (409), we should not raise."""
        tagger = make_tagger()
        tagger._tags_bootstrapped = True
        client = mock_client(
            versions_body=[1],
            version_detail_body={"id": 3, "version": 1},
            tag_post_status=409,
        )
        # Override post to return 409
        r = MagicMock()
        r.status_code = 409
        client.post = AsyncMock(return_value=r)

        # Should not raise
        await tagger.apply_classifications(
            client=client,
            topic="payments",
            detected_entities={"account.number": [{"entity_type": "BANK_ACCOUNT", "score": 0.8}]},
            schema_id=3,
        )
        # Field should be cached despite 409
        assert len(tagger._tagged) == 1
