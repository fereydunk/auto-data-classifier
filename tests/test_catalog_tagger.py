"""
Tests for CatalogTagger — all HTTP calls are mocked.
No Confluent Cloud connection required.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from catalog_tagger import CatalogTagger, _field_qualified_name, _highest_category, TAG_DEFINITIONS


SR_URL = "https://psrc-test.confluent.cloud"
SR_CLUSTER_ID = "lsrc-test01"

ALL_TAG_NAMES = [t["name"] for t in TAG_DEFINITIONS]


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
        assert call_payload[0]["classifications"][0]["typeName"] == "PHI"

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
        assert call_payload[0]["classifications"][0]["typeName"] == "PII"

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
        assert call_payload[0]["classifications"][0]["typeName"] == "CREDENTIALS"

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
        assert call_payload[0]["classifications"][0]["typeName"] == "GOVERNMENT_ID"

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
        assert call_payload[0]["classifications"][0]["typeName"] == "PHI"

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
