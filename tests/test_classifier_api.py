"""
Tests for the /classify and /health FastAPI endpoints.
AnalyzerEngine is mocked — no spaCy or GLiNER needed.
"""
import sys
from unittest.mock import MagicMock, patch

# Stub GLiNER (not installed in the test venv — lives in the Docker image).
# spaCy is installed and real, so we do NOT stub it here.
sys.modules.setdefault("gliner", MagicMock())

import pytest
from fastapi.testclient import TestClient
from presidio_analyzer import RecognizerResult


def _make_result(entity_type, start, end, score=0.9):
    return RecognizerResult(entity_type=entity_type, start=start, end=end, score=score)


@pytest.fixture()
def client():
    """TestClient with mocked analyzers injected at module level."""
    mock_analyzer = MagicMock()

    with patch("main.build_regex_analyzer", return_value=mock_analyzer), \
         patch("main.build_ai_analyzer", return_value=mock_analyzer):
        import main as app_module
        app_module.regex_analyzer = mock_analyzer
        app_module.ai_analyzer = mock_analyzer
        with TestClient(app_module.app) as c:
            c._mock_analyzer = mock_analyzer
            yield c


class TestHealthEndpoint:
    def test_returns_ok_when_analyzer_ready(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert resp.json()["analyzer_ready"] is True
        assert resp.json()["version"] == "3.1.0"


class TestClassifyEndpoint:
    def test_detects_ssn_as_government_id(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("US_SSN", 16, 27, score=0.97)
        ]
        resp = client.post("/classify", json={"fields": {"note": "SSN is 123-45-6789"}})
        assert resp.status_code == 200
        body = resp.json()
        assert "GOVERNMENT_ID" in body["tags"]

    def test_field_name_layer1_detected_from_field_ssn(self, client):
        # Field named "ssn" — Layer 1 catches it without looking at data
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"ssn": "ignored"}})
        body = resp.json()
        assert "GOVERNMENT_ID" in body["tags"]
        entity = body["detected_entities"]["ssn"][0]
        assert entity["layer"] == 1
        assert entity["source"] == "field_name"

    def test_field_named_email_triggers_layer1(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"email": "test@example.com"}})
        body = resp.json()
        assert "PII" in body["tags"]
        entity = body["detected_entities"]["email"][0]
        assert entity["source"] == "field_name"

    def test_regex_results_tagged_layer2(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("MEDICAL_RECORD", 0, 10, score=0.93)
        ]
        resp = client.post("/classify", json={"fields": {"id": "MRN-0012345"}})
        body = resp.json()
        assert "PHI" in body["tags"]
        # Regex and AI analyzers both return the mocked result
        layers = {e["layer"] for e in body["detected_entities"]["id"]}
        assert 2 in layers or 3 in layers

    def test_max_layer_1_skips_data_inspection(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("CREDIT_CARD", 0, 16, score=0.99)
        ]
        # max_layer=1 → analyzers not called, only field name checked
        resp = client.post("/classify", json={
            "fields": {"ref": "4111111111111111"},
            "max_layer": 1,
        })
        body = resp.json()
        # "ref" gives no Layer 1 match → no entities
        assert body["detected_entities"] == {}
        assert body["tags"] == []
        # Analyzers must not have been called
        assert not client._mock_analyzer.analyze.called

    def test_max_layer_1_with_named_field_still_detects(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={
            "fields": {"password": "s3cr3t!"},
            "max_layer": 1,
        })
        body = resp.json()
        assert "CREDENTIALS" in body["tags"]
        assert body["detected_entities"]["password"][0]["layer"] == 1

    def test_layers_used_in_response(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"x": "y"}, "max_layer": 3})
        assert set(resp.json()["layers_used"]) == {1, 2, 3}

    def test_layers_used_max_layer_2(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"x": "y"}, "max_layer": 2})
        assert 3 not in resp.json()["layers_used"]

    def test_clean_message_returns_empty_tags(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"status": "approved"}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["tags"] == []
        assert body["detected_entities"] == {}

    def test_nested_fields_are_flattened(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={
            "fields": {"customer": {"name": "Alice", "address": {"city": "NYC"}}}
        })
        assert resp.status_code == 200

    def test_response_includes_version_timestamp_and_layers_used(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"x": "y"}})
        body = resp.json()
        assert body["classifier_version"] == "3.1.0"
        assert "classified_at" in body
        assert "layers_used" in body

    def test_no_sensitivity_level_in_response(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"x": "y"}})
        assert "sensitivity_level" not in resp.json()
        assert "categories" not in resp.json()
