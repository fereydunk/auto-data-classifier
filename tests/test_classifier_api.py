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
    """TestClient with a mocked analyzer injected at module level."""
    mock_analyzer = MagicMock()

    with patch("main.build_analyzer", return_value=mock_analyzer):
        import main as app_module
        app_module.analyzer = mock_analyzer
        with TestClient(app_module.app) as c:
            c._mock_analyzer = mock_analyzer
            yield c


class TestHealthEndpoint:
    def test_returns_ok_when_analyzer_ready(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert resp.json()["analyzer_ready"] is True
        assert resp.json()["version"] == "2.0.0"


class TestClassifyEndpoint:
    def test_detects_ssn_as_pii_high(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("US_SSN", 16, 27, score=0.97)
        ]
        resp = client.post("/classify", json={"fields": {"note": "SSN is 123-45-6789"}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["sensitivity_level"] == "HIGH"
        assert "PII" in body["categories"]
        assert "note" in body["detected_entities"]
        entity = body["detected_entities"]["note"][0]
        assert entity["entity_type"] == "US_SSN"
        assert entity["category"] == "PII"

    def test_detects_medical_record_as_phi_critical(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("MEDICAL_RECORD", 0, 10, score=0.93)
        ]
        resp = client.post("/classify", json={"fields": {"id": "MRN-0012345"}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["sensitivity_level"] == "CRITICAL"
        assert "PHI" in body["categories"]
        assert body["detected_entities"]["id"][0]["category"] == "PHI"

    def test_detects_password_as_credentials_critical(self, client):
        client._mock_analyzer.analyze.return_value = [
            _make_result("PASSWORD", 0, 12, score=0.99)
        ]
        resp = client.post("/classify", json={"fields": {"config": "password=secret"}})
        assert resp.status_code == 200
        assert resp.json()["sensitivity_level"] == "CRITICAL"
        assert "CREDENTIALS" in resp.json()["categories"]

    def test_clean_message_returns_clean(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"status": "approved"}})
        assert resp.status_code == 200
        body = resp.json()
        assert body["sensitivity_level"] == "CLEAN"
        assert body["categories"] == []
        assert body["detected_entities"] == {}

    def test_multiple_categories_returned(self, client):
        def side_effect(text, language):
            if "ssn" in text.lower():
                return [_make_result("US_SSN", 0, 5)]
            if "mrn" in text.lower():
                return [_make_result("MEDICAL_RECORD", 0, 8)]
            return []

        client._mock_analyzer.analyze.side_effect = side_effect
        resp = client.post("/classify", json={
            "fields": {"note": "SSN here", "id": "MRN-123"}
        })
        body = resp.json()
        assert body["sensitivity_level"] == "CRITICAL"
        assert set(body["categories"]) == {"PII", "PHI"}

    def test_nested_fields_are_flattened(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={
            "fields": {"customer": {"name": "Alice", "address": {"city": "NYC"}}}
        })
        assert resp.status_code == 200
        assert client._mock_analyzer.analyze.call_count == 2

    def test_response_includes_classifier_version_and_timestamp(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"x": "y"}})
        body = resp.json()
        assert body["classifier_version"] == "2.0.0"
        assert "classified_at" in body

    def test_numeric_field_is_stringified_and_analyzed(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {"amount": 9999.99}})
        assert resp.status_code == 200
        assert client._mock_analyzer.analyze.called

    def test_empty_fields_object(self, client):
        client._mock_analyzer.analyze.return_value = []
        resp = client.post("/classify", json={"fields": {}})
        assert resp.status_code == 200
        assert resp.json()["sensitivity_level"] == "CLEAN"
