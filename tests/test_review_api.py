"""
Tests for the Tag Recommendation Review API.
SQLite runs in-memory. Stream Catalog calls are mocked.
"""
import sys
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch

# review-api modules share names with classifier-service (main, models).
# Insert review-api at the front and flush the module cache in each fixture
# so Python loads the right files regardless of test execution order.
_REVIEW_API = str(Path(__file__).parent.parent / "review-api")
_REVIEW_MODULES = ("main", "store", "models", "catalog_client")


def _load_review_api():
    """Ensure review-api is at the front of sys.path and reload its modules."""
    if _REVIEW_API in sys.path:
        sys.path.remove(_REVIEW_API)
    sys.path.insert(0, _REVIEW_API)
    for name in _REVIEW_MODULES:
        sys.modules.pop(name, None)

    import store as store_module
    import main as app_module
    return app_module, store_module


from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path):
    """Each test gets its own SQLite file so connections share the same DB."""
    app_module, store_module = _load_review_api()

    db_file = str(tmp_path / "test_recommendations.db")
    app_module.store = store_module.RecommendationStore(db_path=db_file)

    with TestClient(app_module.app) as c:
        yield c


def _create(client, field_path="customer.email", tag="PII", confidence=0.97,
            topic="payments", entity_type="EMAIL_ADDRESS"):
    return client.post("/recommendations", json={
        "topic": topic,
        "subject": f"{topic}-value",
        "schema_id": 42,
        "field_path": field_path,
        "proposed_tag": tag,
        "entity_type": entity_type,
        "confidence": confidence,
    })


class TestCreateRecommendation:
    def test_creates_with_correct_fields(self, client):
        resp = _create(client)
        assert resp.status_code == 201
        body = resp.json()
        assert body["field_path"] == "customer.email"
        assert body["proposed_tag"] == "PII"
        assert body["confidence"] == 0.97
        assert body["confidence_tier"] == "HIGH"
        assert body["status"] == "PENDING"

    def test_confidence_tier_high(self, client):
        assert _create(client, confidence=0.97).json()["confidence_tier"] == "HIGH"

    def test_confidence_tier_medium(self, client):
        assert _create(client, confidence=0.70).json()["confidence_tier"] == "MEDIUM"

    def test_confidence_tier_low(self, client):
        assert _create(client, confidence=0.40).json()["confidence_tier"] == "LOW"

    def test_upsert_keeps_higher_confidence(self, client):
        _create(client, confidence=0.60)
        resp = _create(client, confidence=0.90)  # same field+tag, higher confidence
        assert resp.json()["confidence"] == 0.90
        assert resp.json()["confidence_tier"] == "HIGH"

    def test_upsert_keeps_existing_if_lower_confidence(self, client):
        _create(client, confidence=0.90)
        resp = _create(client, confidence=0.50)  # lower — should keep 0.90
        assert resp.json()["confidence"] == 0.90

    def test_no_duplicates_for_same_field_tag(self, client):
        _create(client)
        _create(client)
        recs = client.get("/recommendations").json()
        assert len(recs) == 1


class TestListRecommendations:
    def test_sorted_by_confidence_desc(self, client):
        _create(client, field_path="a", confidence=0.50)
        _create(client, field_path="b", confidence=0.99)
        _create(client, field_path="c", confidence=0.75)
        recs = client.get("/recommendations").json()
        scores = [r["confidence"] for r in recs]
        assert scores == sorted(scores, reverse=True)

    def test_filter_by_status(self, client):
        _create(client, field_path="f1")
        _create(client, field_path="f2", tag="PHI")

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            rec_id = client.get("/recommendations").json()[0]["id"]
            client.post(f"/recommendations/{rec_id}/approve")

        pending = client.get("/recommendations?status=PENDING").json()
        approved = client.get("/recommendations?status=APPROVED").json()
        assert len(pending) == 1
        assert len(approved) == 1

    def test_filter_by_topic(self, client):
        _create(client, topic="payments")
        _create(client, field_path="x", topic="orders", tag="PHI")
        recs = client.get("/recommendations?topic=payments").json()
        assert all(r["topic"] == "payments" for r in recs)

    def test_filter_by_tag(self, client):
        _create(client, tag="PII")
        _create(client, field_path="mrn", tag="PHI")
        recs = client.get("/recommendations?tag=PHI").json()
        assert all(r["proposed_tag"] == "PHI" for r in recs)

    def test_filter_by_tier(self, client):
        _create(client, field_path="a", confidence=0.95)
        _create(client, field_path="b", tag="PHI", confidence=0.45)
        high = client.get("/recommendations?tier=HIGH").json()
        assert all(r["confidence_tier"] == "HIGH" for r in high)


class TestApprove:
    def test_approve_calls_catalog_and_updates_status(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]

        with patch("main.apply_tag", new=AsyncMock(return_value=True)) as mock_tag:
            resp = client.post(f"/recommendations/{rec_id}/approve")
            assert resp.status_code == 200
            assert resp.json()["status"] == "APPROVED"
            mock_tag.assert_called_once()

    def test_approve_fails_if_catalog_fails(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]

        with patch("main.apply_tag", new=AsyncMock(return_value=False)):
            resp = client.post(f"/recommendations/{rec_id}/approve")
            assert resp.status_code == 502

    def test_approve_already_approved_returns_409(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            client.post(f"/recommendations/{rec_id}/approve")
            resp = client.post(f"/recommendations/{rec_id}/approve")
            assert resp.status_code == 409

    def test_approve_nonexistent_returns_404(self, client):
        resp = client.post("/recommendations/nonexistent-id/approve")
        assert resp.status_code == 404


class TestReject:
    def test_reject_updates_status(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        resp = client.post(f"/recommendations/{rec_id}/reject")
        assert resp.status_code == 200
        assert resp.json()["status"] == "REJECTED"

    def test_reject_already_rejected_returns_409(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/reject")
        resp = client.post(f"/recommendations/{rec_id}/reject")
        assert resp.status_code == 409


class TestBulkApprove:
    def test_approves_high_confidence_by_default(self, client):
        _create(client, field_path="a", confidence=0.97)  # HIGH
        _create(client, field_path="b", tag="PHI", confidence=0.45)  # LOW

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            resp = client.post("/recommendations/bulk-approve", json={})
            assert resp.status_code == 200
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["field_path"] == "a"

    def test_bulk_approve_scoped_to_topic(self, client):
        _create(client, field_path="a", topic="payments", confidence=0.97)
        _create(client, field_path="b", tag="PHI", topic="orders", confidence=0.97)

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            resp = client.post("/recommendations/bulk-approve", json={"topic": "payments"})
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["topic"] == "payments"

    def test_bulk_approve_scoped_to_tag(self, client):
        _create(client, field_path="a", tag="PII", confidence=0.97)
        _create(client, field_path="b", tag="PHI", confidence=0.97)

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            resp = client.post("/recommendations/bulk-approve", json={"tag": "PHI"})
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["proposed_tag"] == "PHI"

    def test_bulk_approve_custom_threshold(self, client):
        _create(client, field_path="a", confidence=0.70)  # MEDIUM
        _create(client, field_path="b", tag="PHI", confidence=0.40)  # LOW

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            resp = client.post("/recommendations/bulk-approve", json={"min_confidence": 0.60})
            assert len(resp.json()) == 1


class TestSummary:
    def test_summary_counts_by_topic(self, client):
        _create(client, topic="payments", field_path="a")
        _create(client, topic="payments", field_path="b", tag="PHI")
        _create(client, topic="orders", field_path="c")

        with patch("main.apply_tag", new=AsyncMock(return_value=True)):
            rec_id = client.get("/recommendations?topic=orders").json()[0]["id"]
            client.post(f"/recommendations/{rec_id}/approve")

        summary = {s["topic"]: s for s in client.get("/recommendations/summary").json()}
        assert summary["payments"]["pending"] == 2
        assert summary["orders"]["approved"] == 1
