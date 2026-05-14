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
_REVIEW_MODULES = ("main", "store", "models", "catalog_client", "sr_schema")


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

        # Approve = stage now (no catalog push); test filters cover STAGED.
        rec_id = client.get("/recommendations").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/approve")

        pending = client.get("/recommendations?status=PENDING").json()
        staged  = client.get("/recommendations?status=STAGED").json()
        assert len(pending) == 1
        assert len(staged)  == 1

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


class TestApproveStages:
    """Approve no longer pushes to catalog — it transitions PENDING → STAGED.
    The catalog push happens when /submit-staged is called for the whole batch."""

    def test_approve_transitions_to_staged_no_catalog_call(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]

        # No catalog mock needed — approve must NOT touch the catalog now.
        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))) as mock_batch:
            resp = client.post(f"/recommendations/{rec_id}/approve")
            assert resp.status_code == 200
            assert resp.json()["status"] == "STAGED"
            mock_batch.assert_not_called()

    def test_approve_already_staged_returns_409(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/approve")
        resp = client.post(f"/recommendations/{rec_id}/approve")
        assert resp.status_code == 409

    def test_approve_nonexistent_returns_404(self, client):
        resp = client.post("/recommendations/nonexistent-id/approve")
        assert resp.status_code == 404


class TestUnstage:
    def test_unstage_returns_to_pending(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/approve")  # PENDING → STAGED
        resp = client.post(f"/recommendations/{rec_id}/unstage")
        assert resp.status_code == 200
        assert resp.json()["status"] == "PENDING"

    def test_unstage_pending_returns_409(self, client):
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        resp = client.post(f"/recommendations/{rec_id}/unstage")
        assert resp.status_code == 409


class TestSubmitStaged:
    def test_submit_staged_batches_into_one_catalog_call(self, client):
        _create(client, field_path="a", tag="PII")
        _create(client, field_path="b", tag="PHI")
        _create(client, field_path="c", tag="PCI")
        # Stage all three
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))) as mock_batch:
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 200
            assert len(resp.json()) == 3
            mock_batch.assert_called_once()
            # Single batch — assert items list contains all three field_paths
            items = mock_batch.call_args[0][0]
            assert {i["field_path"] for i in items} == {"a", "b", "c"}

    def test_submit_staged_marks_all_approved_after_success(self, client):
        _create(client, field_path="a")
        _create(client, field_path="b")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            client.post("/recommendations/submit-staged")

        approved = client.get("/recommendations?status=APPROVED").json()
        assert len(approved) == 2

    def test_submit_staged_no_status_change_on_batch_failure(self, client):
        """All-or-nothing: batch POST fails → nothing flips to APPROVED."""
        _create(client, field_path="a")
        _create(client, field_path="b")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        with patch("main.apply_tags_batch",
                   new=AsyncMock(return_value=(False, "catalog 500", []))):
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 502

        staged = client.get("/recommendations?status=STAGED").json()
        approved = client.get("/recommendations?status=APPROVED").json()
        assert len(staged) == 2
        assert len(approved) == 0

    def test_submit_staged_with_nothing_to_submit_returns_empty_list(self, client):
        resp = client.post("/recommendations/submit-staged")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_submit_staged_returns_503_when_catalog_not_configured(self, client):
        from catalog_client import CatalogNotConfiguredError
        _create(client)
        rec_id = client.get("/recommendations").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/approve")

        not_configured = AsyncMock(side_effect=CatalogNotConfiguredError(
            "Stream Catalog not configured — missing env vars: CONFLUENT_SR_URL"
        ))
        with patch("main.apply_tags_batch", new=not_configured):
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 503
            assert "CONFLUENT_SR_URL" in resp.json()["detail"]


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

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            resp = client.post("/recommendations/bulk-approve", json={})
            assert resp.status_code == 200
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["field_path"] == "a"

    def test_bulk_approve_scoped_to_topic(self, client):
        _create(client, field_path="a", topic="payments", confidence=0.97)
        _create(client, field_path="b", tag="PHI", topic="orders", confidence=0.97)

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            resp = client.post("/recommendations/bulk-approve", json={"topic": "payments"})
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["topic"] == "payments"

    def test_bulk_approve_scoped_to_tag(self, client):
        _create(client, field_path="a", tag="PII", confidence=0.97)
        _create(client, field_path="b", tag="PHI", confidence=0.97)

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            resp = client.post("/recommendations/bulk-approve", json={"tag": "PHI"})
            approved = resp.json()
            assert len(approved) == 1
            assert approved[0]["proposed_tag"] == "PHI"

    def test_bulk_approve_custom_threshold(self, client):
        _create(client, field_path="a", confidence=0.70)  # MEDIUM
        _create(client, field_path="b", tag="PHI", confidence=0.40)  # LOW

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            resp = client.post("/recommendations/bulk-approve", json={"min_confidence": 0.60})
            assert len(resp.json()) == 1


class TestSummary:
    def test_summary_counts_by_topic(self, client):
        _create(client, topic="payments", field_path="a")
        _create(client, topic="payments", field_path="b", tag="PHI")
        _create(client, topic="orders", field_path="c")

        # Approve = stage; then submit-staged flips to APPROVED.
        rec_id = client.get("/recommendations?topic=orders").json()[0]["id"]
        client.post(f"/recommendations/{rec_id}/approve")
        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            client.post("/recommendations/submit-staged")

        summary = {s["topic"]: s for s in client.get("/recommendations/summary").json()}
        assert summary["payments"]["pending"] == 2
        assert summary["orders"]["approved"] == 1


class TestDeleteByTopic:
    def test_delete_by_topic_wipes_only_that_topic(self, client):
        _create(client, topic="payments", field_path="a")
        _create(client, topic="payments", field_path="b", tag="PHI")
        _create(client, topic="orders",   field_path="c")

        resp = client.delete("/recommendations?topic=payments")
        assert resp.status_code == 200
        assert resp.json() == {"topic": "payments", "deleted": 2}

        remaining = client.get("/recommendations").json()
        assert len(remaining) == 1
        assert remaining[0]["topic"] == "orders"

    def test_delete_unknown_topic_returns_zero(self, client):
        _create(client, topic="payments", field_path="a")
        resp = client.delete("/recommendations?topic=ghost-topic")
        assert resp.status_code == 200
        assert resp.json() == {"topic": "ghost-topic", "deleted": 0}

    def test_delete_without_topic_param_400(self, client):
        # FastAPI rejects missing required query params with 422 — that's fine,
        # the protection (don't wipe all rows accidentally) still holds.
        resp = client.delete("/recommendations")
        assert resp.status_code in (400, 422)


class TestUI:
    def test_root_serves_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Tag Recommendations" in resp.text


# ---------------------------------------------------------------------------
# SR-as-source-of-truth: dropped items get auto-REJECTED on submit, and the
# /validate-staged endpoint surfaces them to the UI before submit.
# ---------------------------------------------------------------------------
class TestDroppedItemsAutoReject:
    def test_dropped_items_marked_rejected(self, client):
        """Items the SR-validation gate dropped must end up REJECTED with a
        descriptive reviewed_by string — not silently disappearing."""
        _create(client, field_path="real.field", tag="PII")
        _create(client, field_path="ghost.field", tag="PHI")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        # Mock: ghost.field is dropped, real.field is posted.
        mock_return = (True, "", [
            {"subject": "payments-value", "field_path": "ghost.field",
             "reason": "field not in current SR schema"},
        ])
        with patch("main.apply_tags_batch", new=AsyncMock(return_value=mock_return)):
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 200
            # response only contains the surviving rec, not the dropped one
            assert len(resp.json()) == 1
            assert resp.json()[0]["field_path"] == "real.field"

        rejected = client.get("/recommendations?status=REJECTED").json()
        assert len(rejected) == 1
        assert rejected[0]["field_path"] == "ghost.field"
        assert "auto:" in rejected[0]["reviewed_by"]
        assert "not in current SR schema" in rejected[0]["reviewed_by"]

    def test_all_dropped_returns_502_and_rejects_all(self, client):
        """If every staged item is dropped by validation, POST never happens
        and we surface a 502; the dropped items still get REJECTED so the
        staged queue clears out."""
        _create(client, field_path="ghost1")
        _create(client, field_path="ghost2", tag="PHI")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        mock_return = (False, "all 2 item(s) dropped — none match the live SR schema", [
            {"subject": "payments-value", "field_path": "ghost1",
             "reason": "field not in current SR schema"},
            {"subject": "payments-value", "field_path": "ghost2",
             "reason": "field not in current SR schema"},
        ])
        with patch("main.apply_tags_batch", new=AsyncMock(return_value=mock_return)):
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 502
            assert "auto-rejected" in resp.json()["detail"]

        rejected = client.get("/recommendations?status=REJECTED").json()
        staged = client.get("/recommendations?status=STAGED").json()
        assert len(rejected) == 2
        assert len(staged) == 0

    def test_clean_batch_no_dropped_no_rejects(self, client):
        """Backwards-compat: when nothing is dropped, behavior is identical
        to the old 2-tuple path."""
        _create(client, field_path="real")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        with patch("main.apply_tags_batch", new=AsyncMock(return_value=(True, "", []))):
            resp = client.post("/recommendations/submit-staged")
            assert resp.status_code == 200
            assert len(resp.json()) == 1

        approved = client.get("/recommendations?status=APPROVED").json()
        rejected = client.get("/recommendations?status=REJECTED").json()
        assert len(approved) == 1
        assert len(rejected) == 0


class TestValidateStaged:
    """GET /recommendations/validate-staged — UI preview that flags stale
    STAGED items before the user clicks Submit."""

    def test_503_when_sr_not_configured(self, client, monkeypatch):
        # Make sure no SR env vars are set in this test process.
        for v in ("CONFLUENT_SR_URL", "CONFLUENT_SR_API_KEY",
                  "CONFLUENT_SR_API_SECRET", "CONFLUENT_SR_CLUSTER_ID"):
            monkeypatch.delenv(v, raising=False)
        resp = client.get("/recommendations/validate-staged")
        assert resp.status_code == 503
        assert "CONFLUENT_SR" in resp.json()["detail"]

    def test_returns_valid_and_stale(self, client, monkeypatch):
        # Configure SR env so the endpoint runs.
        monkeypatch.setenv("CONFLUENT_SR_URL", "https://sr.test")
        monkeypatch.setenv("CONFLUENT_SR_API_KEY", "k")
        monkeypatch.setenv("CONFLUENT_SR_API_SECRET", "s")
        monkeypatch.setenv("CONFLUENT_SR_CLUSTER_ID", "lsrc-x")

        # Stage 2 recs: one matches the schema, one doesn't.
        _create(client, field_path="real.field", tag="PII")
        _create(client, field_path="ghost.field", tag="PHI")
        for r in client.get("/recommendations").json():
            client.post(f"/recommendations/{r['id']}/approve")

        # Patch the SR-side helpers so we don't need a real cluster.
        async def fake_resolve(*_a, **_kw):
            return 1
        async def fake_paths(*_a, **_kw):
            return {"real.field"}
        with patch("main._resolve_version", new=AsyncMock(side_effect=fake_resolve)) if False else \
             patch("catalog_client._resolve_version", new=AsyncMock(side_effect=fake_resolve)), \
             patch("main.fetch_field_paths_cached", new=AsyncMock(side_effect=fake_paths)):
            resp = client.get("/recommendations/validate-staged")
        assert resp.status_code == 200
        data = resp.json()
        assert data["checked"] == 2
        assert len(data["valid"]) == 1
        assert len(data["stale"]) == 1
        assert data["stale"][0]["field_path"] == "ghost.field"
        assert "not in current SR schema" in data["stale"][0]["reason"]

    def test_no_staged_returns_empty(self, client, monkeypatch):
        monkeypatch.setenv("CONFLUENT_SR_URL", "https://sr.test")
        monkeypatch.setenv("CONFLUENT_SR_API_KEY", "k")
        monkeypatch.setenv("CONFLUENT_SR_API_SECRET", "s")
        monkeypatch.setenv("CONFLUENT_SR_CLUSTER_ID", "lsrc-x")
        resp = client.get("/recommendations/validate-staged")
        assert resp.status_code == 200
        assert resp.json() == {"checked": 0, "valid": [], "stale": []}
