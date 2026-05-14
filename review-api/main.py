"""
Tag Recommendation Review API — v1.0.0

Receives classification recommendations from the pipeline, surfaces them
to data stewards for staging, and pushes a batch of staged tags to the
Confluent Stream Catalog in a single POST (one new schema version per
batch, not per tag).

Endpoints:
  GET    /recommendations                   List, filter by status/topic/tag/tier
  GET    /recommendations/summary           Pending/staged/approved/rejected by topic
  GET    /recommendations/sources           Topic → subject + latest SR version + schema_id
  POST   /recommendations                   Create/upsert one (called by pipeline)
  POST   /recommendations/{id}/approve      PENDING → STAGED (no catalog push)
  POST   /recommendations/{id}/unstage      STAGED → PENDING
  POST   /recommendations/{id}/reject       PENDING or STAGED → REJECTED
  POST   /recommendations/bulk-approve      Stage all ≥ min_confidence, then submit batch
  POST   /recommendations/submit-staged     Push every STAGED to catalog in one POST
  GET    /recommendations/validate-staged   Preview which STAGED items would auto-reject
  DELETE /recommendations?topic=X           Wipe every recommendation for one topic
"""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from models import (
    BulkApproveRequest,
    CreateRecommendationRequest,
    Recommendation,
    RecommendationStatus,
    RecommendationSummary,
)
from store import RecommendationStore
from catalog_client import (
    CatalogNotConfiguredError,
    REQUIRED_SR_ENV_VARS,
    _resolve_version,
    apply_tags_batch,
)
from sr_schema import fetch_field_paths_cached, fetch_schema_meta_cached
import asyncio
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger("review-api")

DB_PATH = os.getenv("DB_PATH", "/data/recommendations.db")
store = RecommendationStore(db_path=DB_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await store.init()
    logger.info("Recommendation store ready at %s", DB_PATH)
    yield


app = FastAPI(
    title="Tag Recommendation Review API",
    description="Human-in-the-loop review of auto-detected data classification tags.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# List & summary
# ---------------------------------------------------------------------------
@app.get("/recommendations", response_model=List[Recommendation])
async def list_recommendations(
    status: Optional[RecommendationStatus] = Query(None, description="Filter by status: PENDING, STAGED, APPROVED, REJECTED"),
    topic: Optional[str] = Query(None, description="Filter by Kafka topic"),
    tag: Optional[str] = Query(None, description="Filter by proposed tag, e.g. PII"),
    tier: Optional[str] = Query(None, description="Filter by confidence tier: HIGH, MEDIUM, LOW"),
):
    """
    Returns all recommendations sorted by confidence descending.
    HIGH-confidence items appear first — apply those in bulk, review the rest manually.
    """
    return await store.list_recommendations(
        status=status.value if status else None,
        topic=topic, tag=tag, tier=tier,
    )


@app.get("/recommendations/summary", response_model=List[RecommendationSummary])
async def summary():
    """Pending / approved / rejected counts grouped by topic."""
    return await store.summary()


@app.get("/recommendations/sources")
async def sources():
    """For each topic in the DB, return the current SR subject + latest
    version + schema id. Powers the topic/schema label in the review UI.

    SR fields are best-effort — if SR is unreachable or the subject isn't
    registered, those fields come back null but the topic still appears.
    """
    rows = await store.list_recommendations()
    topics = sorted({r.topic for r in rows})
    out = []

    sr_url    = os.environ.get("CONFLUENT_SR_URL")
    sr_key    = os.environ.get("CONFLUENT_SR_API_KEY")
    sr_secret = os.environ.get("CONFLUENT_SR_API_SECRET")
    sr_configured = bool(sr_url and sr_key and sr_secret)

    async with httpx.AsyncClient() as client:
        for topic in topics:
            subject = f"{topic}-value"
            entry = {
                "topic":     topic,
                "subject":   subject,
                "version":   None,
                "schema_id": None,
                "count":     sum(1 for r in rows if r.topic == topic),
            }
            if sr_configured:
                try:
                    resp = await client.get(
                        f"{sr_url.rstrip('/')}/subjects/{subject}/versions/latest",
                        auth=(sr_key, sr_secret), timeout=5.0,
                    )
                    if resp.status_code == 200:
                        body = resp.json()
                        entry["version"]   = body.get("version")
                        entry["schema_id"] = body.get("id")
                except httpx.HTTPError:
                    pass  # leave version/schema_id as None
            out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Create (called by pipeline)
# ---------------------------------------------------------------------------
@app.post("/recommendations", response_model=Recommendation, status_code=201)
async def create_recommendation(req: CreateRecommendationRequest):
    """
    Upsert a recommendation from the pipeline.
    If the same (topic, field_path, proposed_tag) is still PENDING, the
    record is updated only if the new confidence is higher.
    """
    return await store.create_or_update(req)


# ---------------------------------------------------------------------------
# Approve (= stage) / unstage / reject
# ---------------------------------------------------------------------------
# Approving a recommendation moves it to STAGED — NOT directly applied to
# Stream Catalog. Staged items accumulate at the bottom of the review UI;
# the user clicks "Submit all" to push the entire batch to SR in a single
# POST. This avoids creating one new schema version per tag.
@app.post("/recommendations/{rec_id}/approve", response_model=Recommendation)
async def approve(rec_id: str, reviewed_by: Optional[str] = Query(None)):
    rec = await store.get(rec_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    if rec.status != RecommendationStatus.PENDING:
        raise HTTPException(status_code=409, detail=f"Recommendation is already {rec.status}")
    return await store.update_status(rec_id, RecommendationStatus.STAGED, reviewed_by)


@app.post("/recommendations/{rec_id}/unstage", response_model=Recommendation)
async def unstage(rec_id: str, reviewed_by: Optional[str] = Query(None)):
    """Move a STAGED recommendation back to PENDING (user changed their mind)."""
    rec = await store.get(rec_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    if rec.status != RecommendationStatus.STAGED:
        raise HTTPException(status_code=409, detail=f"Recommendation is {rec.status}, only STAGED can be unstaged")
    return await store.update_status(rec_id, RecommendationStatus.PENDING, reviewed_by)


@app.delete("/recommendations")
async def delete_recommendations_by_topic(topic: str = Query(..., description="Topic to wipe")):
    """Delete every recommendation for a topic. Returns count deleted.

    Called by the wizard at the start of each demo run so the review UI
    matches the freshly-built schema for that topic. No-op if topic
    parameter is missing — refuses to wipe everything by accident.
    """
    if not topic:
        raise HTTPException(status_code=400, detail="topic query param required")
    count = await store.delete_by_topic(topic)
    logger.info("delete_recommendations_by_topic: topic=%s deleted=%d", topic, count)
    return {"topic": topic, "deleted": count}


@app.post("/recommendations/{rec_id}/reject", response_model=Recommendation)
async def reject(rec_id: str, reviewed_by: Optional[str] = Query(None)):
    rec = await store.get(rec_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    # Allow rejecting from PENDING or STAGED — user may stage then change mind.
    if rec.status not in (RecommendationStatus.PENDING, RecommendationStatus.STAGED):
        raise HTTPException(status_code=409, detail=f"Recommendation is already {rec.status}")

    return await store.update_status(rec_id, RecommendationStatus.REJECTED, reviewed_by)


# ---------------------------------------------------------------------------
# Submit staged (batch push to Stream Catalog)
# ---------------------------------------------------------------------------
# Single-flight lock around the read-staged → POST-catalog → flip-status
# sequence. Without this, two concurrent /submit-staged calls each fetch the
# same STAGED set and POST the same payload to Atlas (double tag application,
# double schema-version bump). One request at a time is plenty for review UX.
_submit_lock = asyncio.Lock()


async def _submit_staged_impl(reviewed_by: Optional[str]) -> List[Recommendation]:
    """Push every STAGED recommendation to Stream Catalog in a single POST.

    Extracted from the FastAPI endpoint so other endpoints (bulk-approve)
    can call it without going through Query() default-binding gymnastics.

    Items whose field_path no longer matches the live SR schema are
    auto-REJECTED (with reviewed_by="auto: <reason>") instead of being
    silently dropped — the reviewer sees what happened in the UI.
    """
    async with _submit_lock:
        return await _submit_staged_locked(reviewed_by)


async def _submit_staged_locked(reviewed_by: Optional[str]) -> List[Recommendation]:
    staged = await store.list_recommendations(status=RecommendationStatus.STAGED.value)
    if not staged:
        return []

    items = [{
        "subject":     r.subject,
        "schema_id":   r.schema_id,
        "field_path":  r.field_path,
        "tag_name":    r.proposed_tag,
        "entity_type": r.entity_type,
    } for r in staged]

    ok, err, dropped = await apply_tags_batch(items)

    # Auto-reject any items the SR-validation gate rejected. We do this
    # whether the overall batch succeeded or not — dropped items never
    # made it to the catalog and shouldn't keep clogging the staged queue.
    dropped_index = {(d["subject"], d["field_path"]): d["reason"] for d in dropped}
    rejected_ids: set[str] = set()
    for r in staged:
        reason = dropped_index.get((r.subject, r.field_path))
        if reason is None:
            continue
        try:
            await store.update_status(
                r.id, RecommendationStatus.REJECTED, f"auto: {reason}"
            )
            rejected_ids.add(r.id)
        except Exception as exc:    # noqa: BLE001
            logger.warning("Failed to mark dropped rec %s REJECTED: %s", r.id, exc)

    if not ok:
        raise HTTPException(
            status_code=502,
            detail=f"Batch submit failed: {err}"
                   + (f" ({len(dropped)} item(s) auto-rejected)" if dropped else ""),
        )

    # Flip every non-dropped staged item to APPROVED. If one DB update fails
    # the rest still complete — the SR push already happened so we can't
    # roll it back; best to record what we know.
    updated: list[Recommendation] = []
    for r in staged:
        if r.id in rejected_ids:
            continue
        try:
            updated.append(await store.update_status(
                r.id, RecommendationStatus.APPROVED, reviewed_by
            ))
        except Exception as exc:    # noqa: BLE001
            logger.warning("Failed to mark %s APPROVED after SR push: %s", r.id, exc)
    logger.info(
        "Submitted %d staged recommendations to Stream Catalog (%d auto-rejected pre-flight)",
        len(updated), len(rejected_ids),
    )
    return updated


@app.post("/recommendations/submit-staged", response_model=List[Recommendation])
async def submit_staged(reviewed_by: Optional[str] = Query(None)):
    """Push every currently-STAGED recommendation to Stream Catalog in a
    SINGLE POST, then mark them all APPROVED.

    All-or-nothing: if the batch POST fails, NO items are marked APPROVED.
    """
    try:
        return await _submit_staged_impl(reviewed_by)
    except CatalogNotConfiguredError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# Validate STAGED items against the live SR schema (UI preview)
# ---------------------------------------------------------------------------
@app.get("/recommendations/validate-staged")
async def validate_staged():
    """Read-only preview: which STAGED items would be rejected on submit?

    Iterates STAGED recs, fetches each subject's live SR schema, and reports
    items whose field_path is not in the schema (or whose schema we couldn't
    fetch). The UI calls this to flag stale rows BEFORE the user clicks
    Submit. No DB writes.
    """
    missing = [v for v in REQUIRED_SR_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise HTTPException(
            status_code=503,
            detail="Stream Catalog not configured — missing env vars: " + ", ".join(missing),
        )
    staged = await store.list_recommendations(status=RecommendationStatus.STAGED.value)
    sr_url = os.environ["CONFLUENT_SR_URL"].rstrip("/")
    auth = (os.environ["CONFLUENT_SR_API_KEY"], os.environ["CONFLUENT_SR_API_SECRET"])

    valid_ids: list[str] = []
    stale: list[dict] = []

    async with httpx.AsyncClient() as client:
        # Resolve each (subject, schema_id) → version once. Mirror submit-staged's
        # full criterion: version exists AND field paths fetched AND record
        # qualifier extractable AND clean_path in valid set. If any check fails
        # the row would be auto-rejected on submit; flag it stale here so the UI
        # warns BEFORE the user clicks Submit.
        version_cache: dict[tuple[str, Optional[int]], Optional[int]] = {}
        for r in staged:
            v_key = (r.subject, r.schema_id)
            if v_key not in version_cache:
                version_cache[v_key] = await _resolve_version(
                    client, sr_url, auth, r.subject, r.schema_id
                )
            version = version_cache[v_key]
            if version is None:
                stale.append({
                    "id": r.id, "subject": r.subject, "field_path": r.field_path,
                    "reason": "could not resolve schema version in SR",
                })
                continue
            paths = await fetch_field_paths_cached(client, sr_url, auth, r.subject, version)
            sid, qualifier = await fetch_schema_meta_cached(client, sr_url, auth, r.subject, version)
            clean = r.field_path.replace("[", ".").replace("]", "").strip(".")
            if not paths or sid is None or not qualifier:
                stale.append({
                    "id": r.id, "subject": r.subject, "field_path": r.field_path,
                    "reason": "could not fetch/parse SR schema",
                })
            elif clean not in paths:
                stale.append({
                    "id": r.id, "subject": r.subject, "field_path": r.field_path,
                    "reason": "field not in current SR schema",
                })
            else:
                valid_ids.append(r.id)

    return {"checked": len(staged), "valid": valid_ids, "stale": stale}


# ---------------------------------------------------------------------------
# Bulk-approve = stage everything matching the threshold, then submit the batch.
# Goes PENDING→STAGED directly (no PENDING→APPROVED→STAGED dance) so a crash
# between bulk_stage and _submit_staged_impl leaves rows safely STAGED, not
# falsely APPROVED.
# ---------------------------------------------------------------------------
@app.post("/recommendations/bulk-approve", response_model=List[Recommendation])
async def bulk_approve(req: BulkApproveRequest):
    """Stage everything above min_confidence then submit the batch to SR."""
    await store.bulk_stage(
        min_confidence=req.min_confidence,
        topic=req.topic,
        tag=req.tag,
    )
    try:
        return await _submit_staged_impl(None)
    except CatalogNotConfiguredError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ---------------------------------------------------------------------------
# Local review UI — single-file HTML/JS at /
# ---------------------------------------------------------------------------
_UI_INDEX = Path(__file__).parent / "static" / "index.html"


@app.get("/", include_in_schema=False)
async def ui_root():
    return FileResponse(_UI_INDEX, media_type="text/html")
