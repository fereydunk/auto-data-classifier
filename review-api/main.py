"""
Tag Recommendation Review API — v1.0.0

Receives classification recommendations from the pipeline, surfaces them
to data stewards for approval, and applies approved tags to the
Confluent Stream Catalog.

Endpoints:
  GET  /recommendations               List all, sorted by confidence desc
  GET  /recommendations/summary       Pending/approved/rejected counts by topic
  POST /recommendations               Create/upsert one (called by pipeline)
  POST /recommendations/{id}/approve  Apply tag to Stream Catalog
  POST /recommendations/{id}/reject   Dismiss without tagging
  POST /recommendations/bulk-approve  Approve all above a confidence threshold
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from models import (
    BulkApproveRequest,
    CreateRecommendationRequest,
    Recommendation,
    RecommendationStatus,
    RecommendationSummary,
)
from store import RecommendationStore
from catalog_client import apply_tag

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
    status: Optional[str] = Query(None, description="Filter by status: PENDING, APPROVED, REJECTED"),
    topic: Optional[str] = Query(None, description="Filter by Kafka topic"),
    tag: Optional[str] = Query(None, description="Filter by proposed tag, e.g. PII"),
    tier: Optional[str] = Query(None, description="Filter by confidence tier: HIGH, MEDIUM, LOW"),
):
    """
    Returns all recommendations sorted by confidence descending.
    HIGH-confidence items appear first — apply those in bulk, review the rest manually.
    """
    return await store.list_recommendations(status=status, topic=topic, tag=tag, tier=tier)


@app.get("/recommendations/summary", response_model=List[RecommendationSummary])
async def summary():
    """Pending / approved / rejected counts grouped by topic."""
    return await store.summary()


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
# Approve / reject
# ---------------------------------------------------------------------------
@app.post("/recommendations/{rec_id}/approve", response_model=Recommendation)
async def approve(rec_id: str, reviewed_by: Optional[str] = Query(None)):
    rec = await store.get(rec_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    if rec.status != RecommendationStatus.PENDING:
        raise HTTPException(status_code=409, detail=f"Recommendation is already {rec.status}")

    ok = await apply_tag(
        subject=rec.subject,
        schema_id=rec.schema_id,
        field_path=rec.field_path,
        tag_name=rec.proposed_tag,
        entity_type=rec.entity_type,
    )
    if not ok:
        raise HTTPException(status_code=502, detail="Failed to apply tag to Stream Catalog")

    return await store.update_status(rec_id, RecommendationStatus.APPROVED, reviewed_by)


@app.post("/recommendations/{rec_id}/reject", response_model=Recommendation)
async def reject(rec_id: str, reviewed_by: Optional[str] = Query(None)):
    rec = await store.get(rec_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Recommendation not found")
    if rec.status != RecommendationStatus.PENDING:
        raise HTTPException(status_code=409, detail=f"Recommendation is already {rec.status}")

    return await store.update_status(rec_id, RecommendationStatus.REJECTED, reviewed_by)


# ---------------------------------------------------------------------------
# Bulk approve
# ---------------------------------------------------------------------------
@app.post("/recommendations/bulk-approve", response_model=List[Recommendation])
async def bulk_approve(req: BulkApproveRequest):
    """
    Approve all PENDING recommendations at or above min_confidence (default 0.85 = HIGH tier).
    Optionally scope to a specific topic or tag type.
    Tags are applied to the Stream Catalog for each approved recommendation.
    """
    recs = await store.bulk_approve(
        min_confidence=req.min_confidence,
        topic=req.topic,
        tag=req.tag,
    )

    failed = []
    for rec in recs:
        ok = await apply_tag(
            subject=rec.subject,
            schema_id=rec.schema_id,
            field_path=rec.field_path,
            tag_name=rec.proposed_tag,
            entity_type=rec.entity_type,
        )
        if not ok:
            failed.append(rec.field_path)

    if failed:
        logger.warning("Bulk approve: failed to tag %d fields: %s", len(failed), failed)

    return recs


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
