"""
SQLite-backed recommendation store.

Upsert logic: if the same (topic, field_path, proposed_tag) comes in again
while still PENDING, we keep whichever record has the higher confidence.
This prevents duplicate review items when the same field pattern appears
in many messages.
"""

import uuid
from datetime import datetime, timezone
from typing import List, Optional

import aiosqlite

from models import (
    Recommendation,
    RecommendationStatus,
    CreateRecommendationRequest,
    RecommendationSummary,
    confidence_tier,
)

DB_PATH = "/data/recommendations.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS recommendations (
    id               TEXT PRIMARY KEY,
    topic            TEXT NOT NULL,
    subject          TEXT NOT NULL,
    schema_id        INTEGER,
    field_path       TEXT NOT NULL,
    proposed_tag     TEXT NOT NULL,
    entity_type      TEXT NOT NULL,
    confidence       REAL NOT NULL,
    confidence_tier  TEXT NOT NULL,
    layer            INTEGER NOT NULL DEFAULT 3,
    source           TEXT NOT NULL DEFAULT 'ai_model',
    is_free_text     INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    created_at       TEXT NOT NULL,
    reviewed_at      TEXT,
    reviewed_by      TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_rec
    ON recommendations(topic, field_path, proposed_tag);
"""

_SELECT = """
    SELECT id, topic, subject, schema_id, field_path, proposed_tag,
           entity_type, confidence, confidence_tier, layer, source,
           is_free_text, status, created_at, reviewed_at, reviewed_by
    FROM recommendations
"""


def _row_to_rec(row) -> Recommendation:
    return Recommendation(
        id=row[0], topic=row[1], subject=row[2], schema_id=row[3],
        field_path=row[4], proposed_tag=row[5], entity_type=row[6],
        confidence=row[7], confidence_tier=row[8], layer=row[9],
        source=row[10], is_free_text=bool(row[11]), status=row[12],
        created_at=row[13], reviewed_at=row[14], reviewed_by=row[15],
    )


class RecommendationStore:
    def __init__(self, db_path: str = DB_PATH):
        self._db_path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self._db_path) as db:
            await db.executescript(_SCHEMA)
            await db.commit()

    # -------------------------------------------------------------------------
    # Write
    # -------------------------------------------------------------------------
    async def create_or_update(self, req: CreateRecommendationRequest) -> Recommendation:
        """
        Insert a new recommendation, or update the existing PENDING one
        if the new confidence is higher.
        """
        now = datetime.now(timezone.utc).isoformat()
        tier = confidence_tier(req.confidence).value

        async with aiosqlite.connect(self._db_path) as db:
            # Check if a PENDING record already exists for this (topic, field, tag)
            async with db.execute(
                "SELECT id, confidence FROM recommendations "
                "WHERE topic=? AND field_path=? AND proposed_tag=? AND status='PENDING'",
                (req.topic, req.field_path, req.proposed_tag),
            ) as cur:
                existing = await cur.fetchone()

            if existing:
                existing_id, existing_conf = existing
                if req.confidence > existing_conf:
                    # New detection has higher confidence — update in place
                    await db.execute(
                        "UPDATE recommendations SET confidence=?, confidence_tier=?, "
                        "entity_type=?, schema_id=?, layer=?, source=? WHERE id=?",
                        (req.confidence, tier, req.entity_type, req.schema_id,
                         req.layer, req.source, existing_id),
                    )
                    await db.commit()
                # Return the (possibly updated) record
                async with db.execute(
                    f"{_SELECT} WHERE id=?", (existing_id,)
                ) as cur:
                    return _row_to_rec(await cur.fetchone())

            # New recommendation
            rec_id = str(uuid.uuid4())
            await db.execute(
                "INSERT INTO recommendations "
                "(id, topic, subject, schema_id, field_path, proposed_tag, "
                " entity_type, confidence, confidence_tier, layer, source, "
                " is_free_text, status, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rec_id, req.topic, req.subject, req.schema_id, req.field_path,
                 req.proposed_tag, req.entity_type, req.confidence, tier,
                 req.layer, req.source, int(req.is_free_text),
                 RecommendationStatus.PENDING.value, now),
            )
            await db.commit()

            async with db.execute(f"{_SELECT} WHERE id=?", (rec_id,)) as cur:
                return _row_to_rec(await cur.fetchone())

    async def update_status(
        self,
        rec_id: str,
        status: RecommendationStatus,
        reviewed_by: Optional[str] = None,
    ) -> Optional[Recommendation]:
        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "UPDATE recommendations SET status=?, reviewed_at=?, reviewed_by=? WHERE id=?",
                (status.value, now, reviewed_by, rec_id),
            )
            await db.commit()
            async with db.execute(f"{_SELECT} WHERE id=?", (rec_id,)) as cur:
                row = await cur.fetchone()
                return _row_to_rec(row) if row else None

    async def bulk_approve(
        self,
        min_confidence: float,
        topic: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> List[Recommendation]:
        """Mark all PENDING recommendations above min_confidence as APPROVED."""
        now = datetime.now(timezone.utc).isoformat()
        conditions = ["status='PENDING'", "confidence>=?"]
        params: list = [min_confidence]

        if topic:
            conditions.append("topic=?")
            params.append(topic)
        if tag:
            conditions.append("proposed_tag=?")
            params.append(tag)

        where = " AND ".join(conditions)

        async with aiosqlite.connect(self._db_path) as db:
            # Fetch the matching IDs first
            async with db.execute(
                f"SELECT id FROM recommendations WHERE {where}", params
            ) as cur:
                ids = [row[0] for row in await cur.fetchall()]

            if ids:
                placeholders = ",".join("?" * len(ids))
                await db.execute(
                    f"UPDATE recommendations SET status='APPROVED', reviewed_at=? "
                    f"WHERE id IN ({placeholders})",
                    [now] + ids,
                )
                await db.commit()

            # Return the updated records
            if not ids:
                return []
            async with db.execute(
                f"{_SELECT} WHERE id IN ({','.join('?' * len(ids))})", ids
            ) as cur:
                return [_row_to_rec(row) for row in await cur.fetchall()]

    # -------------------------------------------------------------------------
    # Read
    # -------------------------------------------------------------------------
    async def get(self, rec_id: str) -> Optional[Recommendation]:
        async with aiosqlite.connect(self._db_path) as db:
            async with db.execute(f"{_SELECT} WHERE id=?", (rec_id,)) as cur:
                row = await cur.fetchone()
                return _row_to_rec(row) if row else None

    async def list_recommendations(
        self,
        status: Optional[str] = None,
        topic: Optional[str] = None,
        tag: Optional[str] = None,
        tier: Optional[str] = None,
    ) -> List[Recommendation]:
        conditions, params = [], []

        if status:
            conditions.append("status=?")
            params.append(status)
        if topic:
            conditions.append("topic=?")
            params.append(topic)
        if tag:
            conditions.append("proposed_tag=?")
            params.append(tag)
        if tier:
            conditions.append("confidence_tier=?")
            params.append(tier)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"{_SELECT} {where} ORDER BY confidence DESC"

        async with aiosqlite.connect(self._db_path) as db:
            async with db.execute(sql, params) as cur:
                return [_row_to_rec(row) for row in await cur.fetchall()]

    async def summary(self) -> List[RecommendationSummary]:
        sql = """
            SELECT topic,
                   SUM(CASE WHEN status='PENDING'  THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status='REJECTED' THEN 1 ELSE 0 END)
            FROM recommendations
            GROUP BY topic
            ORDER BY topic
        """
        async with aiosqlite.connect(self._db_path) as db:
            async with db.execute(sql) as cur:
                return [
                    RecommendationSummary(
                        topic=row[0], pending=row[1], approved=row[2], rejected=row[3]
                    )
                    for row in await cur.fetchall()
                ]
