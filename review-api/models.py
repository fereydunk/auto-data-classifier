from enum import Enum
from typing import Optional
from pydantic import BaseModel


class RecommendationStatus(str, Enum):
    PENDING  = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ConfidenceTier(str, Enum):
    HIGH   = "HIGH"    # >= 0.85  — structural matches (IBAN, JWT, AWS key, crypto wallet)
    MEDIUM = "MEDIUM"  # 0.60–0.84 — probable matches (routing numbers, DEA format)
    LOW    = "LOW"     # < 0.60   — ambiguous, always needs human review


def confidence_tier(score: float) -> ConfidenceTier:
    if score >= 0.85:
        return ConfidenceTier.HIGH
    elif score >= 0.60:
        return ConfidenceTier.MEDIUM
    return ConfidenceTier.LOW


class Recommendation(BaseModel):
    id: str
    topic: str
    subject: str
    schema_id: Optional[int]
    field_path: str
    proposed_tag: str
    entity_type: str
    confidence: float
    confidence_tier: str
    layer: int             # 1 = field_name | 2 = regex | 3 = ai_model
    source: str            # "field_name" | "regex" | "ai_model"
    is_free_text: bool     # True when field is generic free-text (comment, notes …)
    status: str
    created_at: str
    reviewed_at: Optional[str] = None
    reviewed_by: Optional[str] = None


class CreateRecommendationRequest(BaseModel):
    topic: str
    subject: str
    schema_id: Optional[int] = None
    field_path: str
    proposed_tag: str
    entity_type: str
    confidence: float
    layer: int = 3
    source: str = "ai_model"
    is_free_text: bool = False


class BulkApproveRequest(BaseModel):
    min_confidence: float = 0.85   # default: approve HIGH tier
    topic: Optional[str] = None    # scope to one topic
    tag: Optional[str] = None      # scope to one tag type


class RecommendationSummary(BaseModel):
    topic: str
    pending: int
    approved: int
    rejected: int
