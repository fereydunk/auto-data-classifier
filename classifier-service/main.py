"""
Auto Data Classification Service
Presidio + GLiNER — runs 100% locally, no external calls at runtime.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.banking_recognizers import get_banking_recognizers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sensitivity levels
# ---------------------------------------------------------------------------
HIGH_SENSITIVITY_ENTITIES = {
    "US_SSN", "CREDIT_CARD", "IBAN_CODE", "BANK_ACCOUNT",
    "US_BANK_ROUTING", "PASSPORT", "DRIVER_LICENSE", "MEDICAL_RECORD",
    "US_ITIN", "SWIFT_CODE",
}
MEDIUM_SENSITIVITY_ENTITIES = {
    "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION", "DATE_TIME", "IP_ADDRESS",
}

CLASSIFIER_VERSION = "1.0.0"


def _sensitivity_level(entity_types: set[str]) -> str:
    if entity_types & HIGH_SENSITIVITY_ENTITIES:
        return "HIGH"
    if entity_types & MEDIUM_SENSITIVITY_ENTITIES:
        return "MEDIUM"
    if entity_types:
        return "LOW"
    return "CLEAN"


# ---------------------------------------------------------------------------
# Build the Presidio analyzer with GLiNER + banking recognizers
# ---------------------------------------------------------------------------
def build_analyzer() -> AnalyzerEngine:
    logger.info("Loading NLP engine (spaCy)...")
    nlp_config = {"nlp_engine_name": "spacy", "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}]}
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)

    logger.info("Loading GLiNER model (local)...")
    registry.add_recognizer(GLiNERRecognizer())

    for recognizer in get_banking_recognizers():
        registry.add_recognizer(recognizer)

    logger.info("Analyzer ready.")
    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


app = FastAPI(title="Auto Data Classifier", version=CLASSIFIER_VERSION)
analyzer: Optional[AnalyzerEngine] = None


@app.on_event("startup")
async def startup():
    global analyzer
    analyzer = build_analyzer()


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------
class FieldClassification(BaseModel):
    entity_type: str
    score: float
    start: int
    end: int
    text_snippet: str


class ClassifyRequest(BaseModel):
    fields: Dict[str, Any]          # field_name → value (any JSON type)
    language: str = "en"


class ClassifyResponse(BaseModel):
    sensitivity_level: str          # HIGH | MEDIUM | LOW | CLEAN
    detected_entities: Dict[str, List[FieldClassification]]
    classified_at: str
    classifier_version: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _flatten_fields(obj: Any, prefix: str = "") -> Dict[str, str]:
    """Recursively extract string-valued leaf fields from a nested dict."""
    flat: Dict[str, str] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            flat.update(_flatten_fields(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            flat.update(_flatten_fields(v, f"{prefix}[{i}]"))
    elif isinstance(obj, str) and obj.strip():
        flat[prefix] = obj
    elif obj is not None:
        flat[prefix] = str(obj)
    return flat


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/classify", response_model=ClassifyResponse)
async def classify(request: ClassifyRequest):
    if analyzer is None:
        raise HTTPException(status_code=503, detail="Analyzer not ready")

    flat_fields = _flatten_fields(request.fields)
    detected: Dict[str, List[FieldClassification]] = {}

    for field_name, text in flat_fields.items():
        results = analyzer.analyze(text=text, language=request.language)
        if results:
            detected[field_name] = [
                FieldClassification(
                    entity_type=r.entity_type,
                    score=round(r.score, 4),
                    start=r.start,
                    end=r.end,
                    text_snippet=text[r.start:r.end],
                )
                for r in results
            ]

    found_types = {e.entity_type for entities in detected.values() for e in entities}
    sensitivity = _sensitivity_level(found_types)

    return ClassifyResponse(
        sensitivity_level=sensitivity,
        detected_entities=detected,
        classified_at=datetime.now(timezone.utc).isoformat(),
        classifier_version=CLASSIFIER_VERSION,
    )


@app.get("/health")
async def health():
    return {"status": "ok", "analyzer_ready": analyzer is not None}
