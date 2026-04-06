"""
Auto Data Classification Service — v3.0.0
Presidio + GLiNER — runs 100% locally, no external calls at runtime.

Classifies fields with one of 11 data tags: PII, PHI, PCI, CREDENTIALS,
FINANCIAL, GOVERNMENT_ID, BIOMETRIC, GENETIC, NPI, LOCATION, MINOR.
Sensitivity decisions are left to the operator.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from classification.taxonomy import DataTag, tag_entity, ENTITY_TAG
from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.pci_recognizers import get_pci_recognizers
from recognizers.phi_recognizers import get_phi_recognizers
from recognizers.credentials_recognizers import get_credentials_recognizers
from recognizers.financial_recognizers import get_financial_recognizers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLASSIFIER_VERSION = "3.0.0"


# ---------------------------------------------------------------------------
# Build the Presidio analyzer
# ---------------------------------------------------------------------------
def build_analyzer() -> AnalyzerEngine:
    logger.info("Loading NLP engine (spaCy)...")
    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)

    logger.info("Loading GLiNER model (local)...")
    registry.add_recognizer(GLiNERRecognizer())

    for r in get_pci_recognizers():
        registry.add_recognizer(r)
    for r in get_phi_recognizers():
        registry.add_recognizer(r)
    for r in get_credentials_recognizers():
        registry.add_recognizer(r)
    for r in get_financial_recognizers():
        registry.add_recognizer(r)

    logger.info("Analyzer ready — %d recognizers loaded.", len(registry.recognizers))
    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


analyzer: Optional[AnalyzerEngine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global analyzer
    analyzer = build_analyzer()
    yield


app = FastAPI(
    title="Auto Data Classifier",
    description="Classifies Kafka message fields into 11 data tags: PII, PHI, PCI, CREDENTIALS, FINANCIAL, GOVERNMENT_ID, BIOMETRIC, GENETIC, NPI, LOCATION, MINOR.",
    version=CLASSIFIER_VERSION,
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------
class FieldClassification(BaseModel):
    entity_type: str
    tag: str          # DataTag value, e.g. "PII", "PHI", "GOVERNMENT_ID"
    score: float
    start: int
    end: int
    text_snippet: str


class ClassifyRequest(BaseModel):
    fields: Dict[str, Any]  # field_name → value (any JSON type, nested ok)
    language: str = "en"


class ClassifyResponse(BaseModel):
    tags: List[str]                                          # distinct DataTag values found
    detected_entities: Dict[str, List[FieldClassification]]  # field → entities
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
    all_tags: set[DataTag] = set()

    for field_name, text in flat_fields.items():
        results = analyzer.analyze(text=text, language=request.language)
        if results:
            field_entries = []
            for r in results:
                data_tag = tag_entity(r.entity_type)
                all_tags.add(data_tag)
                field_entries.append(
                    FieldClassification(
                        entity_type=r.entity_type,
                        tag=data_tag.value,
                        score=round(r.score, 4),
                        start=r.start,
                        end=r.end,
                        text_snippet=text[r.start:r.end],
                    )
                )
            detected[field_name] = field_entries

    return ClassifyResponse(
        tags=sorted(t.value for t in all_tags),
        detected_entities=detected,
        classified_at=datetime.now(timezone.utc).isoformat(),
        classifier_version=CLASSIFIER_VERSION,
    )


@app.get("/health")
async def health():
    return {"status": "ok", "analyzer_ready": analyzer is not None, "version": CLASSIFIER_VERSION}
