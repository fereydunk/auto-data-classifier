"""
Auto Data Classification Service
Presidio + GLiNER — runs 100% locally, no external calls at runtime.

Domain-agnostic: works for retail, healthcare, finance, HR, logistics, etc.
Classification is driven by the taxonomy (classification/taxonomy.py),
not by hard-coded industry assumptions.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from classification.taxonomy import (
    DataCategory,
    SensitivityLevel,
    categorize_entity,
    sensitivity_for_categories,
)
from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.pci_recognizers import get_pci_recognizers
from recognizers.phi_recognizers import get_phi_recognizers
from recognizers.credentials_recognizers import get_credentials_recognizers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLASSIFIER_VERSION = "2.0.0"


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
    description="Domain-agnostic data classification: PII, PHI, PCI, Credentials, and more.",
    version=CLASSIFIER_VERSION,
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------
class FieldClassification(BaseModel):
    entity_type: str
    category: str           # DataCategory value, e.g. "PII", "PHI", "PCI"
    score: float
    start: int
    end: int
    text_snippet: str


class ClassifyRequest(BaseModel):
    fields: Dict[str, Any]  # field_name → value (any JSON type, nested ok)
    language: str = "en"


class ClassifyResponse(BaseModel):
    sensitivity_level: str           # SensitivityLevel value
    categories: List[str]            # distinct DataCategory values found
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
    all_categories: set[DataCategory] = set()

    for field_name, text in flat_fields.items():
        results = analyzer.analyze(text=text, language=request.language)
        if results:
            field_entries = []
            for r in results:
                category = categorize_entity(r.entity_type)
                all_categories.add(category)
                field_entries.append(
                    FieldClassification(
                        entity_type=r.entity_type,
                        category=category.value,
                        score=round(r.score, 4),
                        start=r.start,
                        end=r.end,
                        text_snippet=text[r.start:r.end],
                    )
                )
            detected[field_name] = field_entries

    sensitivity = sensitivity_for_categories(all_categories)

    return ClassifyResponse(
        sensitivity_level=sensitivity.value,
        categories=sorted(c.value for c in all_categories),
        detected_entities=detected,
        classified_at=datetime.now(timezone.utc).isoformat(),
        classifier_version=CLASSIFIER_VERSION,
    )


@app.get("/health")
async def health():
    return {"status": "ok", "analyzer_ready": analyzer is not None, "version": CLASSIFIER_VERSION}
