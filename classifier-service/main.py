"""
Auto Data Classification Service — v3.1.0

Three-layer classification pipeline:
  Layer 1 — field name / schema metadata  (always on, zero data access)
  Layer 2 — regex / structural patterns   (on by default, opt-out available)
  Layer 3 — AI model: spaCy + GLiNER      (on by default, opt-out available)

Customers control depth via max_layer (1 | 2 | 3, default 3).
Each detected entity is tagged with the layer and source that found it.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from classification.taxonomy import DataTag, tag_entity
from recognizers.field_name_recognizer import classify_field_name, is_free_text
from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.pci_recognizers import get_pci_recognizers
from recognizers.phi_recognizers import get_phi_recognizers
from recognizers.credentials_recognizers import get_credentials_recognizers
from recognizers.financial_recognizers import get_financial_recognizers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLASSIFIER_VERSION = "3.1.0"


# ---------------------------------------------------------------------------
# Build analyzers — one per layer
# ---------------------------------------------------------------------------
def build_regex_analyzer() -> AnalyzerEngine:
    """
    Layer 2: pattern/regex recognizers only — no spaCy, no GLiNER.
    Presidio built-in pattern recognizers (email, phone, SSN, credit card …)
    plus all our custom regex recognizers.
    """
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers()   # pattern-only built-ins (no NLP engine passed)

    for r in get_pci_recognizers():
        registry.add_recognizer(r)
    for r in get_phi_recognizers():
        registry.add_recognizer(r)
    for r in get_credentials_recognizers():
        registry.add_recognizer(r)
    for r in get_financial_recognizers():
        registry.add_recognizer(r)

    logger.info("Regex analyzer ready — %d recognizers loaded.", len(registry.recognizers))
    return AnalyzerEngine(registry=registry, supported_languages=["en"])


def build_ai_analyzer() -> AnalyzerEngine:
    """
    Layer 3: spaCy NER + GLiNER — contextual, zero-shot named entity recognition.
    Handles unstructured free-text that regex cannot reliably classify.
    """
    logger.info("Loading spaCy model…")
    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_lg"}],
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)

    logger.info("Loading GLiNER model (local)…")
    registry.add_recognizer(GLiNERRecognizer())

    logger.info("AI analyzer ready — %d recognizers loaded.", len(registry.recognizers))
    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


regex_analyzer: Optional[AnalyzerEngine] = None
ai_analyzer: Optional[AnalyzerEngine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global regex_analyzer, ai_analyzer
    logger.info("Building Layer 2 (regex) analyzer…")
    regex_analyzer = build_regex_analyzer()
    logger.info("Building Layer 3 (AI) analyzer…")
    ai_analyzer = build_ai_analyzer()
    logger.info("All analyzers ready.")
    yield


app = FastAPI(
    title="Auto Data Classifier",
    description=(
        "Three-layer field classification: field name → regex → AI model. "
        "Classifies into 11 data tags. Customers control depth via max_layer."
    ),
    version=CLASSIFIER_VERSION,
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------
class FieldClassification(BaseModel):
    entity_type: str
    tag: str          # DataTag value
    score: float
    start: int
    end: int
    text_snippet: str
    layer: int        # 1 = field_name, 2 = regex, 3 = ai_model
    source: str       # "field_name" | "regex" | "ai_model"
    is_free_text: bool = False  # True when field value looks like unstructured prose


class ClassifyRequest(BaseModel):
    fields: Dict[str, Any]
    language: str = "en"
    max_layer: int = 3          # 1 = field name only | 2 = +regex | 3 = +AI (default)


class ClassifyResponse(BaseModel):
    tags: List[str]
    detected_entities: Dict[str, List[FieldClassification]]
    layers_used: List[int]
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


def _leaf_name(field_path: str) -> str:
    """Extract the leaf field name from a dotted path, e.g. 'customer.email' → 'email'."""
    return field_path.split(".")[-1].strip("[]0123456789")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/classify", response_model=ClassifyResponse)
async def classify(request: ClassifyRequest):
    if regex_analyzer is None or ai_analyzer is None:
        raise HTTPException(status_code=503, detail="Analyzers not ready")

    max_layer = max(1, min(3, request.max_layer))   # clamp to 1–3
    flat_fields = _flatten_fields(request.fields)

    detected: Dict[str, List[FieldClassification]] = {}
    all_tags: set[DataTag] = set()
    layers_used: set[int] = {1}

    for field_path, value in flat_fields.items():
        leaf = _leaf_name(field_path)
        free_text = is_free_text(leaf, value)
        field_entries: List[FieldClassification] = []

        # ── Layer 1: field name ───────────────────────────────────────────────
        for match in classify_field_name(leaf):
            all_tags.add(DataTag(match.tag))
            field_entries.append(FieldClassification(
                entity_type=match.entity_type,
                tag=match.tag,
                score=match.score,
                start=0,
                end=len(value),
                text_snippet=value[:80],
                layer=1,
                source="field_name",
                is_free_text=free_text,
            ))

        # ── Layer 2: regex ────────────────────────────────────────────────────
        if max_layer >= 2:
            layers_used.add(2)
            results = regex_analyzer.analyze(text=value, language=request.language)
            for r in results:
                data_tag = tag_entity(r.entity_type)
                all_tags.add(data_tag)
                field_entries.append(FieldClassification(
                    entity_type=r.entity_type,
                    tag=data_tag.value,
                    score=round(r.score, 4),
                    start=r.start,
                    end=r.end,
                    text_snippet=value[r.start:r.end],
                    layer=2,
                    source="regex",
                    is_free_text=free_text,
                ))

        # ── Layer 3: AI model ─────────────────────────────────────────────────
        if max_layer >= 3:
            layers_used.add(3)
            results = ai_analyzer.analyze(text=value, language=request.language)
            for r in results:
                data_tag = tag_entity(r.entity_type)
                all_tags.add(data_tag)
                field_entries.append(FieldClassification(
                    entity_type=r.entity_type,
                    tag=data_tag.value,
                    score=round(r.score, 4),
                    start=r.start,
                    end=r.end,
                    text_snippet=value[r.start:r.end],
                    layer=3,
                    source="ai_model",
                    is_free_text=free_text,
                ))

        if field_entries:
            detected[field_path] = field_entries

    return ClassifyResponse(
        tags=sorted(t.value for t in all_tags),
        detected_entities=detected,
        layers_used=sorted(layers_used),
        classified_at=datetime.now(timezone.utc).isoformat(),
        classifier_version=CLASSIFIER_VERSION,
    )


@app.get("/health")
async def health():
    ready = regex_analyzer is not None and ai_analyzer is not None
    return {"status": "ok", "analyzer_ready": ready, "version": CLASSIFIER_VERSION}
