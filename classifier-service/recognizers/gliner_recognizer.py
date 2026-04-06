from __future__ import annotations

from typing import List, Optional

from gliner import GLiNER
from presidio_analyzer import EntityRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts

# Mapping from GLiNER labels → Presidio entity types
GLINER_ENTITY_MAP = {
    "person":                  "PERSON",
    "email address":           "EMAIL_ADDRESS",
    "phone number":            "PHONE_NUMBER",
    "address":                 "LOCATION",
    "date of birth":           "DATE_TIME",
    "credit card number":      "CREDIT_CARD",
    "social security number":  "US_SSN",
    "bank account number":     "BANK_ACCOUNT",
    "passport number":         "PASSPORT",
    "driver's license":        "DRIVER_LICENSE",
    "ip address":              "IP_ADDRESS",
    "medical record number":   "MEDICAL_RECORD",
    "tax id":                  "US_ITIN",
    "iban":                    "IBAN_CODE",
    "swift code":              "SWIFT_CODE",
    "routing number":          "US_BANK_ROUTING",
}


class GLiNERRecognizer(EntityRecognizer):
    """Presidio-compatible NER recognizer backed by a local GLiNER model."""

    def __init__(
        self,
        model_name: str = "urchade/gliner_medium-v2.1",
        supported_language: str = "en",
        supported_entities: Optional[List[str]] = None,
        threshold: float = 0.4,
    ):
        self.gliner_model = GLiNER.from_pretrained(model_name)
        self.threshold = threshold
        self._gliner_labels = list(GLINER_ENTITY_MAP.keys())

        if supported_entities is None:
            supported_entities = list(GLINER_ENTITY_MAP.values())

        super().__init__(
            supported_entities=supported_entities,
            supported_language=supported_language,
            name="GLiNERRecognizer",
        )

    def load(self) -> None:
        pass  # model loaded in __init__

    def analyze(
        self,
        text: str,
        entities: List[str],
        nlp_artifacts: Optional[NlpArtifacts] = None,
    ) -> List[RecognizerResult]:
        results = []
        if not text or not text.strip():
            return results

        predictions = self.gliner_model.predict_entities(
            text,
            self._gliner_labels,
            threshold=self.threshold,
        )

        for pred in predictions:
            presidio_type = GLINER_ENTITY_MAP.get(pred["label"])
            if presidio_type and presidio_type in entities:
                results.append(
                    RecognizerResult(
                        entity_type=presidio_type,
                        start=pred["start"],
                        end=pred["end"],
                        score=pred["score"],
                    )
                )
        return results
