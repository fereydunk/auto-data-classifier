from __future__ import annotations

from typing import List, Optional

from gliner import GLiNER
from presidio_analyzer import EntityRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts

# ---------------------------------------------------------------------------
# GLiNER natural-language label → Presidio entity type
#
# GLiNER is a zero-shot NER model: labels are plain English phrases.
# Grouped by DataTag for readability — adding new verticals means
# appending entries here and registering the entity type in taxonomy.py.
# ---------------------------------------------------------------------------
# fmt: off
GLINER_ENTITY_MAP: dict[str, str] = {

    # ── PII — personal identifiers ───────────────────────────────────────────
    "person":                               "PERSON",
    "full name":                            "PERSON",
    "first name":                           "PERSON",
    "last name":                            "PERSON",
    "email address":                        "EMAIL_ADDRESS",
    "phone number":                         "PHONE_NUMBER",
    "mobile number":                        "PHONE_NUMBER",
    "date of birth":                        "DATE_TIME",
    "birthday":                             "DATE_TIME",
    "gender":                               "GENDER",
    "nationality":                          "NATIONALITY",
    "religion":                             "RELIGION",
    "race":                                 "RACE_ETHNICITY",
    "ethnicity":                            "RACE_ETHNICITY",
    "username":                             "USERNAME",
    "employee id":                          "EMPLOYEE_ID",
    "employee number":                      "EMPLOYEE_ID",
    "customer id":                          "CUSTOMER_ID",
    "member id":                            "CUSTOMER_ID",

    # ── GOVERNMENT_ID — government-issued identifiers ────────────────────────
    "social security number":               "US_SSN",
    "ssn":                                  "US_SSN",
    "national insurance number":            "NIN",
    "social insurance number":              "SIN",
    "tax file number":                      "AU_TFN",
    "individual taxpayer identification":   "US_ITIN",
    "tax id":                               "US_ITIN",
    "passport number":                      "PASSPORT",
    "driver's license":                     "DRIVER_LICENSE",
    "driver license number":                "DRIVER_LICENSE",

    # ── LOCATION — geolocation and network identifiers ───────────────────────
    "home address":                         "LOCATION",
    "street address":                       "LOCATION",
    "mailing address":                      "LOCATION",
    "gps coordinates":                      "LOCATION",
    "geolocation":                          "LOCATION",
    "ip address":                           "IP_ADDRESS",

    # ── PHI — protected health information ──────────────────────────────────
    "medical record number":                "MEDICAL_RECORD",
    "health insurance number":              "HEALTH_INSURANCE",
    "insurance member id":                  "HEALTH_INSURANCE",
    "health plan id":                       "HEALTH_INSURANCE",
    "diagnosis":                            "MEDICAL_CONDITION",
    "medical condition":                    "MEDICAL_CONDITION",
    "disease":                              "MEDICAL_CONDITION",
    "medication":                           "MEDICATION",
    "prescription":                         "MEDICATION",
    "drug name":                            "MEDICATION",
    "national provider identifier":         "NATIONAL_PROVIDER_ID",
    "npi number":                           "NATIONAL_PROVIDER_ID",
    "dea number":                           "DEA_NUMBER",

    # ── BIOMETRIC ────────────────────────────────────────────────────────────
    "biometric data":                       "BIOMETRIC",
    "fingerprint":                          "BIOMETRIC",
    "facial recognition data":              "BIOMETRIC",
    "retinal scan":                         "BIOMETRIC",
    "voice print":                          "BIOMETRIC",

    # ── GENETIC ──────────────────────────────────────────────────────────────
    "dna sequence":                         "GENETIC",
    "genetic data":                         "GENETIC",
    "genome":                               "GENETIC",
    "genomic data":                         "GENETIC",
    "genotype":                             "GENETIC",

    # ── PCI — payment card network data ─────────────────────────────────────
    "credit card number":                   "CREDIT_CARD",
    "debit card number":                    "CREDIT_CARD",
    "payment card number":                  "CREDIT_CARD",
    "iban":                                 "IBAN_CODE",
    "swift code":                           "SWIFT_CODE",
    "bic code":                             "SWIFT_CODE",
    "cryptocurrency wallet":                "CRYPTO_WALLET",
    "bitcoin address":                      "CRYPTO_WALLET",
    "ethereum address":                     "CRYPTO_WALLET",

    # ── FINANCIAL — bank account and financial records ───────────────────────
    "bank account number":                  "BANK_ACCOUNT",
    "routing number":                       "US_BANK_ROUTING",
    "aba routing number":                   "US_BANK_ROUTING",

    # ── CREDENTIALS — authentication secrets ────────────────────────────────
    "password":                             "PASSWORD",
    "api key":                              "API_KEY",
    "secret key":                           "SECRET_KEY",
    "access token":                         "ACCESS_TOKEN",
    "bearer token":                         "ACCESS_TOKEN",
    "private key":                          "PRIVATE_KEY",
    "connection string":                    "CONNECTION_STRING",
    "database password":                    "PASSWORD",

    # ── NPI — Non-Public Information (financial / securities) ────────────────
    "insider information":                  "INSIDER_INFO",
    "material non-public information":      "INSIDER_INFO",
    "mnpi":                                 "INSIDER_INFO",
    "earnings forecast":                    "EARNINGS_DATA",
    "earnings before announcement":         "EARNINGS_DATA",
    "pre-release earnings":                 "EARNINGS_DATA",
    "merger information":                   "MERGER_ACQUISITION",
    "acquisition data":                     "MERGER_ACQUISITION",
    "m&a information":                      "MERGER_ACQUISITION",

    # ── MINOR — data relating to children ────────────────────────────────────
    "child's data":                         "MINOR_DATA",
    "minor's information":                  "MINOR_DATA",
    "children's data":                      "MINOR_DATA",
    "data about a minor":                   "MINOR_DATA",
}
# fmt: on


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
            supported_entities = list(set(GLINER_ENTITY_MAP.values()))

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
        if not text or not text.strip():
            return []

        predictions = self.gliner_model.predict_entities(
            text,
            self._gliner_labels,
            threshold=self.threshold,
        )

        results = []
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
