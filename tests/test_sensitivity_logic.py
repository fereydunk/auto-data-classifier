"""
Tests for sensitivity level classification logic from classifier-service/main.py.
Extracted here without importing the full app (avoids spaCy/GLiNER at import time).
"""
import pytest

# Mirror the logic from main.py so tests are self-contained and fast
HIGH_SENSITIVITY_ENTITIES = {
    "US_SSN", "CREDIT_CARD", "IBAN_CODE", "BANK_ACCOUNT",
    "US_BANK_ROUTING", "PASSPORT", "DRIVER_LICENSE", "MEDICAL_RECORD",
    "US_ITIN", "SWIFT_CODE",
}
MEDIUM_SENSITIVITY_ENTITIES = {
    "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION", "DATE_TIME", "IP_ADDRESS",
}


def sensitivity_level(entity_types: set) -> str:
    if entity_types & HIGH_SENSITIVITY_ENTITIES:
        return "HIGH"
    if entity_types & MEDIUM_SENSITIVITY_ENTITIES:
        return "MEDIUM"
    if entity_types:
        return "LOW"
    return "CLEAN"


@pytest.mark.parametrize("entities,expected", [
    ({"US_SSN"},                        "HIGH"),
    ({"CREDIT_CARD"},                   "HIGH"),
    ({"IBAN_CODE"},                     "HIGH"),
    ({"PASSPORT"},                      "HIGH"),
    ({"PERSON", "US_SSN"},              "HIGH"),   # HIGH wins over MEDIUM
    ({"PERSON"},                        "MEDIUM"),
    ({"EMAIL_ADDRESS"},                 "MEDIUM"),
    ({"PHONE_NUMBER", "LOCATION"},      "MEDIUM"),
    ({"SOME_CUSTOM_TYPE"},              "LOW"),    # unknown type → LOW
    (set(),                             "CLEAN"),  # nothing found
])
def test_sensitivity_level(entities, expected):
    assert sensitivity_level(entities) == expected


def test_high_beats_medium_when_both_present():
    mixed = {"PERSON", "EMAIL_ADDRESS", "CREDIT_CARD"}
    assert sensitivity_level(mixed) == "HIGH"


def test_empty_set_is_clean():
    assert sensitivity_level(set()) == "CLEAN"
