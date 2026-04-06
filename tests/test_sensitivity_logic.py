"""
Tests for sensitivity level logic — now delegates to classification/taxonomy.py.
Kept as a focused integration check that the taxonomy wiring is correct end-to-end.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "classifier-service"))

import pytest
from classification.taxonomy import (
    DataCategory,
    SensitivityLevel,
    categorize_entity,
    sensitivity_for_categories,
)


def sensitivity_from_entity_types(entity_types: set) -> str:
    """Convenience: go from raw entity type strings → sensitivity level string."""
    categories = {categorize_entity(e) for e in entity_types}
    return sensitivity_for_categories(categories).value


@pytest.mark.parametrize("entities,expected", [
    # CRITICAL — PHI or CREDENTIALS
    ({"MEDICAL_RECORD"},            "CRITICAL"),
    ({"PASSWORD"},                  "CRITICAL"),
    ({"API_KEY"},                   "CRITICAL"),
    ({"MEDICAL_CONDITION", "PII"},  "CRITICAL"),
    # HIGH — PII or PCI
    ({"US_SSN"},                    "HIGH"),
    ({"CREDIT_CARD"},               "HIGH"),
    ({"IBAN_CODE"},                 "HIGH"),
    ({"PERSON"},                    "HIGH"),
    ({"EMAIL_ADDRESS"},             "HIGH"),
    ({"PHONE_NUMBER"},              "HIGH"),
    ({"PERSON", "US_SSN"},          "HIGH"),
    # MEDIUM — CONFIDENTIAL only
    ({"ORGANIZATION"},              "MEDIUM"),
    ({"CONTRACT_NUMBER"},           "MEDIUM"),
    # LOW — INTERNAL only
    ({"ORDER_NUMBER"},              "LOW"),
    ({"LOYALTY_CARD"},              "LOW"),
    # CLEAN
    (set(),                         "CLEAN"),
])
def test_sensitivity_from_entity_types(entities, expected):
    assert sensitivity_from_entity_types(entities) == expected


def test_phi_beats_pci():
    assert sensitivity_from_entity_types({"MEDICAL_RECORD", "CREDIT_CARD"}) == "CRITICAL"


def test_credentials_beats_pii():
    assert sensitivity_from_entity_types({"PASSWORD", "PERSON"}) == "CRITICAL"


def test_empty_is_clean():
    assert sensitivity_from_entity_types(set()) == "CLEAN"
