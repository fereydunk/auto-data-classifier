"""
Tests for the classification taxonomy — 11 DataTags.
No ML dependencies — pure Python.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "classifier-service"))

from classification.taxonomy import (
    DataTag,
    tag_entity,
    ENTITY_TAG,
)


class TestTagEntity:
    @pytest.mark.parametrize("entity_type,expected_tag", [
        # PII
        ("PERSON",               DataTag.PII),
        ("EMAIL_ADDRESS",        DataTag.PII),
        ("PHONE_NUMBER",         DataTag.PII),
        ("USERNAME",             DataTag.PII),
        ("GENDER",               DataTag.PII),
        ("RACE_ETHNICITY",       DataTag.PII),
        # GOVERNMENT_ID
        ("US_SSN",               DataTag.GOVERNMENT_ID),
        ("PASSPORT",             DataTag.GOVERNMENT_ID),
        ("DRIVER_LICENSE",       DataTag.GOVERNMENT_ID),
        ("NIN",                  DataTag.GOVERNMENT_ID),
        ("AU_TFN",               DataTag.GOVERNMENT_ID),
        ("SIN",                  DataTag.GOVERNMENT_ID),
        ("US_ITIN",              DataTag.GOVERNMENT_ID),
        # LOCATION
        ("LOCATION",             DataTag.LOCATION),
        ("IP_ADDRESS",           DataTag.LOCATION),
        # PHI
        ("MEDICAL_RECORD",       DataTag.PHI),
        ("MEDICAL_CONDITION",    DataTag.PHI),
        ("MEDICATION",           DataTag.PHI),
        ("NATIONAL_PROVIDER_ID", DataTag.PHI),
        ("DEA_NUMBER",           DataTag.PHI),
        ("HEALTH_INSURANCE",     DataTag.PHI),
        # BIOMETRIC
        ("BIOMETRIC",            DataTag.BIOMETRIC),
        # GENETIC
        ("GENETIC",              DataTag.GENETIC),
        # PCI
        ("CREDIT_CARD",          DataTag.PCI),
        ("IBAN_CODE",            DataTag.PCI),
        ("SWIFT_CODE",           DataTag.PCI),
        ("CRYPTO_WALLET",        DataTag.PCI),
        # FINANCIAL
        ("BANK_ACCOUNT",         DataTag.FINANCIAL),
        ("US_BANK_ROUTING",      DataTag.FINANCIAL),
        # CREDENTIALS
        ("PASSWORD",             DataTag.CREDENTIALS),
        ("API_KEY",              DataTag.CREDENTIALS),
        ("JWT_TOKEN",            DataTag.CREDENTIALS),
        ("AWS_ACCESS_KEY",       DataTag.CREDENTIALS),
        ("CONNECTION_STRING",    DataTag.CREDENTIALS),
        # NPI (Non-Public Information)
        ("INSIDER_INFO",         DataTag.NPI),
        ("EARNINGS_DATA",        DataTag.NPI),
        ("MERGER_ACQUISITION",   DataTag.NPI),
        # MINOR
        ("MINOR_DATA",           DataTag.MINOR),
    ])
    def test_known_entities(self, entity_type, expected_tag):
        assert tag_entity(entity_type) == expected_tag

    def test_unknown_entity_defaults_to_pii(self):
        assert tag_entity("SOME_UNKNOWN_TYPE") == DataTag.PII

    def test_empty_string_defaults_to_pii(self):
        assert tag_entity("") == DataTag.PII


class TestDataTagEnum:
    def test_all_11_tags_defined(self):
        expected = {
            "PII", "PHI", "PCI", "CREDENTIALS", "FINANCIAL",
            "GOVERNMENT_ID", "BIOMETRIC", "GENETIC", "NPI", "LOCATION", "MINOR",
        }
        assert {t.value for t in DataTag} == expected

    def test_entity_tag_values_are_valid_datatags(self):
        for entity_type, tag in ENTITY_TAG.items():
            assert isinstance(tag, DataTag), (
                f"Entity '{entity_type}' maps to invalid tag: {tag}"
            )

    def test_no_old_categories_present(self):
        old_names = {"CONFIDENTIAL", "INTERNAL", "SENSITIVE"}
        for tag in DataTag:
            assert tag.value not in old_names
