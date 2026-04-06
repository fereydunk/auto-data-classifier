"""
Tests for the universal classification taxonomy.
No ML dependencies — pure Python.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "classifier-service"))

from classification.taxonomy import (
    DataCategory,
    SensitivityLevel,
    categorize_entity,
    sensitivity_for_categories,
    ENTITY_CATEGORY,
    CATEGORY_SENSITIVITY,
)


class TestCategorizeEntity:
    @pytest.mark.parametrize("entity_type,expected_category", [
        # PII
        ("PERSON",          DataCategory.PII),
        ("EMAIL_ADDRESS",   DataCategory.PII),
        ("US_SSN",          DataCategory.PII),
        ("PASSPORT",        DataCategory.PII),
        ("NIN",             DataCategory.PII),
        ("DRIVER_LICENSE",  DataCategory.PII),
        # PCI
        ("CREDIT_CARD",     DataCategory.PCI),
        ("IBAN_CODE",       DataCategory.PCI),
        ("BANK_ACCOUNT",    DataCategory.PCI),
        ("CRYPTO_WALLET",   DataCategory.PCI),
        # PHI
        ("MEDICAL_RECORD",  DataCategory.PHI),
        ("MEDICAL_CONDITION",DataCategory.PHI),
        ("MEDICATION",      DataCategory.PHI),
        ("NPI",             DataCategory.PHI),
        ("BIOMETRIC",       DataCategory.PHI),
        # CREDENTIALS
        ("PASSWORD",        DataCategory.CREDENTIALS),
        ("API_KEY",         DataCategory.CREDENTIALS),
        ("JWT_TOKEN",       DataCategory.CREDENTIALS),
        ("AWS_ACCESS_KEY",  DataCategory.CREDENTIALS),
        ("CONNECTION_STRING",DataCategory.CREDENTIALS),
        # CONFIDENTIAL
        ("ORGANIZATION",    DataCategory.CONFIDENTIAL),
        ("CONTRACT_NUMBER", DataCategory.CONFIDENTIAL),
        # INTERNAL
        ("ORDER_NUMBER",    DataCategory.INTERNAL),
        ("LOYALTY_CARD",    DataCategory.INTERNAL),
        ("LICENSE_PLATE",   DataCategory.INTERNAL),
    ])
    def test_known_entities(self, entity_type, expected_category):
        assert categorize_entity(entity_type) == expected_category

    def test_unknown_entity_defaults_to_internal(self):
        assert categorize_entity("SOME_UNKNOWN_TYPE") == DataCategory.INTERNAL

    def test_empty_string_defaults_to_internal(self):
        assert categorize_entity("") == DataCategory.INTERNAL


class TestSensitivityForCategories:
    @pytest.mark.parametrize("categories,expected_level", [
        # CRITICAL wins
        ({DataCategory.PHI},                        SensitivityLevel.CRITICAL),
        ({DataCategory.CREDENTIALS},                SensitivityLevel.CRITICAL),
        ({DataCategory.PHI, DataCategory.PII},      SensitivityLevel.CRITICAL),
        ({DataCategory.CREDENTIALS, DataCategory.PCI}, SensitivityLevel.CRITICAL),
        # HIGH
        ({DataCategory.PII},                        SensitivityLevel.HIGH),
        ({DataCategory.PCI},                        SensitivityLevel.HIGH),
        ({DataCategory.PII, DataCategory.PCI},      SensitivityLevel.HIGH),
        ({DataCategory.PII, DataCategory.CONFIDENTIAL}, SensitivityLevel.HIGH),
        # MEDIUM
        ({DataCategory.CONFIDENTIAL},               SensitivityLevel.MEDIUM),
        # LOW
        ({DataCategory.INTERNAL},                   SensitivityLevel.LOW),
        # CLEAN
        (set(),                                     SensitivityLevel.CLEAN),
    ])
    def test_sensitivity_levels(self, categories, expected_level):
        assert sensitivity_for_categories(categories) == expected_level

    def test_critical_beats_everything(self):
        all_cats = set(DataCategory)
        assert sensitivity_for_categories(all_cats) == SensitivityLevel.CRITICAL

    def test_phi_and_pci_together_is_critical(self):
        assert sensitivity_for_categories({DataCategory.PHI, DataCategory.PCI}) == SensitivityLevel.CRITICAL


class TestTaxonomyCoverage:
    def test_all_categories_have_sensitivity_mapping(self):
        for category in DataCategory:
            assert category in CATEGORY_SENSITIVITY, f"{category} has no sensitivity mapping"

    def test_entity_category_values_are_valid_categories(self):
        for entity_type, category in ENTITY_CATEGORY.items():
            assert isinstance(category, DataCategory), (
                f"Entity '{entity_type}' maps to invalid category: {category}"
            )
