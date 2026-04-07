"""
Tests for Layer 1 — field name classifier.
No data access, no ML dependencies.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "classifier-service"))

from recognizers.field_name_recognizer import classify_field_name, is_free_text


class TestClassifyFieldName:

    @pytest.mark.parametrize("field_name,expected_tag", [
        # GOVERNMENT_ID
        ("ssn",                  "GOVERNMENT_ID"),
        ("social_security",      "GOVERNMENT_ID"),
        ("passport_number",      "GOVERNMENT_ID"),
        ("driver_license",       "GOVERNMENT_ID"),
        ("driverLicense",        "GOVERNMENT_ID"),
        ("nin",                  "GOVERNMENT_ID"),
        ("au_tfn",               "GOVERNMENT_ID"),
        # PHI
        ("mrn",                  "PHI"),
        ("medical_record_id",    "PHI"),
        ("patient_id",           "PHI"),
        ("diagnosis",            "PHI"),
        ("medication",           "PHI"),
        ("npi",                  "PHI"),
        ("dea_number",           "PHI"),
        ("health_insurance_id",  "PHI"),
        # BIOMETRIC
        ("fingerprint",          "BIOMETRIC"),
        ("facial_recognition",   "BIOMETRIC"),
        ("retina_scan",          "BIOMETRIC"),
        # GENETIC
        ("dna_sequence",         "GENETIC"),
        ("genome",               "GENETIC"),
        # PCI
        ("credit_card_number",   "PCI"),
        ("creditCardNumber",     "PCI"),
        ("iban",                 "PCI"),
        ("swift_code",           "PCI"),
        ("crypto_wallet",        "PCI"),
        # FINANCIAL
        ("account_number",       "FINANCIAL"),
        ("bank_account",         "FINANCIAL"),
        ("routing_number",       "FINANCIAL"),
        # CREDENTIALS
        ("password",             "CREDENTIALS"),
        ("api_key",              "CREDENTIALS"),
        ("jwt",                  "CREDENTIALS"),
        ("access_token",         "CREDENTIALS"),
        ("private_key",          "CREDENTIALS"),
        ("connection_string",    "CREDENTIALS"),
        # PII
        ("email",                "PII"),
        ("email_address",        "PII"),
        ("emailAddress",         "PII"),
        ("phone_number",         "PII"),
        ("first_name",           "PII"),
        ("firstName",            "PII"),
        ("date_of_birth",        "PII"),
        ("dob",                  "PII"),
        ("ip_address",           "LOCATION"),
        ("gender",               "PII"),
        ("home_address",         "LOCATION"),
        # MINOR
        ("child_id",             "MINOR"),
        ("minor_data",           "MINOR"),
    ])
    def test_tag_from_field_name(self, field_name, expected_tag):
        results = classify_field_name(field_name)
        assert results, f"No match for field '{field_name}'"
        tags = {r.tag for r in results}
        assert expected_tag in tags, f"Expected {expected_tag} for '{field_name}', got {tags}"

    def test_returns_empty_for_generic_names(self):
        assert classify_field_name("id") == []
        assert classify_field_name("value") == []
        assert classify_field_name("data") == []
        assert classify_field_name("ref") == []

    def test_sorted_by_confidence_desc(self):
        results = classify_field_name("patient_ssn")
        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_camel_case_handling(self):
        assert classify_field_name("creditCardNumber")
        assert classify_field_name("dateOfBirth")
        assert classify_field_name("socialSecurityNumber")

    def test_source_and_layer(self):
        results = classify_field_name("email")
        assert all(r.source == "field_name" for r in results)
        assert all(r.layer == 1 for r in results)

    def test_no_duplicate_entity_types(self):
        results = classify_field_name("email_address")
        entity_types = [r.entity_type for r in results]
        assert len(entity_types) == len(set(entity_types))


class TestIsFreeText:

    @pytest.mark.parametrize("field_name", [
        "comment", "comments", "notes", "description", "message",
        "body", "text", "feedback", "review", "narrative", "summary",
        "observation", "reason", "remark", "log",
    ])
    def test_known_free_text_names(self, field_name):
        assert is_free_text(field_name, "anything") is True

    @pytest.mark.parametrize("field_name", [
        "email", "ssn", "credit_card", "password", "account_number",
    ])
    def test_structured_field_names_not_free_text(self, field_name):
        short_value = "test"
        assert is_free_text(field_name, short_value) is False

    def test_value_heuristic_detects_prose(self):
        # Generic field name but long prose value → free-text
        assert is_free_text("info", "The patient presented with chest pain and shortness of breath") is True

    def test_short_value_not_free_text(self):
        assert is_free_text("info", "approved") is False
