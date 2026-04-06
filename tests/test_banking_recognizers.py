"""
Tests for regex-based banking PII recognizers.
No ML model required — pure pattern matching.
"""
import pytest
from recognizers.banking_recognizers import (
    IBANRecognizer,
    SwiftCodeRecognizer,
    USRoutingNumberRecognizer,
    BankAccountRecognizer,
    get_banking_recognizers,
)


def _analyze(recognizer, text):
    """Helper: run a single recognizer against text, return entity types found."""
    results = recognizer.analyze(text=text, entities=[recognizer.supported_entities[0]])
    return results


class TestIBANRecognizer:
    recognizer = IBANRecognizer()

    @pytest.mark.parametrize("text", [
        "Transfer to GB29NWBK60161331926819",
        "IBAN: DE89370400440532013000",
        "Account FR7630006000011234567890189",
    ])
    def test_detects_valid_ibans(self, text):
        results = _analyze(self.recognizer, text)
        assert results, f"Expected IBAN detection in: {text!r}"
        assert results[0].entity_type == "IBAN_CODE"
        assert results[0].score >= 0.9

    @pytest.mark.parametrize("text", [
        "No IBAN here",
        "Call us at 123-456-7890",
        "reference number 12345",
    ])
    def test_ignores_non_iban(self, text):
        results = _analyze(self.recognizer, text)
        assert not results, f"Unexpected IBAN match in: {text!r}"


class TestSwiftCodeRecognizer:
    recognizer = SwiftCodeRecognizer()

    @pytest.mark.parametrize("text", [
        "SWIFT: DEUTDEDB",
        "BIC code is CHASUS33",
        "Wire via BOFAUS3N",
    ])
    def test_detects_swift_codes(self, text):
        results = _analyze(self.recognizer, text)
        assert results, f"Expected SWIFT detection in: {text!r}"
        assert results[0].entity_type == "SWIFT_CODE"

    def test_ignores_short_codes(self):
        results = _analyze(self.recognizer, "code AB12")
        assert not results


class TestUSRoutingNumberRecognizer:
    recognizer = USRoutingNumberRecognizer()

    @pytest.mark.parametrize("text", [
        "Routing number: 021000021",
        "ABA 111000038",
    ])
    def test_detects_routing_numbers(self, text):
        results = _analyze(self.recognizer, text)
        assert results, f"Expected routing number in: {text!r}"
        assert results[0].entity_type == "US_BANK_ROUTING"


class TestGetBankingRecognizers:
    def test_returns_all_four(self):
        recognizers = get_banking_recognizers()
        assert len(recognizers) == 4

    def test_each_has_supported_entity(self):
        for r in get_banking_recognizers():
            assert r.supported_entities, f"{r} has no supported_entities"
