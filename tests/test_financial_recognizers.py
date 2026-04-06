"""
Tests for FINANCIAL regex recognizers — bank accounts and routing numbers.
"""
import pytest
from recognizers.financial_recognizers import (
    BankAccountRecognizer,
    USRoutingNumberRecognizer,
    get_financial_recognizers,
)


class TestBankAccountRecognizer:
    r = BankAccountRecognizer()

    @pytest.mark.parametrize("text", [
        "Account: 12345678",
        "account number 123456789012345",
    ])
    def test_detects_bank_accounts(self, text):
        assert self.r.analyze(text=text, entities=["BANK_ACCOUNT"])

    def test_ignores_short_numbers(self):
        assert not self.r.analyze(text="code 1234567", entities=["BANK_ACCOUNT"])


class TestUSRoutingNumberRecognizer:
    r = USRoutingNumberRecognizer()

    @pytest.mark.parametrize("text", [
        "Routing number: 021000021",
        "ABA 111000038",
    ])
    def test_detects_routing_numbers(self, text):
        assert self.r.analyze(text=text, entities=["US_BANK_ROUTING"])


class TestGetFinancialRecognizers:
    def test_returns_two(self):
        assert len(get_financial_recognizers()) == 2

    def test_each_has_supported_entity(self):
        for r in get_financial_recognizers():
            assert r.supported_entities
