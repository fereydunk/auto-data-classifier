"""
Tests for PCI (Payment Card Industry) regex recognizers.
Covers payment card network data: IBAN, SWIFT, crypto wallets.
Bank account and routing number tests are in test_financial_recognizers.py.
"""
import pytest
from recognizers.pci_recognizers import (
    IBANRecognizer,
    SwiftCodeRecognizer,
    CryptoWalletRecognizer,
    get_pci_recognizers,
)


class TestIBANRecognizer:
    r = IBANRecognizer()

    @pytest.mark.parametrize("text", [
        "Transfer to GB29NWBK60161331926819",
        "IBAN: DE89370400440532013000",
        "Account FR7630006000011234567890189",
    ])
    def test_detects_valid_ibans(self, text):
        assert self.r.analyze(text=text, entities=["IBAN_CODE"])

    @pytest.mark.parametrize("text", [
        "No IBAN here", "reference number 12345",
    ])
    def test_ignores_non_iban(self, text):
        assert not self.r.analyze(text=text, entities=["IBAN_CODE"])


class TestSwiftCodeRecognizer:
    r = SwiftCodeRecognizer()

    @pytest.mark.parametrize("text", [
        "SWIFT: DEUTDEDB", "BIC code is CHASUS33", "Wire via BOFAUS3N",
    ])
    def test_detects_swift_codes(self, text):
        assert self.r.analyze(text=text, entities=["SWIFT_CODE"])


class TestCryptoWalletRecognizer:
    r = CryptoWalletRecognizer()

    @pytest.mark.parametrize("text", [
        "Send to 0x71C7656EC7ab88b098defB751B7401B5f6d8976F",   # ETH
        "BTC: 1A1zP1eP5QGefi2DMPTfTL5SLmv7Divf Na",             # BTC legacy
    ])
    def test_detects_crypto_wallets(self, text):
        assert self.r.analyze(text=text, entities=["CRYPTO_WALLET"])


class TestGetPCIRecognizers:
    def test_returns_three(self):
        assert len(get_pci_recognizers()) == 3

    def test_each_has_supported_entity(self):
        for r in get_pci_recognizers():
            assert r.supported_entities
