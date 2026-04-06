"""
Tests for PHI (Protected Health Information) regex recognizers.
"""
import pytest
from recognizers.phi_recognizers import (
    NationalProviderIDRecognizer,
    DEANumberRecognizer,
    HealthInsuranceRecognizer,
    get_phi_recognizers,
)


class TestNationalProviderIDRecognizer:
    r = NationalProviderIDRecognizer()

    @pytest.mark.parametrize("text", [
        "Provider NPI: 1234567890",
        "NPI 2000000001",
    ])
    def test_detects_npi(self, text):
        assert self.r.analyze(text=text, entities=["NATIONAL_PROVIDER_ID"])

    def test_ignores_short_numbers(self):
        assert not self.r.analyze(text="code 12345", entities=["NATIONAL_PROVIDER_ID"])


class TestDEANumberRecognizer:
    r = DEANumberRecognizer()

    @pytest.mark.parametrize("text", [
        "DEA: AB1234563",
        "registration XY9876543",
    ])
    def test_detects_dea(self, text):
        assert self.r.analyze(text=text, entities=["DEA_NUMBER"])

    def test_ignores_non_dea(self):
        assert not self.r.analyze(text="reference 123", entities=["DEA_NUMBER"])


class TestGetPHIRecognizers:
    def test_returns_three(self):
        assert len(get_phi_recognizers()) == 3

    def test_all_have_supported_entities(self):
        for r in get_phi_recognizers():
            assert r.supported_entities
