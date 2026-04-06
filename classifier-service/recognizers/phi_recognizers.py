"""
Regex-based recognizers for PHI (Protected Health Information).
Relevant for healthcare providers, insurers, pharma, and any industry
that processes patient or health-related data.
"""

from presidio_analyzer import Pattern, PatternRecognizer


class NationalProviderIDRecognizer(PatternRecognizer):
    """US National Provider Identifier — 10-digit number starting with 1 or 2."""
    PATTERNS = [
        Pattern(
            name="NATIONAL_PROVIDER_ID",
            regex=r"\b[12][0-9]{9}\b",
            score=0.55,  # 10-digit numbers are common; GLiNER context raises this
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="NATIONAL_PROVIDER_ID", patterns=self.PATTERNS)


class DEANumberRecognizer(PatternRecognizer):
    """DEA registration number — 2 letters + 7 digits with checksum structure."""
    PATTERNS = [
        Pattern(
            name="DEA_NUMBER",
            regex=r"\b[A-Z]{2}[0-9]{7}\b",
            score=0.75,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="DEA_NUMBER", patterns=self.PATTERNS)


class HealthInsuranceRecognizer(PatternRecognizer):
    """
    Generic health insurance / member ID patterns.
    Covers common US formats (Medicare, Medicaid, commercial plans).
    """
    PATTERNS = [
        Pattern(
            name="MEDICARE_ID",
            regex=r"\b[1-9][A-Z0-9]{10}\b",  # MBI format
            score=0.65,
        ),
        Pattern(
            name="HEALTH_PLAN_ID",
            regex=r"\b[A-Z]{2,4}[0-9]{6,12}\b",
            score=0.50,
        ),
    ]

    def __init__(self):
        super().__init__(supported_entity="HEALTH_INSURANCE", patterns=self.PATTERNS)


def get_phi_recognizers():
    return [
        NationalProviderIDRecognizer(),
        DEANumberRecognizer(),
        HealthInsuranceRecognizer(),
    ]
