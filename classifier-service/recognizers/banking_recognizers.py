"""
Custom regex-based recognizers for banking-specific PII.
These run on the fast path — no model inference required.
"""

from presidio_analyzer import Pattern, PatternRecognizer


class IBANRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="IBAN",
            regex=r"\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b",
            score=0.95,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="IBAN_CODE", patterns=self.PATTERNS)


class SwiftCodeRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="SWIFT_BIC",
            regex=r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?\b",
            score=0.85,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="SWIFT_CODE", patterns=self.PATTERNS)


class USRoutingNumberRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="US_ROUTING",
            regex=r"\b0[0-9]{8}\b|\b[1-9][0-9]{8}\b",
            score=0.6,  # low confidence — 9 digits alone is ambiguous
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="US_BANK_ROUTING", patterns=self.PATTERNS)


class BankAccountRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="BANK_ACCOUNT",
            regex=r"\b[0-9]{8,17}\b",
            score=0.4,  # very low alone — context from GLiNER raises confidence
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="BANK_ACCOUNT", patterns=self.PATTERNS)


def get_banking_recognizers():
    return [
        IBANRecognizer(),
        SwiftCodeRecognizer(),
        USRoutingNumberRecognizer(),
        BankAccountRecognizer(),
    ]
