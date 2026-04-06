"""
Regex-based recognizers for FINANCIAL data.
Covers bank account numbers and routing numbers — financial account data
that is distinct from payment card network data (PCI).
"""

from presidio_analyzer import Pattern, PatternRecognizer


class BankAccountRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="BANK_ACCOUNT",
            regex=r"\b[0-9]{8,17}\b",
            score=0.4,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="BANK_ACCOUNT", patterns=self.PATTERNS)


class USRoutingNumberRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="US_ROUTING",
            regex=r"\b0[0-9]{8}\b|\b[1-9][0-9]{8}\b",
            score=0.6,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="US_BANK_ROUTING", patterns=self.PATTERNS)


def get_financial_recognizers():
    return [
        BankAccountRecognizer(),
        USRoutingNumberRecognizer(),
    ]
