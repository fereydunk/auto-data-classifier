"""
Regex-based recognizers for FINANCIAL data.
Covers bank account numbers and routing numbers — financial account data
that is distinct from payment card network data (PCI).
"""

from presidio_analyzer import Pattern, PatternRecognizer


class BankAccountRecognizer(PatternRecognizer):
    """Detects bank account numbers via regex + a context-word boost.

    The previous unconditional `\\b[0-9]{8,17}\\b` regex matched any longish
    digit string — phone numbers, order IDs, timestamps, tracking numbers —
    flooding the review queue with score 0.4 noise. Now requires either:
      (a) a bank-context word adjacent to the digits (handled by Presidio's
          context_words boost — the regex score stays low so a bare digit
          string only fires when context is present), OR
      (b) the field name match in Layer 1 (handled by field_name_recognizer).
    """

    PATTERNS = [
        Pattern(
            name="BANK_ACCOUNT",
            regex=r"\b[0-9]{8,17}\b",
            score=0.15,    # below recommendation publish threshold without context
        )
    ]

    CONTEXT = [
        "account", "acct", "bank", "iban", "routing", "deposit", "checking", "savings",
    ]

    def __init__(self):
        super().__init__(
            supported_entity="BANK_ACCOUNT",
            patterns=self.PATTERNS,
            context=self.CONTEXT,
        )


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
