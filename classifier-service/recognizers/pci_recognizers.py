"""
Regex-based recognizers for PCI (Payment Card Industry) data.
Covers payment card network data: card numbers, IBANs, SWIFT codes, crypto wallets.

Bank account numbers and routing numbers live in financial_recognizers.py (FINANCIAL tag).
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
    # Tightened: require all-uppercase and anchor to known bank country codes
    # to avoid false positives on common English words.
    PATTERNS = [
        Pattern(
            name="SWIFT_BIC",
            regex=(
                r"\b[A-Z]{4}"                              # bank code (4 uppercase)
                r"(?:AD|AE|AF|AG|AL|AM|AO|AR|AT|AU|AZ|BA|BB|BD|BE|BF|BG|BH|BI|BJ|BN|BO|BR|BS|BT|BW|BY|BZ"
                r"|CA|CD|CF|CG|CH|CI|CL|CM|CN|CO|CR|CU|CV|CY|CZ|DE|DJ|DK|DM|DO|DZ|EC|EE|EG|ER|ES|ET|FI|FJ"
                r"|FM|FR|GA|GB|GD|GE|GH|GM|GN|GQ|GR|GT|GW|GY|HN|HR|HT|HU|ID|IE|IL|IN|IQ|IR|IS|IT|JM|JO|JP"
                r"|KE|KG|KH|KI|KM|KN|KP|KR|KW|KZ|LA|LB|LC|LI|LK|LR|LS|LT|LU|LV|LY|MA|MC|MD|ME|MG|MH|MK"
                r"|ML|MM|MN|MR|MT|MU|MV|MW|MX|MY|MZ|NA|NE|NG|NI|NL|NO|NP|NR|NZ|OM|PA|PE|PG|PH|PK|PL|PT"
                r"|PW|PY|QA|RO|RS|RU|RW|SA|SB|SC|SD|SE|SG|SI|SK|SL|SM|SN|SO|SR|SS|ST|SV|SY|SZ|TC|TD|TG|TH"
                r"|TJ|TL|TM|TN|TO|TR|TT|TV|TZ|UA|UG|US|UY|UZ|VA|VC|VE|VN|VU|WS|XK|YE|ZA|ZM|ZW)"
                r"[A-Z0-9]{2}"                             # location code (2)
                r"(?:[A-Z0-9]{3})?\b"                      # branch code (3, optional)
            ),
            score=0.90,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="SWIFT_CODE", patterns=self.PATTERNS)


class CryptoWalletRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="BTC_ADDRESS",
            regex=r"\b(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,62}\b",
            score=0.85,
        ),
        Pattern(
            name="ETH_ADDRESS",
            regex=r"\b0x[a-fA-F0-9]{40}\b",
            score=0.90,
        ),
    ]

    def __init__(self):
        super().__init__(supported_entity="CRYPTO_WALLET", patterns=self.PATTERNS)


def get_pci_recognizers():
    return [
        IBANRecognizer(),
        SwiftCodeRecognizer(),
        CryptoWalletRecognizer(),
    ]
