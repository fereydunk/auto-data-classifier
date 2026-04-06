"""
Regex-based recognizers for CREDENTIALS — API keys, tokens, connection strings.
Relevant for any industry that logs or stores technical/infrastructure data
(SaaS platforms, DevOps pipelines, audit logs, config management, etc.).
"""

from presidio_analyzer import Pattern, PatternRecognizer


class AWSAccessKeyRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            name="AWS_ACCESS_KEY",
            regex=r"\bAKIA[0-9A-Z]{16}\b",
            score=0.99,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="AWS_ACCESS_KEY", patterns=self.PATTERNS)


class JWTRecognizer(PatternRecognizer):
    """JSON Web Token — three base64url segments separated by dots."""
    PATTERNS = [
        Pattern(
            name="JWT",
            regex=r"\beyJ[A-Za-z0-9\-_]+\.eyJ[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\b",
            score=0.97,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="JWT_TOKEN", patterns=self.PATTERNS)


class ConnectionStringRecognizer(PatternRecognizer):
    """Database / service connection strings containing embedded credentials."""
    PATTERNS = [
        Pattern(
            name="DB_CONNECTION_STRING",
            regex=(
                r"\b(mongodb(\+srv)?|postgresql|mysql|redis|amqp|kafka)"
                r"://[A-Za-z0-9._~!$&'()*+,;=%-]+:[A-Za-z0-9._~!$&'()*+,;=%-]+"
                r"@[^\s\"'<>]+"
            ),
            score=0.95,
        )
    ]

    def __init__(self):
        super().__init__(supported_entity="CONNECTION_STRING", patterns=self.PATTERNS)


class GenericAPIKeyRecognizer(PatternRecognizer):
    """
    Common API key formats (hex or base62, 32–64 chars).
    Low confidence alone — intended to complement GLiNER context detection.
    """
    PATTERNS = [
        Pattern(
            name="API_KEY_HEX",
            regex=r"\b[a-f0-9]{32,64}\b",
            score=0.35,
        ),
        Pattern(
            name="API_KEY_ALPHANUM",
            regex=r"\b[A-Za-z0-9]{40,64}\b",
            score=0.30,
        ),
    ]

    def __init__(self):
        super().__init__(supported_entity="API_KEY", patterns=self.PATTERNS)


def get_credentials_recognizers():
    return [
        AWSAccessKeyRecognizer(),
        JWTRecognizer(),
        ConnectionStringRecognizer(),
        GenericAPIKeyRecognizer(),
    ]
