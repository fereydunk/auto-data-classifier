"""
Tests for CREDENTIALS regex recognizers.
"""
import pytest
from recognizers.credentials_recognizers import (
    AWSAccessKeyRecognizer,
    JWTRecognizer,
    ConnectionStringRecognizer,
    get_credentials_recognizers,
)


class TestAWSAccessKeyRecognizer:
    r = AWSAccessKeyRecognizer()

    @pytest.mark.parametrize("text", [
        "key=AKIAIOSFODNN7EXAMPLE",
        "AWS_ACCESS_KEY_ID=AKIAI44QH8DHBEXAMPLE",
    ])
    def test_detects_aws_keys(self, text):
        assert self.r.analyze(text=text, entities=["AWS_ACCESS_KEY"])

    def test_ignores_non_aws(self):
        assert not self.r.analyze(text="some random string", entities=["AWS_ACCESS_KEY"])


class TestJWTRecognizer:
    r = JWTRecognizer()

    VALID_JWT = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    )

    def test_detects_jwt(self):
        assert self.r.analyze(text=f"Authorization: Bearer {self.VALID_JWT}", entities=["JWT_TOKEN"])

    def test_ignores_plain_text(self):
        assert not self.r.analyze(text="hello world", entities=["JWT_TOKEN"])


class TestConnectionStringRecognizer:
    r = ConnectionStringRecognizer()

    @pytest.mark.parametrize("text", [
        "mongodb://user:password@host:27017/db",
        "postgresql://admin:secret@localhost:5432/mydb",
        "redis://user:pass@redis-host:6379",
        "kafka://user:pass@broker:9092",
    ])
    def test_detects_connection_strings(self, text):
        assert self.r.analyze(text=text, entities=["CONNECTION_STRING"])

    def test_ignores_url_without_credentials(self):
        # URL without password should not match
        assert not self.r.analyze(text="https://example.com/path", entities=["CONNECTION_STRING"])


class TestGetCredentialsRecognizers:
    def test_returns_four(self):
        assert len(get_credentials_recognizers()) == 4

    def test_all_have_supported_entities(self):
        for r in get_credentials_recognizers():
            assert r.supported_entities
