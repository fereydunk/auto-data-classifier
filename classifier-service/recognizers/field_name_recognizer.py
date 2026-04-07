"""
Layer 1 — Field name / schema metadata classifier.

Looks only at the field name itself (zero data access).
Handles snake_case, camelCase, PascalCase, kebab-case and common abbreviations.

Returns results tagged with source="field_name" and layer=1.
"""

import re
from dataclasses import dataclass
from typing import Optional

from classification.taxonomy import DataTag, tag_entity

# ---------------------------------------------------------------------------
# Free-text field detection
# Field names that signal unstructured content — always escalate to Layer 3.
# ---------------------------------------------------------------------------
FREE_TEXT_FIELD_NAMES = {
    "comment", "comments", "note", "notes", "description", "message",
    "messages", "body", "text", "content", "details", "remark",
    "remarks", "feedback", "review", "narrative", "summary", "observation",
    "reason", "response", "log", "annotation", "memo", "transcript",
    "freetext", "opentext", "narrativetext",
}

FREE_TEXT_VALUE_MIN_WORDS = 6   # values with this many words are treated as free-text


def is_free_text(field_name: str, value: str) -> bool:
    """
    True if the field is likely unstructured free-text.
    Checked by field name first (zero data access), then value heuristic.
    """
    tokens = _tokenize(field_name)
    if any(t in FREE_TEXT_FIELD_NAMES for t in tokens):
        return True
    # Value heuristic — only reached if field name gave no signal
    return len(value.split()) >= FREE_TEXT_VALUE_MIN_WORDS and " " in value


# ---------------------------------------------------------------------------
# Field name → entity type patterns
# Each entry: (entity_type, keywords, confidence)
# Keywords are matched against the normalized (tokenized) field name.
# ---------------------------------------------------------------------------
# fmt: off
_PATTERNS: list[tuple[str, list[str], float]] = [

    # ── GOVERNMENT_ID — very specific identifiers ────────────────────────────
    ("US_SSN",           ["ssn", "socialsecurity", "social_security"],                  0.92),
    ("NIN",              ["nin", "nationalinsurance", "national_insurance"],             0.92),
    ("PASSPORT",         ["passport"],                                                   0.92),
    ("DRIVER_LICENSE",   ["driverlicense", "driver_license", "drivinglicense", "dlnumber", "dl"], 0.88),
    ("US_ITIN",          ["itin", "taxpayerid", "taxpayer_id"],                         0.88),
    ("AU_TFN",           ["tfn", "taxfile", "tax_file"],                                0.88),
    ("SIN",              ["sin", "socialinsurance", "social_insurance"],                0.88),

    # ── PHI — healthcare identifiers ─────────────────────────────────────────
    ("MEDICAL_RECORD",       ["mrn", "medicalrecord", "medical_record", "patientid", "patient_id"],   0.92),
    ("MEDICAL_CONDITION",    ["diagnosis", "condition", "disease", "icd", "icd10", "icdcode"],        0.88),
    ("MEDICATION",           ["medication", "drug", "prescription", "rx", "dosage", "medicine"],      0.88),
    ("NATIONAL_PROVIDER_ID", ["npi", "providerid", "provider_id", "npinumber"],                       0.90),
    ("DEA_NUMBER",           ["dea", "deanumber", "dea_number"],                                      0.92),
    ("HEALTH_INSURANCE",     ["insuranceid", "insurance_id", "healthplan", "health_plan",
                               "memberid", "member_id", "policynumber", "policy_number"],             0.85),

    # ── BIOMETRIC ─────────────────────────────────────────────────────────────
    ("BIOMETRIC", ["fingerprint", "biometric", "faceid", "face_id", "facial",
                   "retina", "iris", "voiceprint", "voice_print"],                                    0.92),

    # ── GENETIC ───────────────────────────────────────────────────────────────
    ("GENETIC", ["dna", "genome", "genotype", "genetic", "snp", "genomic"],                           0.92),

    # ── PCI — payment card network ────────────────────────────────────────────
    ("CREDIT_CARD",   ["creditcard", "credit_card", "cardnumber", "card_number",
                       "pan", "ccnumber", "cc_number", "cardno", "card_no"],                          0.92),
    ("IBAN_CODE",     ["iban"],                                                                        0.95),
    ("SWIFT_CODE",    ["swift", "bic", "sortcode", "sort_code"],                                      0.90),
    ("CRYPTO_WALLET", ["wallet", "bitcoin", "ethereum", "cryptoaddress", "crypto_address"],            0.88),

    # ── FINANCIAL — bank account data ─────────────────────────────────────────
    ("BANK_ACCOUNT",    ["accountnumber", "account_number", "bankaccount", "bank_account",
                         "acctno", "acct_no", "accountno"],                                           0.88),
    ("US_BANK_ROUTING", ["routing", "routingnumber", "routing_number", "aba"],                        0.88),

    # ── CREDENTIALS ───────────────────────────────────────────────────────────
    ("PASSWORD",          ["password", "passwd", "pwd"],                                               0.95),
    ("API_KEY",           ["apikey", "api_key", "apisecret", "api_secret"],                           0.92),
    ("SECRET_KEY",        ["secret", "secretkey", "secret_key"],                                      0.88),
    ("ACCESS_TOKEN",      ["token", "accesstoken", "access_token", "authtoken",
                           "auth_token", "bearer"],                                                    0.88),
    ("JWT_TOKEN",         ["jwt"],                                                                     0.95),
    ("AWS_ACCESS_KEY",    ["awskey", "aws_key", "awsaccesskey", "aws_access_key"],                    0.92),
    ("CONNECTION_STRING", ["connectionstring", "connection_string", "connstring",
                           "conn_string", "databaseurl", "database_url", "dburl"],                    0.92),
    ("PRIVATE_KEY",       ["privatekey", "private_key", "pem"],                                       0.92),

    # ── PII — personal identifiers ────────────────────────────────────────────
    ("EMAIL_ADDRESS", ["email", "emailaddress", "email_address"],                                      0.92),
    ("PHONE_NUMBER",  ["phone", "telephone", "mobile", "cell", "fax",
                       "phonenumber", "phone_number"],                                                 0.88),
    ("DATE_TIME",     ["dob", "dateofbirth", "date_of_birth", "birthdate", "birthday"],               0.88),
    ("PERSON",        ["name", "firstname", "first_name", "lastname", "last_name",
                       "fullname", "full_name", "givenname", "given_name",
                       "surname", "forename", "middlename"],                                          0.82),
    ("USERNAME",      ["username", "user_name", "login", "handle", "screenname"],                     0.85),
    ("EMPLOYEE_ID",   ["employeeid", "employee_id", "empid", "emp_id",
                       "staffid", "staff_id", "workerid", "worker_id"],                               0.88),
    ("CUSTOMER_ID",   ["customerid", "customer_id", "clientid", "client_id",
                       "accountholder", "account_holder"],                                            0.82),
    ("IP_ADDRESS",    ["ip", "ipaddress", "ip_address", "ipaddr"],                                    0.92),
    ("GENDER",        ["gender", "sex"],                                                               0.88),
    ("NATIONALITY",   ["nationality", "citizenship"],                                                  0.88),
    ("LOCATION",      ["address", "street", "city", "state", "country",
                       "zip", "postcode", "postalcode", "postal_code",
                       "latitude", "longitude", "lat", "lng",
                       "coordinates", "geolocation", "location"],                                     0.82),

    # ── NPI — Non-Public Information ──────────────────────────────────────────
    ("INSIDER_INFO",      ["insider", "mnpi", "nonpublic", "non_public"],               0.82),
    ("EARNINGS_DATA",     ["earnings", "forecast", "guidance"],                         0.78),
    ("MERGER_ACQUISITION",["merger", "acquisition", "dealinfo", "deal_info", "mainfo"], 0.82),

    # ── MINOR ─────────────────────────────────────────────────────────────────
    ("MINOR_DATA", ["child", "minor", "juvenile", "underage"],                          0.88),
]
# fmt: on


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
@dataclass
class FieldNameMatch:
    entity_type: str
    tag: str
    score: float
    matched_keyword: str
    layer: int = 1
    source: str = "field_name"


def _word_tokens(name: str) -> list[str]:
    """
    Split a name into lowercase word tokens.
    Handles snake_case, kebab-case, camelCase, PascalCase, spaces.
    """
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1 \2", s)
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s)
    return [t.lower() for t in s.split() if t]


# Keep _tokenize as alias for is_free_text (uses list form)
def _tokenize(field_name: str) -> list[str]:
    return _word_tokens(field_name)


def _keyword_matches(keyword: str, field_tokens: list[str]) -> bool:
    """
    True if the keyword's tokens appear as a consecutive run in field_tokens.

    Examples:
      "credit_card" vs ["credit", "card", "number"]  → True  (prefix match)
      "patient_id"  vs ["id"]                         → False (field too short)
      "ip"          vs ["ip", "address"]              → True  (single-token prefix)
    """
    kw_tokens = _word_tokens(keyword)
    k, n = len(kw_tokens), len(field_tokens)
    for i in range(n - k + 1):
        if field_tokens[i : i + k] == kw_tokens:
            return True
    return False


def classify_field_name(field_name: str) -> list[FieldNameMatch]:
    """
    Return tag matches inferred from the field name alone.
    A field can match multiple tags (e.g. "patient_email" → PII + PHI context).
    Results are sorted by confidence descending.
    """
    field_tokens = _word_tokens(field_name)
    seen_entity_types: set[str] = set()
    results: list[FieldNameMatch] = []

    for entity_type, keywords, confidence in _PATTERNS:
        if entity_type in seen_entity_types:
            continue
        for keyword in keywords:
            if _keyword_matches(keyword, field_tokens):
                data_tag = tag_entity(entity_type)
                results.append(FieldNameMatch(
                    entity_type=entity_type,
                    tag=data_tag.value,
                    score=confidence,
                    matched_keyword=keyword,
                ))
                seen_entity_types.add(entity_type)
                break

    results.sort(key=lambda r: r.score, reverse=True)
    return results
