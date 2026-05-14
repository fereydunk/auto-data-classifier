"""Vocabulary of demo schema fields — the wizard composes a fresh schema
from a random subset of these at runtime.

Each entry is a FieldSpec with:
    name      — Avro field name
    avro_type — full Avro type (always nullable: ["null", "string"|"int"|...])
    category  — tag family the field belongs to (PII, GOVERNMENT_ID, …)
    sample    — callable(rng) → realistic-but-fake value

Adding a new field: append to FIELD_POOL — no other code changes needed.
The wizard's schema_builder picks N fields (balanced across categories) and
the producer iterates the chosen specs' `sample` callables to fill rows.
"""
from __future__ import annotations

import random
import string
from dataclasses import dataclass
from typing import Callable, List


@dataclass(frozen=True)
class FieldSpec:
    name:      str
    avro_type: object
    category:  str
    sample:    Callable[[random.Random], object]


_NULLABLE_STR = ["null", "string"]
_NULLABLE_INT = ["null", "int"]


# ── Sample-data pools ────────────────────────────────────────────────────────
_FIRST = ["Alice", "Bob", "Carlos", "Diana", "Eve", "Frank",
          "Grace", "Henry", "Ingrid", "Jose", "Kira", "Liam"]
_LAST  = ["Smith", "Jones", "Patel", "Kim", "Mueller", "Tanaka",
          "Silva", "Nguyen", "Okafor", "Chen"]
_DOMAIN = ["example.com", "acme.org", "test.net", "corp.io", "bigco.co"]
_CITY   = ["Austin", "Boston", "Chicago", "Denver", "Seattle",
           "Portland", "Phoenix", "Atlanta"]
_STREET = ["Main St", "Oak Ave", "Maple Dr", "Park Blvd", "Cedar Ln"]
_NOTES = [
    "Customer reports being charged twice for last month's invoice.",
    "Patient confirmed allergy to penicillin during intake.",
    "Wire transfer of $50,000 flagged for additional review by compliance.",
    "Reset password requested via support ticket #4821.",
    "Account holder lives at the registered billing address per ID verification.",
    "VIP customer — escalate any service issue to the account executive.",
    "Tax ID provided for invoice was rejected by the verification service.",
    "Father (legal guardian) reachable at the alternate phone on file.",
]


def _name(rng):  return rng.choice(_FIRST)
def _surname(rng): return rng.choice(_LAST)
def _email(rng): return f"{rng.choice(_FIRST).lower()}.{rng.choice(_LAST).lower()}@{rng.choice(_DOMAIN)}"
def _phone(rng): return f"+1-{rng.randint(200, 999)}-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
def _dob(rng):   return f"{rng.randint(1950, 2005):04d}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
def _ip(rng):    return f"{rng.randint(1,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}"
def _ssn(rng):   return f"{rng.randint(100, 899):03d}-{rng.randint(10, 99):02d}-{rng.randint(1000, 9999):04d}"
def _passport(rng): return f"{rng.choice('ABCDEFGHIJ')}{rng.randint(10_000_000, 99_999_999)}"
def _drivers(rng):  return f"D{rng.randint(1_000_000, 9_999_999)}"
def _tax_id(rng):   return f"{rng.randint(10, 99):02d}-{rng.randint(1_000_000, 9_999_999):07d}"
def _cc(rng):       return f"4{rng.randint(100_000_000_000_000, 999_999_999_999_999):015d}"
def _iban(rng):     return f"GB{rng.randint(10, 99):02d}NWBK{rng.randint(10_000_000, 99_999_999):08d}{rng.randint(1000, 9999):04d}"
def _swift(rng):    return f"{''.join(rng.choices(string.ascii_uppercase, k=6))}{rng.choice('US')}{rng.randint(10, 99)}"
def _routing(rng):  return f"{rng.randint(100_000_000, 999_999_999):09d}"
def _bank_acct(rng):return f"{rng.randint(1_000_000_000, 9_999_999_999):010d}"
def _username(rng): return f"{rng.choice(_FIRST).lower()}{rng.randint(10, 99)}"
def _password(rng): return "Hunter" + str(rng.randint(2, 99)) + "!"
def _api_key(rng):  return f"sk-{''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=40))}"
def _aws_key(rng):  return f"AKIA{''.join(rng.choices(string.ascii_uppercase + string.digits, k=16))}"
def _jwt(rng):
    pad = "".join(rng.choices(string.ascii_letters + string.digits, k=20))
    return f"eyJ{pad}.eyJ{pad}.{pad}"
def _street(rng):   return f"{rng.randint(1, 9999)} {rng.choice(_STREET)}"
def _city(rng):     return rng.choice(_CITY)
def _zip(rng):      return f"{rng.randint(10_000, 99_999)}"
def _gps(rng):      return f"{rng.uniform(-90, 90):.6f},{rng.uniform(-180, 180):.6f}"
def _state(rng):    return rng.choice(["CA", "TX", "NY", "FL", "WA", "OR", "MA", "GA"])
def _country(rng):  return rng.choice(["US", "GB", "DE", "FR", "JP", "BR", "IN"])
def _notes(rng):    return rng.choice(_NOTES)
def _comment(rng):  return rng.choice(_NOTES)
def _id(rng):       return f"CUST-{rng.randint(100_000, 999_999)}"
def _order_id(rng): return rng.randint(1, 1_000_000)
def _amount(rng):   return rng.randint(1, 100_000)
def _diagnosis(rng):return rng.choice(["Type 2 Diabetes", "Hypertension", "Asthma", "Migraine"])
def _med(rng):      return rng.choice(["Metformin 500mg", "Lisinopril 10mg", "Albuterol", "Sumatriptan"])
def _npi(rng):      return f"{rng.randint(1_000_000_000, 1_999_999_999):010d}"
def _dea(rng):      return f"{rng.choice('ABCDEFGHIJKLM')}{rng.choice('ABCDEFGHIJKLM')}{rng.randint(1_000_000, 9_999_999)}"
def _mrn(rng):      return f"MRN-{rng.randint(10_000_000, 99_999_999)}"
def _genome(rng):   return "".join(rng.choices("ACGT", k=40))
def _crypto(rng):   return f"{''.join(rng.choices(string.ascii_letters + string.digits, k=34))}"
def _fingerprint(rng): return f"{''.join(rng.choices('0123456789abcdef', k=64))}"


# Each entry has a unique field name. Adding rows here is the only place to
# extend the demo vocabulary.
FIELD_POOL: List[FieldSpec] = [
    # ── PII (12) ──────────────────────────────────────────────────────
    FieldSpec("first_name",       _NULLABLE_STR, "PII", _name),
    FieldSpec("last_name",        _NULLABLE_STR, "PII", _surname),
    FieldSpec("email",            _NULLABLE_STR, "PII", _email),
    FieldSpec("phone_number",     _NULLABLE_STR, "PII", _phone),
    FieldSpec("date_of_birth",    _NULLABLE_STR, "PII", _dob),
    FieldSpec("ip_address",       _NULLABLE_STR, "PII", _ip),
    FieldSpec("customer_id",      _NULLABLE_STR, "PII", _id),
    FieldSpec("middle_name",      _NULLABLE_STR, "PII", _name),
    FieldSpec("nickname",         _NULLABLE_STR, "PII", _name),
    FieldSpec("guardian_name",    _NULLABLE_STR, "PII", _name),
    FieldSpec("emergency_contact",_NULLABLE_STR, "PII", _phone),
    FieldSpec("personal_email",   _NULLABLE_STR, "PII", _email),

    # ── GOVERNMENT_ID (5) ─────────────────────────────────────────────
    FieldSpec("ssn",              _NULLABLE_STR, "GOVERNMENT_ID", _ssn),
    FieldSpec("passport_number",  _NULLABLE_STR, "GOVERNMENT_ID", _passport),
    FieldSpec("driver_license",   _NULLABLE_STR, "GOVERNMENT_ID", _drivers),
    FieldSpec("tax_id",           _NULLABLE_STR, "GOVERNMENT_ID", _tax_id),
    FieldSpec("national_id",      _NULLABLE_STR, "GOVERNMENT_ID", _ssn),

    # ── PCI (4) ───────────────────────────────────────────────────────
    FieldSpec("credit_card_number", _NULLABLE_STR, "PCI", _cc),
    FieldSpec("iban",               _NULLABLE_STR, "PCI", _iban),
    FieldSpec("swift_code",         _NULLABLE_STR, "PCI", _swift),
    FieldSpec("crypto_wallet",      _NULLABLE_STR, "PCI", _crypto),

    # ── FINANCIAL (3) ─────────────────────────────────────────────────
    FieldSpec("bank_account_number", _NULLABLE_STR, "FINANCIAL", _bank_acct),
    FieldSpec("routing_number",      _NULLABLE_STR, "FINANCIAL", _routing),
    FieldSpec("transaction_amount",  _NULLABLE_INT, "FINANCIAL", _amount),

    # ── CREDENTIALS (5) ───────────────────────────────────────────────
    FieldSpec("username",            _NULLABLE_STR, "CREDENTIALS", _username),
    FieldSpec("password",            _NULLABLE_STR, "CREDENTIALS", _password),
    FieldSpec("api_key",             _NULLABLE_STR, "CREDENTIALS", _api_key),
    FieldSpec("aws_access_key_id",   _NULLABLE_STR, "CREDENTIALS", _aws_key),
    FieldSpec("session_token",       _NULLABLE_STR, "CREDENTIALS", _jwt),

    # ── LOCATION (6) ──────────────────────────────────────────────────
    FieldSpec("billing_street",      _NULLABLE_STR, "LOCATION", _street),
    FieldSpec("billing_city",        _NULLABLE_STR, "LOCATION", _city),
    FieldSpec("billing_postal_code", _NULLABLE_STR, "LOCATION", _zip),
    FieldSpec("billing_state",       _NULLABLE_STR, "LOCATION", _state),
    FieldSpec("billing_country",     _NULLABLE_STR, "LOCATION", _country),
    FieldSpec("gps_coordinates",     _NULLABLE_STR, "LOCATION", _gps),

    # ── PHI (5) ───────────────────────────────────────────────────────
    FieldSpec("patient_diagnosis",   _NULLABLE_STR, "PHI", _diagnosis),
    FieldSpec("medication",          _NULLABLE_STR, "PHI", _med),
    FieldSpec("medical_record_number",_NULLABLE_STR,"PHI", _mrn),
    FieldSpec("provider_npi",        _NULLABLE_STR, "PHI", _npi),
    FieldSpec("dea_number",          _NULLABLE_STR, "PHI", _dea),

    # ── BIOMETRIC (1), GENETIC (1) ────────────────────────────────────
    FieldSpec("fingerprint_hash",    _NULLABLE_STR, "BIOMETRIC", _fingerprint),
    FieldSpec("dna_sequence",        _NULLABLE_STR, "GENETIC",   _genome),

    # ── FREE TEXT (3) — trips the AI layer ────────────────────────────
    FieldSpec("notes",               _NULLABLE_STR, "FREE_TEXT", _notes),
    FieldSpec("comment",             _NULLABLE_STR, "FREE_TEXT", _comment),
    FieldSpec("description",         _NULLABLE_STR, "FREE_TEXT", _notes),

    # ── Order/transaction context (no tag) ────────────────────────────
    FieldSpec("order_id",            _NULLABLE_INT, "NEUTRAL",   _order_id),
]

POOL_SIZE = len(FIELD_POOL)
CATEGORIES = sorted({f.category for f in FIELD_POOL})


def by_category() -> dict[str, list[FieldSpec]]:
    """Return FIELD_POOL grouped by category."""
    out: dict[str, list[FieldSpec]] = {c: [] for c in CATEGORIES}
    for f in FIELD_POOL:
        out[f.category].append(f)
    return out
