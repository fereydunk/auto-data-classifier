"""
Universal data classification taxonomy — 11 tags.

Domain-agnostic: works for retail, healthcare, finance, HR, logistics, or any industry.
Each detected field is assigned one or more DataTags that describe WHAT kind of data
was found. Sensitivity decisions (what to do with a tag) are left to the operator.

Adding a new industry vertical means:
  1. Registering new entity types in ENTITY_TAG below
  2. Adding corresponding recognizers / GLiNER labels
  No other files need to change.
"""

from enum import Enum


class DataTag(str, Enum):
    PII           = "PII"           # Personally Identifiable Information
    PHI           = "PHI"           # Protected Health Information
    PCI           = "PCI"           # Payment Card / card-network data
    CREDENTIALS   = "CREDENTIALS"   # Passwords, tokens, API keys, secrets
    FINANCIAL     = "FINANCIAL"     # Bank accounts, routing numbers, financial records
    GOVERNMENT_ID = "GOVERNMENT_ID" # Passports, national IDs, driver's licences, SSN
    BIOMETRIC     = "BIOMETRIC"     # Fingerprints, face scans, voice prints
    GENETIC       = "GENETIC"       # DNA sequences, genomic data
    NPI           = "NPI"           # Non-Public Information (insider/pre-release financials)
    LOCATION      = "LOCATION"      # Precise geolocation, IP addresses
    MINOR         = "MINOR"         # Data relating to a person under 13 / 16


# ---------------------------------------------------------------------------
# Entity type → DataTag
# One entity type maps to exactly one tag.
# ---------------------------------------------------------------------------
# fmt: off
ENTITY_TAG: dict[str, DataTag] = {

    # ── PII — universal personal identifiers ────────────────────────────────
    "PERSON":           DataTag.PII,
    "EMAIL_ADDRESS":    DataTag.PII,
    "PHONE_NUMBER":     DataTag.PII,
    "DATE_TIME":        DataTag.PII,   # DOB in context; bare dates filtered by GLiNER
    "USERNAME":         DataTag.PII,
    "EMPLOYEE_ID":      DataTag.PII,
    "CUSTOMER_ID":      DataTag.PII,
    "GENDER":           DataTag.PII,
    "NATIONALITY":      DataTag.PII,
    "RELIGION":         DataTag.PII,
    "RACE_ETHNICITY":   DataTag.PII,

    # ── GOVERNMENT_ID — government-issued identifiers ────────────────────────
    "US_SSN":           DataTag.GOVERNMENT_ID,
    "NIN":              DataTag.GOVERNMENT_ID,   # UK National Insurance Number
    "SIN":              DataTag.GOVERNMENT_ID,   # Canadian Social Insurance Number
    "AU_TFN":           DataTag.GOVERNMENT_ID,   # Australian Tax File Number
    "US_ITIN":          DataTag.GOVERNMENT_ID,   # US Individual Taxpayer ID
    "PASSPORT":         DataTag.GOVERNMENT_ID,
    "DRIVER_LICENSE":   DataTag.GOVERNMENT_ID,

    # ── LOCATION — geolocation and network identifiers ───────────────────────
    "LOCATION":         DataTag.LOCATION,
    "IP_ADDRESS":       DataTag.LOCATION,

    # ── PHI — protected health information ──────────────────────────────────
    "MEDICAL_RECORD":       DataTag.PHI,
    "HEALTH_INSURANCE":     DataTag.PHI,
    "MEDICAL_CONDITION":    DataTag.PHI,
    "MEDICATION":           DataTag.PHI,
    "NATIONAL_PROVIDER_ID": DataTag.PHI,   # US NPI (provider identifier)
    "DEA_NUMBER":           DataTag.PHI,

    # ── BIOMETRIC ────────────────────────────────────────────────────────────
    "BIOMETRIC":            DataTag.BIOMETRIC,

    # ── GENETIC ──────────────────────────────────────────────────────────────
    "GENETIC":              DataTag.GENETIC,

    # ── PCI — payment card network data ──────────────────────────────────────
    "CREDIT_CARD":          DataTag.PCI,
    "IBAN_CODE":            DataTag.PCI,
    "SWIFT_CODE":           DataTag.PCI,
    "CRYPTO_WALLET":        DataTag.PCI,

    # ── FINANCIAL — bank account and financial record data ───────────────────
    "BANK_ACCOUNT":         DataTag.FINANCIAL,
    "US_BANK_ROUTING":      DataTag.FINANCIAL,

    # ── CREDENTIALS — authentication secrets ────────────────────────────────
    "PASSWORD":             DataTag.CREDENTIALS,
    "API_KEY":              DataTag.CREDENTIALS,
    "SECRET_KEY":           DataTag.CREDENTIALS,
    "ACCESS_TOKEN":         DataTag.CREDENTIALS,
    "PRIVATE_KEY":          DataTag.CREDENTIALS,
    "JWT_TOKEN":            DataTag.CREDENTIALS,
    "AWS_ACCESS_KEY":       DataTag.CREDENTIALS,
    "CONNECTION_STRING":    DataTag.CREDENTIALS,

    # ── NPI — Non-Public Information (financial / securities) ────────────────
    "INSIDER_INFO":         DataTag.NPI,
    "EARNINGS_DATA":        DataTag.NPI,
    "MERGER_ACQUISITION":   DataTag.NPI,

    # ── MINOR — data relating to children ────────────────────────────────────
    "MINOR_DATA":           DataTag.MINOR,
}
# fmt: on


def tag_entity(entity_type: str) -> DataTag:
    """Map a Presidio entity type to its DataTag. Unknown types → PII (safe default)."""
    return ENTITY_TAG.get(entity_type, DataTag.PII)
