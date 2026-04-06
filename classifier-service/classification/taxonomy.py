"""
Universal data classification taxonomy.

Completely domain-agnostic — the same pipeline works for retail,
healthcare, finance, HR, logistics, or any other industry.

Layer 1 — DataCategory: WHAT kind of data was found.
Layer 2 — SensitivityLevel: HOW sensitive is it (derived from category).

Adding a new industry vertical means:
  1. Registering new entity types in ENTITY_CATEGORY
  2. Adding corresponding recognizers / GLiNER labels
  No changes needed to sensitivity logic or catalog tagging.
"""

from enum import Enum


class DataCategory(str, Enum):
    PII         = "PII"          # Personally Identifiable Information
    PHI         = "PHI"          # Protected Health Information
    PCI         = "PCI"          # Payment Card / Financial account data
    CREDENTIALS = "CREDENTIALS"  # Passwords, API keys, tokens, secrets
    CONFIDENTIAL = "CONFIDENTIAL" # Business-confidential (contracts, IP, strategy)
    INTERNAL    = "INTERNAL"     # Internal operational data (order IDs, loyalty cards)


class SensitivityLevel(str, Enum):
    CRITICAL = "CRITICAL"  # PHI or CREDENTIALS present
    HIGH     = "HIGH"      # PII or PCI present
    MEDIUM   = "MEDIUM"    # CONFIDENTIAL data present
    LOW      = "LOW"       # INTERNAL data only
    CLEAN    = "CLEAN"     # No sensitive data detected


# ---------------------------------------------------------------------------
# Entity type → DataCategory
# ---------------------------------------------------------------------------
# fmt: off
ENTITY_CATEGORY: dict[str, DataCategory] = {

    # ── PII (universal across all industries) ───────────────────────────────
    "PERSON":           DataCategory.PII,
    "EMAIL_ADDRESS":    DataCategory.PII,
    "PHONE_NUMBER":     DataCategory.PII,
    "LOCATION":         DataCategory.PII,
    "DATE_TIME":        DataCategory.PII,   # DOB is PII; generic dates are noise-filtered by GLiNER
    "US_SSN":           DataCategory.PII,
    "NIN":              DataCategory.PII,   # UK National Insurance Number
    "SIN":              DataCategory.PII,   # Canadian Social Insurance Number
    "AU_TFN":           DataCategory.PII,   # Australian Tax File Number
    "US_ITIN":          DataCategory.PII,
    "PASSPORT":         DataCategory.PII,
    "DRIVER_LICENSE":   DataCategory.PII,
    "IP_ADDRESS":       DataCategory.PII,
    "GENDER":           DataCategory.PII,
    "NATIONALITY":      DataCategory.PII,
    "RELIGION":         DataCategory.PII,
    "RACE_ETHNICITY":   DataCategory.PII,
    "USERNAME":         DataCategory.PII,
    "EMPLOYEE_ID":      DataCategory.PII,
    "CUSTOMER_ID":      DataCategory.PII,

    # ── PCI (financial account data — not just banking) ──────────────────────
    "CREDIT_CARD":      DataCategory.PCI,
    "BANK_ACCOUNT":     DataCategory.PCI,
    "IBAN_CODE":        DataCategory.PCI,
    "SWIFT_CODE":       DataCategory.PCI,
    "US_BANK_ROUTING":  DataCategory.PCI,
    "CRYPTO_WALLET":    DataCategory.PCI,

    # ── PHI (healthcare) ────────────────────────────────────────────────────
    "MEDICAL_RECORD":   DataCategory.PHI,
    "HEALTH_INSURANCE": DataCategory.PHI,
    "MEDICAL_CONDITION":DataCategory.PHI,
    "MEDICATION":       DataCategory.PHI,
    "NPI":              DataCategory.PHI,   # National Provider Identifier (US)
    "DEA_NUMBER":       DataCategory.PHI,   # Drug Enforcement Administration #
    "BIOMETRIC":        DataCategory.PHI,

    # ── CREDENTIALS ─────────────────────────────────────────────────────────
    "PASSWORD":         DataCategory.CREDENTIALS,
    "API_KEY":          DataCategory.CREDENTIALS,
    "SECRET_KEY":       DataCategory.CREDENTIALS,
    "ACCESS_TOKEN":     DataCategory.CREDENTIALS,
    "PRIVATE_KEY":      DataCategory.CREDENTIALS,
    "JWT_TOKEN":        DataCategory.CREDENTIALS,
    "AWS_ACCESS_KEY":   DataCategory.CREDENTIALS,
    "CONNECTION_STRING":DataCategory.CREDENTIALS,

    # ── CONFIDENTIAL (business-sensitive) ────────────────────────────────────
    "ORGANIZATION":     DataCategory.CONFIDENTIAL,
    "CONTRACT_NUMBER":  DataCategory.CONFIDENTIAL,
    "TRADE_SECRET":     DataCategory.CONFIDENTIAL,

    # ── INTERNAL (operational, low-risk) ────────────────────────────────────
    "ORDER_NUMBER":     DataCategory.INTERNAL,
    "LOYALTY_CARD":     DataCategory.INTERNAL,
    "LICENSE_PLATE":    DataCategory.INTERNAL,
    "VEHICLE_ID":       DataCategory.INTERNAL,
    "PRODUCT_ID":       DataCategory.INTERNAL,
    "SHIPMENT_ID":      DataCategory.INTERNAL,
}
# fmt: on


# ---------------------------------------------------------------------------
# DataCategory → SensitivityLevel (highest wins when multiple categories present)
# ---------------------------------------------------------------------------
CATEGORY_SENSITIVITY: dict[DataCategory, SensitivityLevel] = {
    DataCategory.PHI:          SensitivityLevel.CRITICAL,
    DataCategory.CREDENTIALS:  SensitivityLevel.CRITICAL,
    DataCategory.PII:          SensitivityLevel.HIGH,
    DataCategory.PCI:          SensitivityLevel.HIGH,
    DataCategory.CONFIDENTIAL: SensitivityLevel.MEDIUM,
    DataCategory.INTERNAL:     SensitivityLevel.LOW,
}

_LEVEL_ORDER = [
    SensitivityLevel.CRITICAL,
    SensitivityLevel.HIGH,
    SensitivityLevel.MEDIUM,
    SensitivityLevel.LOW,
    SensitivityLevel.CLEAN,
]


def categorize_entity(entity_type: str) -> DataCategory:
    """Map a Presidio entity type to its DataCategory. Unknown types → INTERNAL."""
    return ENTITY_CATEGORY.get(entity_type, DataCategory.INTERNAL)


def sensitivity_for_categories(categories: set[DataCategory]) -> SensitivityLevel:
    """Return the highest SensitivityLevel across a set of DataCategories."""
    if not categories:
        return SensitivityLevel.CLEAN
    levels = {CATEGORY_SENSITIVITY.get(c, SensitivityLevel.LOW) for c in categories}
    for level in _LEVEL_ORDER:
        if level in levels:
            return level
    return SensitivityLevel.CLEAN
