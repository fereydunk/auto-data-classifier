# Classification Taxonomy

## Design principles

- **11 flat tags** — no nested hierarchy, no sensitivity levels baked in
- **Sensitivity is your policy** — organisations define HIGH/MEDIUM/LOW differently; the classifier gives you the facts, you apply your policy
- **Every entity maps to exactly one tag** — no ambiguity about where a result lands
- **Tags are additive** — a single field can carry multiple tags (e.g. `patient_ssn` → PHI + GOVERNMENT_ID)

---

## The 11 tags

### `PII` — Personally Identifiable Information
General-purpose personal data not covered by a more specific tag.

| Entity type | Examples |
|---|---|
| `EMAIL_ADDRESS` | alice@example.com |
| `PHONE_NUMBER` | +1-555-123-4567 |
| `PERSON` | First name, last name, full name |
| `DATE_TIME` | Date of birth, birthdate |
| `USERNAME` | Login handle, screen name |
| `EMPLOYEE_ID` | Staff ID, worker ID |
| `CUSTOMER_ID` | Client ID, account holder ID |
| `GENDER` | Gender, sex |
| `NATIONALITY` | Citizenship, nationality |
| `RELIGION` | Religious affiliation |
| `RACE_ETHNICITY` | Race, ethnicity |

Field name signals: `email`, `phone_number`, `first_name`, `last_name`, `date_of_birth`, `dob`, `gender`, `username`

---

### `PHI` — Protected Health Information
Healthcare data governed by HIPAA and similar regulations.

| Entity type | Examples |
|---|---|
| `MEDICAL_RECORD` | MRN, patient ID |
| `MEDICAL_CONDITION` | Diagnosis, ICD code, disease |
| `MEDICATION` | Drug name, prescription, dosage |
| `NATIONAL_PROVIDER_ID` | NPI number (healthcare provider) |
| `DEA_NUMBER` | Drug Enforcement Administration number |
| `HEALTH_INSURANCE` | Insurance ID, member ID, policy number |

Field name signals: `mrn`, `patient_id`, `diagnosis`, `medication`, `npi`, `dea_number`, `health_insurance_id`

---

### `PCI` — Payment Card Industry data
Payment card network data governed by PCI-DSS.

| Entity type | Examples |
|---|---|
| `CREDIT_CARD` | Card number (PAN), 4111-1111-1111-1111 |
| `IBAN_CODE` | GB29NWBK60161331926819 |
| `SWIFT_CODE` | BARCGB22 |
| `CRYPTO_WALLET` | Bitcoin/Ethereum wallet addresses |

Field name signals: `credit_card_number`, `iban`, `swift`, `crypto_wallet`, `pan`

---

### `CREDENTIALS` — Authentication secrets
Any secret that grants access to a system.

| Entity type | Examples |
|---|---|
| `PASSWORD` | Plaintext or hashed password |
| `API_KEY` | API key, API secret |
| `SECRET_KEY` | Generic secret |
| `ACCESS_TOKEN` | OAuth token, bearer token, auth token |
| `JWT_TOKEN` | JSON Web Token |
| `AWS_ACCESS_KEY` | AKIA... access key |
| `CONNECTION_STRING` | postgresql://user:pass@host/db |
| `PRIVATE_KEY` | PEM-encoded private key |

Field name signals: `password`, `api_key`, `access_token`, `jwt`, `connection_string`, `private_key`

---

### `FINANCIAL` — Bank account data
Financial account identifiers that are not payment card data.

| Entity type | Examples |
|---|---|
| `BANK_ACCOUNT` | Account number (8–17 digits) |
| `US_BANK_ROUTING` | 9-digit ABA routing number |

Field name signals: `account_number`, `bank_account`, `routing_number`

---

### `GOVERNMENT_ID` — Government-issued identifiers
State-issued identification numbers.

| Entity type | Examples |
|---|---|
| `US_SSN` | 123-45-6789 |
| `NIN` | UK National Insurance number |
| `PASSPORT` | Passport number |
| `DRIVER_LICENSE` | Driver's licence number |
| `US_ITIN` | Individual Taxpayer Identification Number |
| `AU_TFN` | Australian Tax File Number |
| `SIN` | Canadian Social Insurance Number |

Field name signals: `ssn`, `passport_number`, `driver_license`, `nin`, `au_tfn`

---

### `BIOMETRIC` — Biometric identifiers
Unique physical or behavioural characteristics.

| Entity type | Examples |
|---|---|
| `BIOMETRIC` | Fingerprint hash, facial geometry, retina scan, voiceprint |

Field name signals: `fingerprint`, `facial_recognition`, `retina_scan`, `biometric`

---

### `GENETIC` — Genetic and genomic data
DNA and genome data subject to strict regulations in many jurisdictions.

| Entity type | Examples |
|---|---|
| `GENETIC` | DNA sequence, genome, genotype, SNP data |

Field name signals: `dna_sequence`, `genome`, `genotype`, `genetic`

---

### `NPI` — Non-Public Information
Material non-public information (MNPI) — insider financial data.

| Entity type | Examples |
|---|---|
| `INSIDER_INFO` | Pre-announcement earnings, insider trading data |
| `EARNINGS_DATA` | Unreleased earnings forecast, financial guidance |
| `MERGER_ACQUISITION` | M&A deal information |

Field name signals: `mnpi`, `insider`, `nonpublic`, `earnings`, `merger`, `acquisition`

> Note: `NPI` as a tag means Non-Public Information. The healthcare concept "National Provider Identifier" maps to entity type `NATIONAL_PROVIDER_ID` → tag `PHI`.

---

### `LOCATION` — Geolocation and network identifiers
Precise location data that can identify an individual's movements.

| Entity type | Examples |
|---|---|
| `LOCATION` | Street address, city+postcode |
| `IP_ADDRESS` | IPv4/IPv6 address |

Field name signals: `address`, `street`, `city`, `latitude`, `longitude`, `ip_address`, `geolocation`

---

### `MINOR` — Data relating to minors
Data about individuals under 13 (COPPA) or 16 (GDPR Art. 8).

| Entity type | Examples |
|---|---|
| `MINOR_DATA` | Child ID, minor data, juvenile record |

Field name signals: `child_id`, `minor_data`, `child`, `juvenile`

---

## Entity type → tag mapping

Full mapping defined in `classifier-service/classification/taxonomy.py`:

```python
ENTITY_TAG = {
    # PII
    "EMAIL_ADDRESS":   DataTag.PII,
    "PHONE_NUMBER":    DataTag.PII,
    "PERSON":          DataTag.PII,
    "DATE_TIME":       DataTag.PII,
    "USERNAME":        DataTag.PII,
    "EMPLOYEE_ID":     DataTag.PII,
    "CUSTOMER_ID":     DataTag.PII,
    "GENDER":          DataTag.PII,
    "NATIONALITY":     DataTag.PII,
    "RELIGION":        DataTag.PII,
    "RACE_ETHNICITY":  DataTag.PII,

    # PHI
    "MEDICAL_RECORD":      DataTag.PHI,
    "MEDICAL_CONDITION":   DataTag.PHI,
    "MEDICATION":          DataTag.PHI,
    "NATIONAL_PROVIDER_ID":DataTag.PHI,
    "DEA_NUMBER":          DataTag.PHI,
    "HEALTH_INSURANCE":    DataTag.PHI,

    # PCI
    "CREDIT_CARD":    DataTag.PCI,
    "IBAN_CODE":      DataTag.PCI,
    "SWIFT_CODE":     DataTag.PCI,
    "CRYPTO_WALLET":  DataTag.PCI,

    # CREDENTIALS
    "PASSWORD":          DataTag.CREDENTIALS,
    "API_KEY":           DataTag.CREDENTIALS,
    "SECRET_KEY":        DataTag.CREDENTIALS,
    "ACCESS_TOKEN":      DataTag.CREDENTIALS,
    "JWT_TOKEN":         DataTag.CREDENTIALS,
    "AWS_ACCESS_KEY":    DataTag.CREDENTIALS,
    "CONNECTION_STRING": DataTag.CREDENTIALS,
    "PRIVATE_KEY":       DataTag.CREDENTIALS,

    # FINANCIAL
    "BANK_ACCOUNT":    DataTag.FINANCIAL,
    "US_BANK_ROUTING": DataTag.FINANCIAL,

    # GOVERNMENT_ID
    "US_SSN":         DataTag.GOVERNMENT_ID,
    "NIN":            DataTag.GOVERNMENT_ID,
    "PASSPORT":       DataTag.GOVERNMENT_ID,
    "DRIVER_LICENSE": DataTag.GOVERNMENT_ID,
    "US_ITIN":        DataTag.GOVERNMENT_ID,
    "AU_TFN":         DataTag.GOVERNMENT_ID,
    "SIN":            DataTag.GOVERNMENT_ID,

    # BIOMETRIC
    "BIOMETRIC": DataTag.BIOMETRIC,

    # GENETIC
    "GENETIC": DataTag.GENETIC,

    # NPI
    "INSIDER_INFO":       DataTag.NPI,
    "EARNINGS_DATA":      DataTag.NPI,
    "MERGER_ACQUISITION": DataTag.NPI,

    # LOCATION
    "LOCATION":   DataTag.LOCATION,
    "IP_ADDRESS": DataTag.LOCATION,

    # MINOR
    "MINOR_DATA": DataTag.MINOR,
}
```

Any entity type not in this map defaults to `DataTag.PII`.

---

## Confidence scoring

Scores run from 0.0 to 1.0 and are set per recogniser:

| Layer | Source | Score range | Notes |
|---|---|---|---|
| 1 | Field name | 0.78 – 0.95 | Deterministic — based on keyword match quality |
| 2 | Regex | 0.40 – 0.99 | Higher for more specific patterns (JWT > bank account) |
| 3 | AI model | 0.50 – 0.99 | spaCy built-ins + GLiNER contextual scores |

### Confidence tiers (used by review-api)

| Tier | Range | Recommended action |
|---|---|---|
| HIGH | ≥ 0.85 | Bulk-approve — high reliability |
| MEDIUM | 0.60 – 0.84 | Review individually |
| LOW | < 0.60 | Inspect example snippet before approving |

### Priority when multiple tags detected on the same field

When the streaming pipeline or Flink scanner needs to pick a single tag (e.g. for Stream Catalog), it uses this priority order:

```
PHI > CREDENTIALS > PCI > FINANCIAL > GOVERNMENT_ID > BIOMETRIC > GENETIC > NPI > PII > LOCATION > MINOR
```

The review-api surfaces all detected `(field, tag)` pairs separately — users can approve whichever tags are relevant to them.
