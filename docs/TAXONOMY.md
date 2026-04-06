# Classification Taxonomy

The taxonomy is the single source of truth for how entity types map to data categories and sensitivity levels.
All classification logic derives from `classifier-service/classification/taxonomy.py` — no sensitivity rules live anywhere else.

## Layers

```
Detected text
    │
    ▼
Entity type          e.g.  "MEDICAL_RECORD"
    │  (categorize_entity)
    ▼
DataCategory         e.g.  PHI
    │  (sensitivity_for_categories)
    ▼
SensitivityLevel     e.g.  CRITICAL
```

## DataCategory

| Category | Description | Typical industries |
|---|---|---|
| `PHI` | Protected Health Information | Healthcare, insurance, pharma |
| `CREDENTIALS` | Authentication secrets — passwords, tokens, API keys | Any (especially DevOps/SaaS) |
| `PII` | Personally Identifiable Information | All industries |
| `PCI` | Payment card and financial account data | Retail, finance, e-commerce |
| `CONFIDENTIAL` | Business-confidential data | All industries |
| `INTERNAL` | Internal operational data, low risk | All industries |

## SensitivityLevel

Derived from the **highest-priority** DataCategory present in a field or message.

| Level | Triggered by | Recommended action |
|---|---|---|
| `CRITICAL` | PHI or CREDENTIALS | Block / quarantine / alert immediately |
| `HIGH` | PII or PCI | Route to restricted topic, enforce RBAC |
| `MEDIUM` | CONFIDENTIAL only | Tag and audit |
| `LOW` | INTERNAL only | Tag for catalog visibility |
| `CLEAN` | Nothing detected | Pass through unchanged |

Priority order: `CRITICAL > HIGH > MEDIUM > LOW > CLEAN`

## Entity type catalogue

### PII — Personally Identifiable Information → HIGH

| Entity type | Description | Detection method |
|---|---|---|
| `PERSON` | Full or partial name | spaCy NER + GLiNER |
| `EMAIL_ADDRESS` | Email address | Presidio regex |
| `PHONE_NUMBER` | Phone number (international formats) | Presidio regex |
| `LOCATION` | Physical address or city | spaCy NER + GLiNER |
| `DATE_TIME` | Date of birth (context-sensitive) | spaCy NER + GLiNER |
| `US_SSN` | US Social Security Number | Presidio regex |
| `NIN` | UK National Insurance Number | GLiNER |
| `SIN` | Canadian Social Insurance Number | GLiNER |
| `AU_TFN` | Australian Tax File Number | GLiNER |
| `US_ITIN` | US Individual Taxpayer Identification Number | Presidio regex |
| `PASSPORT` | Passport number | Presidio regex + GLiNER |
| `DRIVER_LICENSE` | Driver's licence number | Presidio regex + GLiNER |
| `IP_ADDRESS` | IPv4 / IPv6 address | Presidio regex |
| `GENDER` | Gender identity | GLiNER |
| `NATIONALITY` | Nationality | GLiNER |
| `RELIGION` | Religious affiliation | GLiNER |
| `RACE_ETHNICITY` | Race or ethnicity | GLiNER |
| `USERNAME` | Login username | GLiNER |
| `EMPLOYEE_ID` | Employee identifier | GLiNER |
| `CUSTOMER_ID` | Customer / member identifier | GLiNER |

### PCI — Payment Card Industry → HIGH

| Entity type | Description | Detection method |
|---|---|---|
| `CREDIT_CARD` | Credit or debit card number (Luhn validated) | Presidio regex |
| `BANK_ACCOUNT` | Bank account number (8–17 digits) | Custom regex |
| `IBAN_CODE` | International Bank Account Number | Custom regex |
| `SWIFT_CODE` | SWIFT / BIC code | Custom regex (country-anchored) |
| `US_BANK_ROUTING` | US ABA routing number | Custom regex |
| `CRYPTO_WALLET` | Bitcoin or Ethereum wallet address | Custom regex |

### PHI — Protected Health Information → CRITICAL

| Entity type | Description | Detection method |
|---|---|---|
| `MEDICAL_RECORD` | Medical record number (MRN) | GLiNER |
| `HEALTH_INSURANCE` | Health insurance / member ID | Custom regex + GLiNER |
| `MEDICAL_CONDITION` | Diagnosis or medical condition | GLiNER |
| `MEDICATION` | Medication or prescription name | GLiNER |
| `NPI` | US National Provider Identifier (10-digit) | Custom regex |
| `DEA_NUMBER` | DEA registration number | Custom regex |
| `BIOMETRIC` | Biometric data (fingerprint, facial scan) | GLiNER |

### CREDENTIALS — Authentication secrets → CRITICAL

| Entity type | Description | Detection method |
|---|---|---|
| `PASSWORD` | Password or passphrase | GLiNER |
| `API_KEY` | Generic API key (hex/alphanumeric, 32–64 chars) | Custom regex |
| `AWS_ACCESS_KEY` | AWS access key ID (`AKIA...`) | Custom regex |
| `JWT_TOKEN` | JSON Web Token | Custom regex |
| `SECRET_KEY` | Secret / private key material | GLiNER |
| `ACCESS_TOKEN` | Bearer / access token | GLiNER |
| `PRIVATE_KEY` | PEM private key | GLiNER |
| `CONNECTION_STRING` | Database URL with embedded credentials | Custom regex |

### CONFIDENTIAL — Business sensitive → MEDIUM

| Entity type | Description | Detection method |
|---|---|---|
| `ORGANIZATION` | Company or organisation name | spaCy NER + GLiNER |
| `CONTRACT_NUMBER` | Contract or agreement number | GLiNER |
| `TRADE_SECRET` | Trade secret reference | GLiNER |

### INTERNAL — Operational data → LOW

| Entity type | Description | Detection method |
|---|---|---|
| `ORDER_NUMBER` | Order or transaction ID | GLiNER |
| `LOYALTY_CARD` | Loyalty / rewards card number | GLiNER |
| `LICENSE_PLATE` | Vehicle licence plate | GLiNER |
| `VEHICLE_ID` | VIN or vehicle identifier | GLiNER |
| `PRODUCT_ID` | Product ID or SKU | GLiNER |
| `SHIPMENT_ID` | Shipment or tracking number | GLiNER |

## Extending the taxonomy

To add a new entity type:

1. Add it to `ENTITY_CATEGORY` in `taxonomy.py`:
   ```python
   "MY_NEW_ENTITY": DataCategory.PII,
   ```

2. Add a GLiNER label in `gliner_recognizer.py`:
   ```python
   "my natural language label": "MY_NEW_ENTITY",
   ```

3. Optionally add a regex recognizer if the format is structural.

4. Add a test in `tests/test_taxonomy.py` and the relevant recognizer test file.
