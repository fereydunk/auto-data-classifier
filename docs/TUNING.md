# Tuning Guide

## Known false positives (observed in e2e demo)

### 1. SWIFT code matching common English words

**Symptom:** Words like `"Engineering"`, `"Electronics"`, `"acmecorp"` are flagged as `SWIFT_CODE` (PCI).

**Root cause:** Presidio's spaCy pipeline normalises text (lowercases tokens) before passing to pattern recognisers. The SWIFT regex `[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}` then matches lowercase 8-character words despite being uppercase-only.

**Fix options:**
- Add a `validate()` method to `SwiftCodeRecognizer` that checks the matched text is uppercase:
  ```python
  def validate_result(self, pattern_text: str) -> bool:
      return pattern_text == pattern_text.upper() and len(pattern_text) >= 8
  ```
- Raise the minimum score threshold (see below).

---

### 2. DATE_TIME firing on bare numbers

**Symptom:** Salary `"95000"`, account number `"12345678901234"` flagged as `DATE_TIME` (PII).

**Root cause:** spaCy's NER interprets long numeric strings as dates in some contexts.

**Fix:** Add a global minimum score threshold in `/classify`:
```python
results = analyzer.analyze(text=text, language=request.language)
results = [r for r in results if r.score >= 0.5]  # filter noise
```

---

### 3. ORGANIZATION matching currency codes and short tokens

**Symptom:** `"USD"`, `"EUR"` flagged as `ORGANIZATION` (CONFIDENTIAL → MEDIUM).

**Root cause:** spaCy NER misidentifies short all-caps tokens as organisation names.

**Fix:** Add a minimum token length validator for ORGANIZATION, or add a deny list:
```python
ORGANIZATION_DENY_LIST = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"}
```

---

### 4. Generic API key (hex) matching hash values

**Symptom:** MD5 / SHA hashes in log events flagged as `API_KEY`.

**Root cause:** `GenericAPIKeyRecognizer` matches 32–64 lowercase hex chars.

**Fix:** The score is already low (0.35). Set a threshold ≥ 0.4 in the endpoint to suppress it, or use GLiNER context — with GLiNER enabled, hex strings in a non-credential context will not have the label confirmed.

---

## Score threshold configuration

Add a configurable threshold to `main.py` to suppress low-confidence noise:

```python
MIN_SCORE = float(os.getenv("MIN_CLASSIFIER_SCORE", "0.0"))

# In classify():
results = analyzer.analyze(text=text, language=request.language)
results = [r for r in results if r.score >= MIN_SCORE]
```

Recommended starting values:

| Environment | `MIN_CLASSIFIER_SCORE` | Effect |
|---|---|---|
| Development / demo | `0.0` | See everything |
| Staging | `0.4` | Remove very low confidence hits |
| Production | `0.5` | Balance precision vs recall |

---

## Adjusting recognizer scores

Each `Pattern` in a recognizer has a `score` (0–1). The score represents confidence when that pattern fires in isolation. When multiple recognisers fire on the same text, Presidio returns all results independently — they are not merged.

To raise or lower a recognizer's confidence:

```python
# In pci_recognizers.py
Pattern(
    name="BANK_ACCOUNT",
    regex=r"\b[0-9]{8,17}\b",
    score=0.4,          # ← adjust this
)
```

Guidelines:
- `0.9–1.0`: Near-certain structural match (IBAN with checksum, credit card with Luhn, JWT with three segments)
- `0.7–0.9`: Strong structural match (SWIFT with country code, AWS `AKIA` prefix, DEA format)
- `0.5–0.7`: Probable match, needs context (NPI 10-digit, routing numbers)
- `0.3–0.5`: Ambiguous match, only useful with GLiNER context (generic account numbers, hex strings)

---

## Adding a deny list for a recognizer

Presidio's `PatternRecognizer` supports a `deny_list` and `context` list out of the box:

```python
class SwiftCodeRecognizer(PatternRecognizer):
    def __init__(self):
        super().__init__(
            supported_entity="SWIFT_CODE",
            patterns=self.PATTERNS,
            context=["swift", "bic", "wire", "transfer"],  # boosts score when nearby
        )
```

The `context` list raises the score when those words appear near the match. This is useful for low-confidence recognisers (routing numbers, account numbers) where field names provide strong signal.

---

## GLiNER threshold tuning

The GLiNER recogniser has a `threshold` parameter (default `0.4`). Lower values increase recall at the cost of precision:

```python
# In main.py → build_analyzer()
registry.add_recognizer(GLiNERRecognizer(threshold=0.5))  # stricter
```

In production with high-value data (healthcare, finance), start at `0.4` and tune based on observed false positives in the audit topic.

---

## Field-name-aware scoring (future enhancement)

A significant accuracy improvement is to weight entity scores by the field name. A 10-digit number in a field called `npi_provider` is almost certainly an NPI; the same number in a field called `timestamp` is almost certainly noise.

This can be implemented as a post-processing step in `classify()`:

```python
FIELD_NAME_HINTS = {
    "ssn": {"US_SSN": 0.3},
    "npi": {"NPI": 0.3},
    "iban": {"IBAN_CODE": 0.2},
    "email": {"EMAIL_ADDRESS": 0.1},
}

for field_name, results in detected.items():
    for hint_key, boosts in FIELD_NAME_HINTS.items():
        if hint_key in field_name.lower():
            for r in results:
                if r.entity_type in boosts:
                    r.score = min(1.0, r.score + boosts[r.entity_type])
```
