#!/usr/bin/env python3
"""
Auto Data Classifier — end-to-end demonstration (v3.1.0)

Runs the three-layer classification engine directly against realistic
sample data from five industry verticals, and prints a rich console
report showing every detected field, its tag, confidence, and which
layer found it.

Usage:
    python e2e_test.py              # all verticals, all layers (default)
    python e2e_test.py --layer 2   # field name + regex only (no AI model)
    python e2e_test.py --layer 1   # field name only
    python e2e_test.py --vertical healthcare

No Confluent Cloud connection or Docker required.
GLiNER is stubbed when not installed so the demo always runs.

The 11 supported tags:
    PII           Personally Identifiable Information
    PHI           Protected Health Information
    PCI           Payment Card data
    CREDENTIALS   Passwords, API keys, tokens, connection strings
    FINANCIAL     Bank accounts, routing numbers
    GOVERNMENT_ID Passports, SSN, driver's licence, national IDs
    BIOMETRIC     Fingerprints, facial geometry, retina scans
    GENETIC       DNA sequences, genomic data
    NPI           Non-Public Information (insider / pre-release financials)
    LOCATION      GPS coordinates, IP addresses, geolocation
    MINOR         Data relating to persons under 13 or 16
"""

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock

# ── Stub GLiNER when not installed ──────────────────────────────────────────
try:
    import gliner  # noqa: F401
    GLINER_AVAILABLE = True
except ImportError:
    _gliner_mock = MagicMock()
    _gliner_mock.GLiNER.from_pretrained.return_value.predict_entities.return_value = []
    sys.modules["gliner"] = _gliner_mock
    GLINER_AVAILABLE = False

# ── Path setup ───────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "classifier-service"))

# ── Imports ──────────────────────────────────────────────────────────────────
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from classification.taxonomy import DataTag, tag_entity
from recognizers.field_name_recognizer import classify_field_name, is_free_text
from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.pci_recognizers import get_pci_recognizers
from recognizers.phi_recognizers import get_phi_recognizers
from recognizers.credentials_recognizers import get_credentials_recognizers
from recognizers.financial_recognizers import get_financial_recognizers

# ── ANSI colours ─────────────────────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
GREEN   = "\033[92m"
BLUE    = "\033[94m"
MAGENTA = "\033[95m"
WHITE   = "\033[97m"
ORANGE  = "\033[38;5;208m"

TAG_COLOURS = {
    "PII":           CYAN,
    "PHI":           RED,
    "PCI":           YELLOW,
    "CREDENTIALS":   MAGENTA,
    "FINANCIAL":     ORANGE,
    "GOVERNMENT_ID": BLUE,
    "BIOMETRIC":     GREEN,
    "GENETIC":       GREEN,
    "NPI":           RED,
    "LOCATION":      CYAN,
    "MINOR":         YELLOW,
}

LAYER_LABEL  = {1: "field_name", 2: "regex",  3: "ai_model"}
LAYER_COLOUR = {1: GREEN,        2: YELLOW,    3: CYAN}


# ── Sample data — five industry verticals ─────────────────────────────────────

VERTICALS = {

    "retail": {
        "label": "Retail / E-commerce — customer order",
        "fields": {
            "order_id":           "ORD-88421",
            "customer": {
                "first_name":     "Alice",
                "last_name":      "Smith",
                "email_address":  "alice.smith@example.com",
                "phone_number":   "+1-415-555-0192",
                "date_of_birth":  "1989-03-14",
                "ip_address":     "192.168.1.42",
            },
            "payment": {
                "credit_card_number": "4111-1111-1111-1111",
                "iban":               "GB29NWBK60161331926819",
                "billing_address": {
                    "street":      "123 Main St",
                    "city":        "San Francisco",
                    "postal_code": "94105",
                },
            },
            "notes": (
                "Customer requested gift wrapping and mentioned her daughter's birthday. "
                "She paid with her Visa ending 1111."
            ),
        },
    },

    "healthcare": {
        "label": "Healthcare — patient intake record",
        "fields": {
            "patient_id":          "MRN-204817",
            "mrn":                 "MRN-204817",
            "ssn":                 "123-45-6789",
            "first_name":          "Bob",
            "last_name":           "Jones",
            "date_of_birth":       "1952-07-22",
            "diagnosis":           "ICD10:E11 — Type 2 Diabetes Mellitus",
            "medication":          "Metformin 500mg twice daily",
            "npi":                 "1234567890",
            "dea_number":          "BJ1234563",
            "health_insurance_id": "INS-GHI-887765",
            "comment": (
                "Patient presented with elevated A1C and reported difficulty managing "
                "glucose levels. Dr. Smith prescribed Metformin 500mg. Patient's SSN "
                "is 123-45-6789 per intake form."
            ),
        },
    },

    "finance": {
        "label": "Financial Services — payment transaction",
        "fields": {
            "transaction_id":     "TXN-9944221",
            "account_number":     "12345678901234",
            "routing_number":     "021000021",
            "credit_card_number": "5500-0000-0000-0004",
            "swift_code":         "BARCGB22",
            "iban":               "DE89370400440532013000",
            "crypto_wallet":      "1A1zP1eP5QGefi2DMPTfTL5SLmv7Divf NA",
            "sender": {
                "first_name":     "Carlos",
                "email":          "carlos@acme.com",
            },
            "notes": (
                "Wire transfer flagged for manual review. Sender Carlos confirmed his "
                "account number ending 1234 and provided routing 021000021."
            ),
        },
    },

    "devops": {
        "label": "DevOps / SaaS — application config event",
        "fields": {
            "service":            "payment-processor",
            "environment":        "production",
            "username":           "deploy-bot",
            "password":           "Xk9!mQ2#rL7$vP4@",
            "api_key":            "ak_live_4xT7mKqRzL9pN2bW8sY3vH",
            "jwt":                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.signature",
            "connection_string":  "postgresql://admin:s3cr3t@db.internal:5432/prod",
            "private_key":        "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...",
            "access_token":       "ghp_16C7e42F292c6912E7710c838347Ae178B4a",
            "ip_address":         "10.0.1.55",
        },
    },

    "hr": {
        "label": "HR / Payroll — employee record (all 11 tags)",
        "fields": {
            "employee_id":    "EMP-55291",
            "first_name":     "Diana",
            "last_name":      "Müller",
            "email_address":  "diana.muller@corp.io",
            "ssn":            "987-65-4321",
            "passport_number":"P12345678",
            "driver_license": "DL7654321",
            "date_of_birth":  "1985-11-03",
            "bank_account":   "9876543210",
            "routing_number": "026009593",
            "fingerprint":    "FP:a3f4b2c1d5e6f7890abc",
            "dna_sequence":   "ATCGATCGATCGATCGATCGATCGATCGATCG",
            "genome":         "GRCh38:7:117548628",
            "ip_address":     "10.0.0.22",
            "child_id":       "MINOR-8821",
            "minor_data":     "child age_verified=false",
            "mnpi":           "Q3 earnings above analyst consensus — embargoed until 08:00 EST",
            "notes": (
                "Diana holds dual citizenship. Genome sequencing uploaded for research study. "
                "Her child (minor) is linked to this account."
            ),
        },
    },
}


# ── Classifier engine ──────────────────────────────────────────────────────────

def build_regex_analyzer() -> AnalyzerEngine:
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers()
    for r in (get_pci_recognizers() + get_phi_recognizers() +
              get_credentials_recognizers() + get_financial_recognizers()):
        registry.add_recognizer(r)
    return AnalyzerEngine(registry=registry, supported_languages=["en"])


def build_ai_analyzer() -> AnalyzerEngine:
    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)
    if GLINER_AVAILABLE:
        registry.add_recognizer(GLiNERRecognizer())
    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


def _flatten(obj, prefix=""):
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            flat.update(_flatten(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            flat.update(_flatten(v, f"{prefix}[{i}]"))
    else:
        flat[prefix] = str(obj) if obj is not None else ""
    return flat


def classify(fields: dict, max_layer: int, regex_analyzer, ai_analyzer):
    flat = _flatten(fields)
    detected = {}
    all_tags: set[str] = set()

    for field_path, value in flat.items():
        leaf = field_path.split(".")[-1].strip("[]0123456789")
        entries = []

        # Layer 1 — field name (always runs)
        for m in classify_field_name(leaf):
            entries.append({
                "entity_type": m.entity_type,
                "tag":         m.tag,
                "score":       m.score,
                "layer":       1,
                "source":      "field_name",
                "snippet":     value[:60],
            })
            all_tags.add(m.tag)

        # Layer 2 — regex
        if max_layer >= 2:
            for r in regex_analyzer.analyze(text=value, language="en"):
                t = tag_entity(r.entity_type).value
                entries.append({
                    "entity_type": r.entity_type,
                    "tag":         t,
                    "score":       round(r.score, 3),
                    "layer":       2,
                    "source":      "regex",
                    "snippet":     value[r.start:r.end],
                })
                all_tags.add(t)

        # Layer 3 — AI model
        if max_layer >= 3:
            for r in ai_analyzer.analyze(text=value, language="en"):
                t = tag_entity(r.entity_type).value
                entries.append({
                    "entity_type": r.entity_type,
                    "tag":         t,
                    "score":       round(r.score, 3),
                    "layer":       3,
                    "source":      "ai_model",
                    "snippet":     value[r.start:r.end],
                })
                all_tags.add(t)

        if entries:
            # Deduplicate: best entity per (tag, entity_type)
            best: dict[tuple, dict] = {}
            for e in entries:
                key = (e["tag"], e["entity_type"])
                if key not in best or e["score"] > best[key]["score"]:
                    best[key] = e
            detected[field_path] = sorted(best.values(), key=lambda x: -x["score"])

    return detected, sorted(all_tags)


# ── Display ────────────────────────────────────────────────────────────────────

def _confidence_bar(score: float) -> str:
    filled = int(score * 10)
    bar = "█" * filled + "░" * (10 - filled)
    colour = GREEN if score >= 0.85 else (YELLOW if score >= 0.60 else RED)
    return f"{colour}{bar}{RESET} {score:.2f}"


def _tag_badge(tag: str) -> str:
    c = TAG_COLOURS.get(tag, WHITE)
    return f"{BOLD}{c}[{tag}]{RESET}"


def _layer_badge(layer: int) -> str:
    c = LAYER_COLOUR.get(layer, WHITE)
    return f"{c}L{layer}:{LAYER_LABEL.get(layer, '?')}{RESET}"


def print_vertical(label: str, fields: dict, max_layer: int, regex_analyzer, ai_analyzer):
    print(f"\n{'═' * 76}")
    print(f"{BOLD}{WHITE}  {label}{RESET}")
    print(f"{'─' * 76}")

    detected, all_tags = classify(fields, max_layer, regex_analyzer, ai_analyzer)

    if not detected:
        print(f"  {DIM}No sensitive fields detected.{RESET}")
    else:
        for field_path, entities in sorted(detected.items()):
            best = entities[0]
            snippet = best["snippet"][:45].replace("\n", " ")
            snippet_str = f'{DIM}"{snippet}"{RESET}' if snippet else ""
            print(
                f"  {BOLD}{field_path:<38}{RESET} "
                f"{_tag_badge(best['tag']):<28} "
                f"{_layer_badge(best['layer']):<28} "
                f"{_confidence_bar(best['score'])}  {snippet_str}"
            )
            # Secondary hits (different tag on same field)
            for e in entities[1:]:
                if e["tag"] != best["tag"]:
                    print(
                        f"  {'':38} "
                        f"{_tag_badge(e['tag']):<28} "
                        f"{_layer_badge(e['layer']):<28} "
                        f"{_confidence_bar(e['score'])}"
                    )

    tag_line = "  ".join(_tag_badge(t) for t in all_tags) if all_tags else f"{DIM}(none){RESET}"
    print(f"\n  {BOLD}Tags:{RESET} {tag_line}")


def print_summary(results: dict):
    print(f"\n{'═' * 76}")
    print(f"{BOLD}{WHITE}  CLASSIFICATION SUMMARY{RESET}")
    print(f"{'─' * 76}")

    all_possible = [t.value for t in DataTag]
    all_seen = set()
    for _, tags in results.values():
        all_seen.update(tags)

    print(f"\n  {'Vertical':<22}", end="")
    for tag in all_possible:
        c = TAG_COLOURS.get(tag, WHITE)
        print(f"{c}{tag[:4]}{RESET}  ", end="")
    print()
    print(f"  {'─' * 22}" + "─" * (6 * len(all_possible)))

    for v_key, (label, tags) in results.items():
        short = label.split("—")[0].strip()[:20]
        print(f"  {short:<22}", end="")
        for tag in all_possible:
            c = TAG_COLOURS.get(tag, WHITE)
            mark = f"{BOLD}{c}✓{RESET}   " if tag in tags else f"{DIM}·   {RESET}"
            print(mark, end="")
        print()

    print(f"\n  {BOLD}All 11 supported tags:{RESET}")
    descriptions = {
        "PII":           "Names, email, phone, date of birth, username",
        "PHI":           "Medical records, diagnoses, medications, NPI/DEA numbers",
        "PCI":           "Credit/debit cards, IBAN, SWIFT codes, crypto wallets",
        "CREDENTIALS":   "Passwords, API keys, tokens, connection strings, private keys",
        "FINANCIAL":     "Bank account numbers, routing numbers",
        "GOVERNMENT_ID": "SSN, passport, driver's licence, national tax IDs",
        "BIOMETRIC":     "Fingerprints, facial geometry, retina/iris scans, voiceprints",
        "GENETIC":       "DNA sequences, genome/genotype data",
        "NPI":           "Non-Public Information — insider financials, M&A data",
        "LOCATION":      "GPS coordinates, IP addresses, precise geolocation",
        "MINOR":         "Data relating to a person under 13 or 16 (COPPA / GDPR)",
    }
    for tag in all_possible:
        c = TAG_COLOURS.get(tag, WHITE)
        seen_marker = f"{GREEN}✓ detected{RESET}" if tag in all_seen else f"{DIM}not triggered{RESET}"
        print(f"    {BOLD}{c}{tag:<16}{RESET}  {descriptions.get(tag, '')}  [{seen_marker}]")

    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Auto Data Classifier — e2e demo v3.1.0")
    parser.add_argument("--layer",    type=int, default=3, choices=[1, 2, 3],
                        help="Max layer: 1=field name, 2=+regex, 3=+AI (default)")
    parser.add_argument("--vertical", default="all",
                        choices=["all"] + list(VERTICALS.keys()),
                        help="Industry vertical to run (default: all)")
    args = parser.parse_args()

    print(f"\n{BOLD}{'═' * 76}{RESET}")
    print(f"{BOLD}  Auto Data Classifier  ·  v3.1.0  ·  11 data tags  ·  3-layer engine{RESET}")
    print(f"{BOLD}{'═' * 76}{RESET}")
    layer_desc = {
        1: f"{GREEN}Layer 1{RESET} — field name only (zero data access)",
        2: f"{GREEN}Layer 1{RESET} + {YELLOW}Layer 2{RESET} — field name + regex patterns",
        3: f"{GREEN}Layer 1{RESET} + {YELLOW}Layer 2{RESET} + {CYAN}Layer 3{RESET} — field name + regex + AI model",
    }
    print(f"  Running: {layer_desc[args.layer]}")
    if not GLINER_AVAILABLE and args.layer == 3:
        print(f"  {DIM}(GLiNER not installed — Layer 3 uses spaCy NER only){RESET}")

    print("\n  Building analyzers…", end=" ", flush=True)
    regex_analyzer = build_regex_analyzer()
    ai_analyzer    = build_ai_analyzer() if args.layer >= 3 else regex_analyzer
    print("ready.\n")

    to_run = VERTICALS if args.vertical == "all" else {args.vertical: VERTICALS[args.vertical]}
    results = {}

    for key, v in to_run.items():
        print_vertical(v["label"], v["fields"], args.layer, regex_analyzer, ai_analyzer)
        _, tags = classify(v["fields"], args.layer, regex_analyzer, ai_analyzer)
        results[key] = (v["label"], set(tags))

    if args.vertical == "all":
        print_summary(results)

    print(f"{'═' * 76}\n")


if __name__ == "__main__":
    main()
