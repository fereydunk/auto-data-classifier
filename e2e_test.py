#!/usr/bin/env python3
"""
End-to-end classification demo with sample data from multiple industries.

Demonstrates that the classifier is completely domain-agnostic — retail,
healthcare, finance, HR, and DevOps data all flow through the same pipeline
and surface the right categories and sensitivity levels.

Usage:
    python e2e_test.py

Note: GLiNER is stubbed here (ML model not installed in the test venv).
      In the Docker image, GLiNER is pre-downloaded and provides NER-based
      detection on top of the regex layer shown here.
"""

import sys
import json
from pathlib import Path
from unittest.mock import MagicMock

# ── Stub GLiNER (not installed outside Docker) ───────────────────────────────
_gliner_mock = MagicMock()
_gliner_mock.GLiNER.from_pretrained.return_value.predict_entities.return_value = []
sys.modules["gliner"] = _gliner_mock

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "classifier-service"))

# ── Imports ───────────────────────────────────────────────────────────────────
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from classification.taxonomy import categorize_entity, sensitivity_for_categories, DataCategory
from recognizers.gliner_recognizer import GLiNERRecognizer
from recognizers.pci_recognizers import get_pci_recognizers
from recognizers.phi_recognizers import get_phi_recognizers
from recognizers.credentials_recognizers import get_credentials_recognizers


# ── ANSI colours ──────────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
BLUE   = "\033[94m"
MAGENTA= "\033[95m"
GREY   = "\033[90m"

LEVEL_COLOR = {
    "CRITICAL": RED,
    "HIGH":     YELLOW,
    "MEDIUM":   CYAN,
    "LOW":      GREEN,
    "CLEAN":    GREY,
}

CATEGORY_COLOR = {
    "PHI":          RED,
    "CREDENTIALS":  RED,
    "PII":          YELLOW,
    "PCI":          YELLOW,
    "CONFIDENTIAL": CYAN,
    "INTERNAL":     GREEN,
}


# ── Sample data ───────────────────────────────────────────────────────────────
SAMPLES = [
    {
        "industry": "Retail",
        "scenario": "Customer order with payment info",
        "message": {
            "order_id": "ORD-2026-00123",
            "customer": {
                "name": "Alice Johnson",
                "email": "alice.johnson@gmail.com",
                "phone": "+1-555-234-5678",
                "loyalty_card": "LYL-98765432",
            },
            "payment": {
                "card_number": "4111111111111111",
                "billing_zip": "98101",
            },
            "shipping_address": "123 Main St, Seattle, WA 98101",
        },
    },
    {
        "industry": "Healthcare",
        "scenario": "Patient admission record",
        "message": {
            "mrn": "MRN-2026-00456",
            "patient": {
                "name": "Bob Smith",
                "dob": "1975-03-15",
                "ssn": "123-45-6789",
                "phone": "555-987-6543",
            },
            "clinical": {
                "diagnosis": "Type 2 diabetes mellitus",
                "medication": "Metformin 500mg twice daily",
                "npi_provider": "1234567890",
                "dea_number": "AB1234563",
            },
            "insurance": {
                "member_id": "BC1234567890",
                "plan": "BlueCross Gold PPO",
            },
        },
    },
    {
        "industry": "Financial Services",
        "scenario": "Wire transfer instruction",
        "message": {
            "transaction_id": "TXN-20260406-789012",
            "sender": {
                "account_number": "12345678901234",
                "iban": "GB29NWBK60161331926819",
                "swift_bic": "CHASUS33XXX",
                "routing": "021000021",
            },
            "beneficiary": {
                "name": "Acme Corp",
                "iban": "DE89370400440532013000",
            },
            "amount": "50000.00",
            "currency": "USD",
        },
    },
    {
        "industry": "HR / Payroll",
        "scenario": "Employee onboarding record",
        "message": {
            "employee_id": "EMP-2026-0042",
            "personal": {
                "name": "Carol Davis",
                "email": "carol.davis@acmecorp.com",
                "phone": "415-555-0101",
                "ssn": "987-65-4321",
                "dob": "1988-07-22",
            },
            "payroll": {
                "bank_account": "98765432109876",
                "routing_number": "111000038",
                "salary": "95000",
            },
            "department": "Engineering",
        },
    },
    {
        "industry": "DevOps / SaaS",
        "scenario": "Leaked config in log event",
        "message": {
            "service": "payment-processor",
            "level": "ERROR",
            "event": "DB connection failed",
            "config": {
                "aws_access_key": "AKIAIOSFODNN7EXAMPLE",
                "db_url": "postgresql://admin:secret123@prod-db.internal:5432/payments",
                "jwt_token": (
                    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                    ".eyJzdWIiOiJ1c2VyMTIzIn0"
                    ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
                ),
            },
        },
    },
    {
        "industry": "Retail / E-commerce",
        "scenario": "Clean product catalog entry",
        "message": {
            "product_id": "SKU-8891234",
            "name": "Wireless Bluetooth Headphones",
            "category": "Electronics",
            "price": "79.99",
            "stock": "342",
            "warehouse": "SEA-3",
        },
    },
    {
        "industry": "Web3 / Crypto",
        "scenario": "Transaction log with wallet addresses",
        "message": {
            "tx_hash": "0xabc123",
            "from_wallet": "0x71C7656EC7ab88b098defB751B7401B5f6d8976F",
            "to_wallet": "0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BA",
            "amount_eth": "1.5",
            "gas_price": "21000",
        },
    },
]


# ── Classifier setup ──────────────────────────────────────────────────────────
def build_analyzer() -> AnalyzerEngine:
    nlp_config = {
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
    }
    provider = NlpEngineProvider(nlp_configuration=nlp_config)
    nlp_engine = provider.create_engine()

    registry = RecognizerRegistry()
    registry.load_predefined_recognizers(nlp_engine=nlp_engine)
    registry.add_recognizer(GLiNERRecognizer())       # stubbed → no NER hits
    for r in get_pci_recognizers():
        registry.add_recognizer(r)
    for r in get_phi_recognizers():
        registry.add_recognizer(r)
    for r in get_credentials_recognizers():
        registry.add_recognizer(r)

    return AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])


def flatten(obj, prefix=""):
    flat = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            flat.update(flatten(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            flat.update(flatten(v, f"{prefix}[{i}]"))
    elif isinstance(obj, str) and obj.strip():
        flat[prefix] = obj
    elif obj is not None:
        flat[prefix] = str(obj)
    return flat


def classify(analyzer: AnalyzerEngine, message: dict) -> dict:
    flat = flatten(message)
    detected = {}
    all_categories: set[DataCategory] = set()

    for field, text in flat.items():
        results = analyzer.analyze(text=text, language="en")
        if results:
            entries = []
            for r in results:
                cat = categorize_entity(r.entity_type)
                all_categories.add(cat)
                entries.append({
                    "entity_type": r.entity_type,
                    "category": cat.value,
                    "score": round(r.score, 3),
                    "value": text[r.start:r.end],
                })
            detected[field] = entries

    sensitivity = sensitivity_for_categories(all_categories)
    return {
        "sensitivity_level": sensitivity.value,
        "categories": sorted(c.value for c in all_categories),
        "detected_entities": detected,
    }


# ── Pretty printing ───────────────────────────────────────────────────────────
def print_result(sample: dict, result: dict):
    level = result["sensitivity_level"]
    lc = LEVEL_COLOR.get(level, RESET)

    print(f"\n{'─'*70}")
    print(f"  {BOLD}Industry:{RESET}  {sample['industry']}")
    print(f"  {BOLD}Scenario:{RESET}  {sample['scenario']}")
    print(f"  {BOLD}Sensitivity:{RESET} {lc}{BOLD}{level}{RESET}")

    cats = result["categories"]
    if cats:
        colored_cats = "  ".join(
            f"{CATEGORY_COLOR.get(c, RESET)}{c}{RESET}" for c in cats
        )
        print(f"  {BOLD}Categories:{RESET}  {colored_cats}")
    else:
        print(f"  {BOLD}Categories:{RESET}  {GREY}none{RESET}")

    detected = result["detected_entities"]
    if detected:
        print(f"\n  {BOLD}Detected fields:{RESET}")
        for field, entities in detected.items():
            for e in entities:
                cc = CATEGORY_COLOR.get(e["category"], RESET)
                score_color = RED if e["score"] >= 0.85 else YELLOW if e["score"] >= 0.5 else GREY
                print(
                    f"    {BLUE}{field:<35}{RESET}"
                    f"  {cc}{e['category']:<12}{RESET}"
                    f"  {GREY}{e['entity_type']:<22}{RESET}"
                    f"  {score_color}score={e['score']:.3f}{RESET}"
                    f"  {GREY}→ {e['value']!r:.30}{RESET}"
                )
    else:
        print(f"\n  {GREY}  No sensitive data detected.{RESET}")


def print_summary(results: list[tuple[dict, dict]]):
    print(f"\n\n{'═'*70}")
    print(f"  {BOLD}SUMMARY{RESET}")
    print(f"{'═'*70}")
    total = len(results)
    by_level = {}
    for _, r in results:
        l = r["sensitivity_level"]
        by_level[l] = by_level.get(l, 0) + 1

    level_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "CLEAN"]
    for level in level_order:
        count = by_level.get(level, 0)
        if count:
            lc = LEVEL_COLOR.get(level, RESET)
            bar = "█" * count
            print(f"  {lc}{level:<10}{RESET}  {bar}  ({count}/{total})")
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{BOLD}{'═'*70}{RESET}")
    print(f"{BOLD}  Auto Data Classifier — End-to-End Demo{RESET}")
    print(f"{BOLD}{'═'*70}{RESET}")
    print(f"  Building analyzer (spaCy + Presidio + regex recognizers)...")

    analyzer = build_analyzer()
    recognizer_count = len(analyzer.registry.recognizers)
    print(f"  {GREEN}✓{RESET} Analyzer ready  ({recognizer_count} recognizers loaded)")
    print(f"  {GREY}  Note: GLiNER is stubbed — NER-based entities (names, addresses){RESET}")
    print(f"  {GREY}  will appear in production. Regex detections are real.{RESET}")

    results = []
    for sample in SAMPLES:
        result = classify(analyzer, sample["message"])
        print_result(sample, result)
        results.append((sample, result))

    print_summary(results)


if __name__ == "__main__":
    main()
