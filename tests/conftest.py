"""
Adds classifier-service and kafka-pipeline to sys.path so tests
can import from both without installing them as packages.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "classifier-service"))
sys.path.insert(0, str(ROOT / "kafka-pipeline"))
