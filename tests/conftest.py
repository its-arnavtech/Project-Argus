"""Pytest path setup: the Python components live in non-package dirs
(data/scripts, graph, ml/*), so add them to sys.path for imports here rather
than restructuring the whole repo into packages just for tests."""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for sub in ("data/scripts", "graph", "ml", "ml/training", "ml/inference"):
    p = str(REPO_ROOT / sub)
    if p not in sys.path:
        sys.path.insert(0, p)
