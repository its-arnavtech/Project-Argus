"""
Chunk 11 Step 5: full-scale model evaluation.

Chunk 6 reported precision/recall/F1/PR-AUC/FP-rate against the held-out
TEST SPLIT ONLY (51 positives, ring-component 20% holdout). This script
evaluates the same trained artifacts against EVERY known ring label in the
full graph (all 315 ring accounts + all 39,974 legit accounts) using the
identical `eval_split` logic from train_gnn.py, just with an all-True mask
instead of the test-only mask -- reusing the exact same feature
construction, normalization, and metric code as training so the two
numbers are directly comparable.

This is expected to look different from the held-out number, honestly:
it includes the 60% of rings the model was TRAINED on (so precision/recall
there should look best-case), diluted with the 20% val split. It is NOT a
replacement for the held-out test number -- it answers a different
question ("how does the model do on everything it's ever seen or been
scored against, at full scale") rather than "how does it generalize to
unseen rings" (that's what the test split already answered in Chunk 6).

Run: .venv/Scripts/python.exe ml/training/eval_full_scale.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "ml"))
sys.path.insert(0, str(REPO_ROOT / "ml" / "training"))

from model_def import InstitutionalFraudSAGE  # noqa: E402
from features import build_features_and_graph  # noqa: E402
from train_gnn import eval_split, ring_component_split, SEED  # noqa: E402

ARTIFACT_DIR = REPO_ROOT / "ml" / "artifacts"


def main() -> None:
    torch.manual_seed(SEED)
    rng = np.random.default_rng(SEED)

    cfg = json.loads((ARTIFACT_DIR / "model_config.json").read_text())
    stats = json.loads((ARTIFACT_DIR / "feature_stats.json").read_text())
    mu = np.array(stats["mean"], dtype=np.float32)
    sigma = np.array(stats["std"], dtype=np.float32)

    model = InstitutionalFraudSAGE(cfg["in_channels"], cfg["hidden_channels"], cfg["out_channels"])
    model.load_state_dict(torch.load(ARTIFACT_DIR / "model.pt", weights_only=True))
    model.eval()

    acct_ids, x_np, edge_index_np, y_np, ring_id, meta = build_features_and_graph()
    print(f"[EVAL-FULL] full graph: {meta['n_nodes']} nodes, {meta['n_edges_directed']} directed edges, "
          f"{int(y_np.sum())} positives ({int(y_np.sum()) / len(y_np) * 100:.2f}%)")

    x_norm = (x_np - mu) / sigma
    x = torch.tensor(x_norm)
    edge_index = torch.tensor(edge_index_np)

    # Recompute the exact same ring-component split used in training so we
    # can ALSO report the held-out-test-only number here for a clean,
    # apples-to-apples comparison line, not just quote Chunk 6's log.
    train_mask, val_mask, test_mask, n_components = ring_component_split(ring_id, y_np, rng)
    all_mask = np.ones(len(y_np), dtype=bool)

    test_result = eval_split(model, x, edge_index, y_np, test_mask)
    full_result = eval_split(model, x, edge_index, y_np, all_mask)

    print("\n=== HELD-OUT TEST SPLIT (recomputed, should match Chunk 6) ===")
    print(json.dumps(test_result, indent=2))

    print("\n=== FULL GRAPH (ALL known ring labels, train+val+test combined) ===")
    print(json.dumps(full_result, indent=2))

    print("\n=== COMPARISON ===")
    print(f"{'metric':12s} {'test-split':>12s} {'full-graph':>12s}")
    for k in ("precision", "recall", "f1", "pr_auc", "fp_rate"):
        print(f"{k:12s} {test_result[k]:12.4f} {full_result[k]:12.4f}")
    print(f"{'tp/fp/fn/tn':12s} "
          f"{test_result['tp']}/{test_result['fp']}/{test_result['fn']}/{test_result['tn']:>6} "
          f"{full_result['tp']}/{full_result['fp']}/{full_result['fn']}/{full_result['tn']:>6}")


if __name__ == "__main__":
    main()
