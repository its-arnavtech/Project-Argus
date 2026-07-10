"""
Chunk 6: GNN training pipeline -- InstitutionalFraudSAGE per
docs/specs/POC_Blueprint.md section 3 (2-layer GraphSAGE, max aggregation),
trained with a REAL loop (optimizer, weighted loss, epochs, early stopping)
on the full real corpus, replacing the POC's single-forward-pass demo.

Split discipline: train/val/test hold out ENTIRE RINGS, not individual
nodes -- splitting nodes within one ring would leak structural signal from
train-set neighbors into test predictions through GraphSAGE aggregation
(transductive message passing sees all edges; only the label masks differ).
Because device_cluster rings reuse members from other rings (~40% of the
time, by design in ring_injector.py), rings sharing any member are first
merged into components via union-find, and components are what get split.

Class imbalance (~0.78% positive): weighted NLL loss with inverse-frequency
class weights. Chosen over under/oversampling because (a) transductive
full-graph training has no natural minibatch to resample, (b) discarding
99% of legit nodes would starve the negative class of the very diversity
the FP-rate target cares about, and (c) it's one hyperparameter-free line.

Run: python ml/training/train_gnn.py
MLflow tracking: local file store at ml/training/mlruns (gitignored).
Artifacts for Chunk 7: ml/artifacts/{model.pt,model_config.json,feature_stats.json}
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import mlflow
import mlflow.pytorch
import numpy as np
import torch
import torch.nn.functional as F
from sklearn.metrics import average_precision_score, confusion_matrix, precision_recall_fscore_support

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "ml"))

from model_def import InstitutionalFraudSAGE  # noqa: E402
from features import build_features_and_graph  # noqa: E402

ARTIFACT_DIR = REPO_ROOT / "ml" / "artifacts"
# MLflow 3.14+ hard-deprecated the ./mlruns filesystem backend (raises unless
# opted out); sqlite is the recommended local backend now. mlflow.db is
# gitignored alongside mlruns/.
MLFLOW_DB = Path(__file__).resolve().parent / "mlflow.db"

SEED = 42
HIDDEN = 64
DROPOUT = 0.2
LR = 0.01
WEIGHT_DECAY = 5e-4
EPOCHS = 300
PATIENCE = 40


def ring_component_split(ring_id: np.ndarray, y: np.ndarray, rng: np.random.Generator):
    """Assign nodes to train/val/test. Ring members: whole ring-components
    (rings merged when they share members) go to one split, ~60/20/20.
    Legit accounts: random 60/20/20."""
    n = len(y)

    ring_members: dict[str, list[int]] = {}
    for i, r in enumerate(ring_id):
        if r is not None and not (isinstance(r, float) and np.isnan(r)):
            ring_members.setdefault(str(r), []).append(i)

    # union-find over rings sharing members
    parent = {r: r for r in ring_members}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        parent[find(a)] = find(b)

    node_to_rings: dict[int, list[str]] = {}
    for r, members in ring_members.items():
        for m in members:
            node_to_rings.setdefault(m, []).append(r)
    for rings in node_to_rings.values():
        for other in rings[1:]:
            union(rings[0], other)

    components: dict[str, set[int]] = {}
    for r, members in ring_members.items():
        components.setdefault(find(r), set()).update(members)

    comp_list = list(components.values())
    rng.shuffle(comp_list)
    n_comp = len(comp_list)
    n_train_c = int(round(n_comp * 0.6))
    n_val_c = int(round(n_comp * 0.2))

    train_mask = np.zeros(n, dtype=bool)
    val_mask = np.zeros(n, dtype=bool)
    test_mask = np.zeros(n, dtype=bool)

    for ci, comp in enumerate(comp_list):
        target = train_mask if ci < n_train_c else val_mask if ci < n_train_c + n_val_c else test_mask
        for node in comp:
            target[node] = True

    legit = np.where(y == 0)[0]
    rng.shuffle(legit)
    n_train_l = int(len(legit) * 0.6)
    n_val_l = int(len(legit) * 0.2)
    train_mask[legit[:n_train_l]] = True
    val_mask[legit[n_train_l : n_train_l + n_val_l]] = True
    test_mask[legit[n_train_l + n_val_l :]] = True

    return train_mask, val_mask, test_mask, len(comp_list)


def eval_split(model, x, edge_index, y, mask) -> dict:
    model.eval()
    with torch.no_grad():
        logp = model(x, edge_index)
        prob1 = logp.exp()[:, 1].cpu().numpy()
        pred = logp.argmax(dim=1).cpu().numpy()
    ym, pm, probm = y[mask], pred[mask], prob1[mask]
    precision, recall, f1, _ = precision_recall_fscore_support(
        ym, pm, average="binary", zero_division=0
    )
    pr_auc = average_precision_score(ym, probm) if ym.sum() > 0 else float("nan")
    tn, fp, fn, tp = confusion_matrix(ym, pm, labels=[0, 1]).ravel()
    fp_rate = fp / (fp + tn) if (fp + tn) > 0 else 0.0
    return {
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "pr_auc": float(pr_auc),
        "fp_rate": float(fp_rate),
        "tp": int(tp), "fp": int(fp), "fn": int(fn), "tn": int(tn),
    }


def main() -> None:
    torch.manual_seed(SEED)
    rng = np.random.default_rng(SEED)

    print("[TRAIN] building features + graph from full real corpus...")
    acct_ids, x_np, edge_index_np, y_np, ring_id, meta = build_features_and_graph()
    print(f"[TRAIN] {meta}")

    train_mask, val_mask, test_mask, n_components = ring_component_split(ring_id, y_np, rng)
    for name, m in (("train", train_mask), ("val", val_mask), ("test", test_mask)):
        print(f"[TRAIN] {name}: {m.sum()} nodes, {int(y_np[m].sum())} positives")

    # z-score using TRAIN-mask statistics only (no peeking at val/test)
    mu = x_np[train_mask].mean(axis=0)
    sigma = x_np[train_mask].std(axis=0)
    sigma[sigma == 0] = 1.0
    x_scaled = (x_np - mu) / sigma

    x = torch.tensor(x_scaled)
    edge_index = torch.tensor(edge_index_np)
    y = torch.tensor(y_np)

    n_pos = int(y_np[train_mask].sum())
    n_neg = int(train_mask.sum()) - n_pos
    class_weights = torch.tensor([1.0, n_neg / max(n_pos, 1)], dtype=torch.float32)
    print(f"[TRAIN] class weights: {class_weights.tolist()}")

    model = InstitutionalFraudSAGE(x.shape[1], HIDDEN)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    train_idx = torch.tensor(np.where(train_mask)[0])

    mlflow.set_tracking_uri(f"sqlite:///{MLFLOW_DB.as_posix()}")
    mlflow.set_experiment("argus-gnn")
    with mlflow.start_run(run_name="chunk6-institutional-fraud-sage"):
        mlflow.log_params(
            {
                "hidden": HIDDEN, "dropout": DROPOUT, "lr": LR,
                "weight_decay": WEIGHT_DECAY, "epochs_max": EPOCHS,
                "patience": PATIENCE, "aggr": "max", "layers": 2,
                "imbalance_handling": "inverse-frequency weighted NLL loss",
                "pos_weight": float(class_weights[1]),
                "split": "ring-component holdout 60/20/20",
                "n_ring_components": n_components,
                "sharing_cap": meta["sharing_cap"],
                "n_nodes": meta["n_nodes"], "n_edges": meta["n_edges_directed"],
                "seed": SEED,
            }
        )

        best_val, best_state, best_epoch, since_best = -1.0, None, 0, 0
        for epoch in range(1, EPOCHS + 1):
            model.train()
            optimizer.zero_grad()
            out = model(x, edge_index)
            loss = F.nll_loss(out[train_idx], y[train_idx], weight=class_weights)
            loss.backward()
            optimizer.step()

            if epoch % 5 == 0 or epoch == 1:
                val = eval_split(model, x, edge_index, y_np, val_mask)
                mlflow.log_metrics(
                    {"train_loss": float(loss), "val_f1": val["f1"], "val_pr_auc": val["pr_auc"]},
                    step=epoch,
                )
                print(
                    f"[TRAIN] epoch {epoch:3d} loss={float(loss):.4f} "
                    f"val_f1={val['f1']:.3f} val_pr_auc={val['pr_auc']:.3f} val_recall={val['recall']:.3f}"
                )
                score = val["pr_auc"]
                if score > best_val:
                    best_val, best_epoch, since_best = score, epoch, 0
                    best_state = {k: v.clone() for k, v in model.state_dict().items()}
                else:
                    since_best += 5
                    if since_best >= PATIENCE:
                        print(f"[TRAIN] early stop at epoch {epoch} (best val_pr_auc @ {best_epoch})")
                        break

        model.load_state_dict(best_state)
        test = eval_split(model, x, edge_index, y_np, test_mask)
        val_final = eval_split(model, x, edge_index, y_np, val_mask)
        mlflow.log_metrics({f"test_{k}": v for k, v in test.items() if isinstance(v, float)})
        mlflow.log_metric("best_epoch", best_epoch)

        print("\n=== TEST METRICS (ring-component holdout) ===")
        print(json.dumps(test, indent=2))
        print(f"val (for reference): {json.dumps(val_final)}")

        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        torch.save(model.state_dict(), ARTIFACT_DIR / "model.pt")
        (ARTIFACT_DIR / "model_config.json").write_text(
            json.dumps(
                {
                    "architecture": "InstitutionalFraudSAGE",
                    "in_channels": int(x.shape[1]),
                    "hidden_channels": HIDDEN,
                    "out_channels": 2,
                    "aggr": "max",
                    "dropout": DROPOUT,
                    "feature_names": meta["feature_names"],
                    "best_epoch": best_epoch,
                    "test_metrics": test,
                },
                indent=2,
            )
        )
        (ARTIFACT_DIR / "feature_stats.json").write_text(
            json.dumps({"mean": mu.tolist(), "std": sigma.tolist(), "feature_names": meta["feature_names"]}, indent=2)
        )
        # Logged as plain artifacts rather than mlflow.pytorch.log_model:
        # MLflow 3.14's pytorch flavor defaults to 'pt2' traced-graph
        # serialization, which requires tracing forward() on an example
        # input -- brittle for dynamic-graph GNN forward signatures. The
        # state_dict + config pair is the artifact Chunk 7 loads anyway.
        for f in ("model.pt", "model_config.json", "feature_stats.json"):
            mlflow.log_artifact(str(ARTIFACT_DIR / f), artifact_path="model")
        print(f"[TRAIN] artifacts -> {ARTIFACT_DIR} (also logged to MLflow run)")


if __name__ == "__main__":
    main()
