"""
Batch account scorer — a direct, one-shot alternative to the streaming
inference service (inference_service.py).

WHEN YOU'D USE THIS
-------------------
The normal scoring path is event-driven: transactions flow through Event
Hubs and inference_service.py scores the accounts each event touches. That
means an account only gets a gnn_risk_score once a transaction involving it
happens to be consumed. This script scores accounts *directly* instead —
one full-graph forward pass, then write the resulting scores straight to
Cosmos — with no Event Hubs round-trip.

That's useful for:
  - Bootstrapping / restoring a coherent demo state after a graph reload
    (e.g. the Chunk-12 enterprise benchmark dropped and reloaded the graph,
    leaving it unscored; this is what re-scored the ring members so the
    LangGraph orchestrator had flagged accounts to write SARs for).
  - Re-scoring the whole graph offline without replaying the corpus.

It deliberately reuses graph/loader.py's SYNCHRONOUS Gremlin client for the
writes rather than inference_service.py's concurrent async client: the async
`submit_async` path is unreliable on Windows (proactor-loop teardown errors),
and for a bounded one-shot write of a few hundred/thousand vertices the
synchronous client is simpler and rock-solid. The GNN forward pass is
identical to what the streaming service runs (same model artifacts, same
feature construction), so the scores are the same — this only changes how
they're delivered to Cosmos.

USAGE
-----
    python ml/inference/batch_score_accounts.py --ring-members
    python ml/inference/batch_score_accounts.py --all
    python ml/inference/batch_score_accounts.py --min-score 0.5

Run with the repo venv: .venv/Scripts/python.exe (torch lives there).
Auth is Entra-token via DefaultAzureCredential + the Gremlin RBAC grant —
no account key, same as the loader and inference service.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "ml"))
sys.path.insert(0, str(REPO_ROOT / "ml" / "training"))
sys.path.insert(0, str(REPO_ROOT / "graph"))

from model_def import InstitutionalFraudSAGE  # noqa: E402
from features import build_features_and_graph  # noqa: E402
import loader  # noqa: E402  (graph/loader.py — reused sync Gremlin client)

ARTIFACT_DIR = REPO_ROOT / "ml" / "artifacts"
PARTITION_KEY_VALUE = "argus"


def score_all_accounts() -> tuple[list[str], np.ndarray, np.ndarray]:
    """One full-graph forward pass. Returns (acct_ids, prob1, y) where
    prob1[i] is P(fraud) for account i and y is the ground-truth label."""
    acct_ids, x_np, edge_index_np, y, _ring_id, meta = build_features_and_graph()
    cfg = json.loads((ARTIFACT_DIR / "model_config.json").read_text())
    stats = json.loads((ARTIFACT_DIR / "feature_stats.json").read_text())
    mu = np.array(stats["mean"], dtype=np.float32)
    sigma = np.array(stats["std"], dtype=np.float32)

    model = InstitutionalFraudSAGE(cfg["in_channels"], cfg["hidden_channels"], cfg["out_channels"])
    model.load_state_dict(torch.load(ARTIFACT_DIR / "model.pt", weights_only=True))
    model.eval()

    x = (torch.tensor(x_np.astype(np.float32)) - torch.tensor(mu)) / torch.tensor(sigma)
    with torch.no_grad():
        prob1 = model(x, torch.tensor(edge_index_np)).exp()[:, 1].numpy()
    print(f"[SCORE] forward pass over {meta['n_nodes']} nodes / "
          f"{meta['n_edges_directed']} edges complete")
    return acct_ids, prob1, y


def write_scores(client, acct_ids, prob1, indices) -> int:
    """Synchronously write gnn_risk_score for the given node indices to the
    Account vertices that actually exist in Cosmos."""
    present = set(loader.submit(client, "g.V().hasLabel('Account').values('acct_id')"))
    ts = int(time.time())
    n = 0
    for i in indices:
        aid = acct_ids[i]
        if aid not in present:
            continue
        loader.submit(
            client,
            "g.V(a).has('partitionKey',pk)"
            ".property('gnn_risk_score',s).property('gnn_scored_at',ts)",
            {"a": aid, "pk": PARTITION_KEY_VALUE, "s": float(prob1[i]), "ts": ts},
        )
        n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Directly score accounts and write to Cosmos.")
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--ring-members", action="store_true",
                     help="score only the ground-truth ring accounts (y==1)")
    grp.add_argument("--all", action="store_true", help="score every account")
    grp.add_argument("--min-score", type=float, metavar="T",
                     help="score every account whose model P(fraud) exceeds T")
    args = ap.parse_args()

    acct_ids, prob1, y = score_all_accounts()

    if args.ring_members:
        indices = [i for i in range(len(acct_ids)) if int(y[i]) == 1]
        label = f"{len(indices)} ring members"
    elif args.all:
        indices = list(range(len(acct_ids)))
        label = "all accounts"
    else:
        indices = [i for i in range(len(acct_ids)) if float(prob1[i]) > args.min_score]
        label = f"{len(indices)} accounts with P(fraud) > {args.min_score}"

    client = loader.make_client(loader.get_cosmos_token(), pool_size=4)
    try:
        print(f"[SCORE] writing scores for {label} ...")
        n = write_scores(client, acct_ids, prob1, indices)
        flagged = loader.submit(
            client, "g.V().hasLabel('Account').has('gnn_risk_score',gt(0.5)).count()")[0]
        print(f"[SCORE] wrote {n} scores; accounts now flagged (>0.5): {flagged}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
