"""
Chunk 6: per-account node features + homogeneous account graph construction.

Features are exactly the four named in docs/specs/POC_Blueprint.md section 3
-- [Tx Count, Unique Counterparties, Value Variance, Device Association
Count] -- computed as REAL per-account aggregates over the full corpus in
data/simulated/ (~590K FUNDS_TRANSFER edges), not the POC's 5-node mock.

Leakage guard: all four features are purely behavioral (transaction/device
aggregates). Notably, `risk_base` from vertices_account.parquet is NOT used
as a feature -- graph_schema.py derives it partly from the isFraud label,
so including it would leak target signal. `is_ring_member` and ring_* are
labels/split-keys only.

Graph construction: the GNN operates on a homogeneous Account-only graph
(matching the POC's plain SAGEConv architecture). Account-Account message-
passing edges come from three sources:
  1. FUNDS_TRANSFER edges (direct transfers), both directions.
  2. Shared-Device: accounts sharing a Device vertex, pairwise -- but only
     for devices shared by <= SHARING_CAP accounts. IEEE-CIS DeviceInfo
     strings are coarse (e.g. "Windows" -> one device_hash shared by
     thousands of accounts); pairwise-connecting those would create
     millions of meaningless edges. Small sharing cliques are the actual
     synthetic-identity signal; a device shared by 5,000 accounts is a
     generic OS string, not a hardware fingerprint.
  3. Shared-IPAddress: same construction and cap as devices.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path

import os
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
SIM_DIR = REPO_ROOT / "data" / "simulated"

# Max accounts sharing a device/IP before it's treated as a generic
# identifier (e.g. an OS string) and no pairwise edges are added. Issue #5:
# this was a hard-coded magic number; it's now overridable via env so its
# effect on PR-AUC / FP-rate can actually be swept, rather than being an
# untunable constant. 10 remains the default the committed model was trained
# with -- changing it means retraining for consistent train/serve features.
SHARING_CAP = int(os.environ.get("ARGUS_SHARING_CAP", "10"))


def build_features_and_graph():
    accounts = pd.read_parquet(SIM_DIR / "vertices_account.parquet")
    ft = pd.read_parquet(SIM_DIR / "edges_funds_transfer.parquet")
    ud = pd.read_parquet(SIM_DIR / "edges_used_device.parquet")
    af = pd.read_parquet(SIM_DIR / "edges_accessed_from.parquet")

    acct_ids = accounts["acct_id"].to_numpy()
    idx = {a: i for i, a in enumerate(acct_ids)}
    n = len(acct_ids)

    # --- features -----------------------------------------------------------
    out_grp = ft.groupby("src_acct_id")["amount"]
    in_grp = ft.groupby("dst_acct_id")["amount"]

    tx_count = pd.Series(0.0, index=acct_ids)
    tx_count = tx_count.add(out_grp.count(), fill_value=0).add(in_grp.count(), fill_value=0)

    out_cp = ft.groupby("src_acct_id")["dst_acct_id"].agg(set)
    in_cp = ft.groupby("dst_acct_id")["src_acct_id"].agg(set)
    cp = pd.Series([set() for _ in range(n)], index=acct_ids)
    for s in (out_cp, in_cp):
        for a, v in s.items():
            cp.loc[a] |= v
    unique_counterparties = cp.apply(len).astype(float)

    amounts = pd.concat(
        [
            ft[["src_acct_id", "amount"]].rename(columns={"src_acct_id": "acct"}),
            ft[["dst_acct_id", "amount"]].rename(columns={"dst_acct_id": "acct"}),
        ]
    )
    value_variance = amounts.groupby("acct")["amount"].var().reindex(acct_ids).fillna(0.0)

    device_assoc = (
        ud.groupby("src_acct_id")["dst_device_hash"].nunique().reindex(acct_ids).fillna(0).astype(float)
    )

    x = np.stack(
        [
            tx_count.reindex(acct_ids).fillna(0).to_numpy(),
            unique_counterparties.reindex(acct_ids).fillna(0).to_numpy(),
            value_variance.to_numpy(),
            device_assoc.to_numpy(),
        ],
        axis=1,
    ).astype(np.float32)
    feature_names = ["tx_count", "unique_counterparties", "value_variance", "device_assoc_count"]

    # --- message-passing edges ----------------------------------------------
    edge_set: set[tuple[int, int]] = set()

    for s, d in zip(ft["src_acct_id"], ft["dst_acct_id"]):
        si, di = idx[s], idx[d]
        if si != di:
            edge_set.add((si, di))
            edge_set.add((di, si))

    def add_shared(df: pd.DataFrame, key_col: str) -> int:
        added = 0
        for _, grp in df.groupby(key_col)["src_acct_id"]:
            members = grp.unique()
            if 2 <= len(members) <= SHARING_CAP:
                for a, b in combinations(members, 2):
                    ai, bi = idx[a], idx[b]
                    edge_set.add((ai, bi))
                    edge_set.add((bi, ai))
                    added += 2
        return added

    n_dev = add_shared(ud, "dst_device_hash")
    n_ip = add_shared(af, "dst_ip_string")

    edge_index = np.array(sorted(edge_set), dtype=np.int64).T

    y = accounts["is_ring_member"].to_numpy().astype(np.int64)
    ring_id = accounts["ring_id"].to_numpy(dtype=object)

    meta = {
        "n_nodes": n,
        "n_edges_directed": int(edge_index.shape[1]),
        "shared_device_edges_added": n_dev,
        "shared_ip_edges_added": n_ip,
        "positives": int(y.sum()),
        "sharing_cap": SHARING_CAP,
        "feature_names": feature_names,
    }
    return acct_ids, x, edge_index, y, ring_id, meta
