"""
Chunk 9: Tableau extract export job.

DELIBERATE SCOPE-DOWN (logged in context.md): Tableau cannot natively query
a live Gremlin graph, and the real enterprise path -- Cosmos DB Analytical
Store + Synapse Link, with Tableau reading the auto-synced columnar store
through the Synapse SQL endpoint on the PDD's "custom extract refreshes
scheduled every 15 minutes" -- is not budget-justified for this build
(Synapse workspace + analytical store storage + SQL pool costs, for a
portfolio demo). This job stands in for that path: it runs the necessary
Gremlin traversals + transaction-level queries and flattens everything into
one extract file that IS the Tableau data source. Re-running the job is the
"refresh". The enterprise migration is: enable analytical storage on the
container (one Terraform property: `analytical_storage_ttl`), provision
Synapse Link, and point Tableau's Azure Synapse connector at it -- the
dashboard workbook wouldn't change, only its data source connection.

Extract grain: one row per FUNDS_TRANSFER leg (the transaction), joined
with per-account graph attributes. Columns (superset of what PDD section
3's three calculated fields need):

  tx_id, amount, timestamp          -- transaction (parquet corpus)
  acct_id                           -- source account of the transfer
  velocity_score_1m                 -- REAL per-transaction metric: count of
                                       this account's transfers in the 60s
                                       window ending at this transfer
  device_hash                       -- account's primary device (USED_DEVICE)
  proxy_flag                        -- true if any IP the account accessed
                                       has proxy_flag=true (ACCESSED_FROM ->
                                       IPAddress)
  gnn_risk_score                    -- from Cosmos (Chunk 7 write-back);
                                       null for unscored accounts
  hop_distance                      -- min hops (undirected FUNDS_TRANSFER)
                                       from the nearest flagged account
                                       (gnn_risk_score > 0.5); 0 for flagged
                                       accounts themselves, null if
                                       unreachable within HOP_CAP
  is_ring_member, ring_type         -- ground-truth labels for demo overlays
  sar_grounded                      -- whether a grounded SAR exists (Chunk 8)

Output: dashboards/extracts/argus_tableau_extract.csv (the .twb's data
source) + .parquet twin. Both gitignored -- regenerate with this script.
"""
from __future__ import annotations

import sys
import time
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SIM_DIR = REPO_ROOT / "data" / "simulated"
OUT_DIR = Path(__file__).resolve().parent / "extracts"

sys.path.insert(0, str(REPO_ROOT / "agents"))
from compliance_graph import PARTITION_KEY_VALUE, make_gremlin_client  # noqa: E402

FLAG_THRESHOLD = 0.5
HOP_CAP = 10


def fetch_scored_accounts() -> pd.DataFrame:
    """Gremlin query: every Account carrying a gnn_risk_score (Chunk 7's
    write-back), plus its SAR-grounded flag where Chunk 8 stored one."""
    c = make_gremlin_client()
    try:
        rows = c.submit(
            message=(
                "g.V().hasLabel('Account').has('partitionKey', pk).has('gnn_risk_score')"
                ".project('acct_id','gnn_risk_score','sar_grounded')"
                ".by(values('acct_id'))"
                ".by(values('gnn_risk_score'))"
                ".by(coalesce(values('sar_grounded'), constant(false)))"
            ),
            bindings={"pk": PARTITION_KEY_VALUE},
        ).all().result()
    finally:
        c.close()
    df = pd.DataFrame(rows)
    print(f"[EXTRACT] Cosmos: {len(df)} scored accounts "
          f"({int((df['gnn_risk_score'] > FLAG_THRESHOLD).sum())} above {FLAG_THRESHOLD}, "
          f"{int(df['sar_grounded'].sum())} with grounded SARs)")
    return df


def velocity_1m(ft: pd.DataFrame) -> np.ndarray:
    """Per-transfer 60s trailing count for the source account (two-pointer
    per account over sorted timestamps)."""
    out = np.zeros(len(ft), dtype=np.int32)
    order = ft.sort_values(["src_acct_id", "timestamp"]).index.to_numpy()
    src = ft["src_acct_id"].to_numpy()
    ts = ft["timestamp"].to_numpy()
    j = 0
    for k, idx in enumerate(order):
        if k == 0 or src[order[k]] != src[order[j]]:
            j = k
        while ts[order[k]] - ts[order[j]] > 60:
            j += 1
        out[idx] = k - j + 1
    return out


def hop_distances(ft: pd.DataFrame, flagged: set[str]) -> dict[str, int]:
    """Multi-source BFS over the undirected FUNDS_TRANSFER graph from all
    flagged accounts."""
    adj: dict[str, list[str]] = {}
    for s, d in zip(ft["src_acct_id"], ft["dst_acct_id"]):
        adj.setdefault(s, []).append(d)
        adj.setdefault(d, []).append(s)
    dist: dict[str, int] = {a: 0 for a in flagged if a in adj}
    q = deque(dist)
    while q:
        node = q.popleft()
        if dist[node] >= HOP_CAP:
            continue
        for nb in adj.get(node, []):
            if nb not in dist:
                dist[nb] = dist[node] + 1
                q.append(nb)
    return dist


def main() -> None:
    t0 = time.time()
    ft = pd.read_parquet(SIM_DIR / "edges_funds_transfer.parquet",
                         columns=["src_acct_id", "dst_acct_id", "tx_id", "amount", "timestamp"])
    accounts = pd.read_parquet(SIM_DIR / "vertices_account.parquet",
                               columns=["acct_id", "is_ring_member", "ring_type"])
    ud = pd.read_parquet(SIM_DIR / "edges_used_device.parquet",
                         columns=["src_acct_id", "dst_device_hash"])
    af = pd.read_parquet(SIM_DIR / "edges_accessed_from.parquet",
                         columns=["src_acct_id", "dst_ip_string"])
    ips = pd.read_parquet(SIM_DIR / "vertices_ipaddress.parquet",
                          columns=["ip_string", "proxy_flag"])

    scored = fetch_scored_accounts()
    flagged = set(scored.loc[scored["gnn_risk_score"] > FLAG_THRESHOLD, "acct_id"])

    print("[EXTRACT] computing velocity_score_1m over "
          f"{len(ft):,} transfers...")
    ft = ft.reset_index(drop=True)
    ft["velocity_score_1m"] = velocity_1m(ft)

    print(f"[EXTRACT] multi-source BFS hop_distance from {len(flagged)} flagged accounts...")
    dist = hop_distances(ft, flagged)

    device_by_acct = ud.drop_duplicates("src_acct_id").set_index("src_acct_id")["dst_device_hash"]
    proxy_ips = set(ips.loc[ips["proxy_flag"], "ip_string"])
    proxy_by_acct = (
        af.assign(is_proxy=af["dst_ip_string"].isin(proxy_ips))
        .groupby("src_acct_id")["is_proxy"].any()
    )

    extract = ft[["tx_id", "amount", "timestamp", "src_acct_id"]].rename(
        columns={"src_acct_id": "acct_id"}
    )
    extract["velocity_score_1m"] = ft["velocity_score_1m"]
    extract["device_hash"] = extract["acct_id"].map(device_by_acct)
    extract["proxy_flag"] = extract["acct_id"].map(proxy_by_acct).fillna(False)
    extract = extract.merge(scored, on="acct_id", how="left")
    extract["hop_distance"] = extract["acct_id"].map(dist)
    extract = extract.merge(accounts, on="acct_id", how="left")
    extract["sar_grounded"] = extract["sar_grounded"].fillna(False).astype(bool)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pq = OUT_DIR / "argus_tableau_extract.parquet"
    csv = OUT_DIR / "argus_tableau_extract.csv"
    extract.to_parquet(pq, index=False)
    # lowercase true/false so Tableau's textscan infers booleans and the
    # PDD-verbatim formula `IF [proxy_flag] THEN 1.5 ELSE 1.0 END` works.
    csv_df = extract.copy()
    for col in ("proxy_flag", "is_ring_member", "sar_grounded"):
        csv_df[col] = csv_df[col].map(lambda v: "true" if bool(v) else "false")
    csv_df.to_csv(csv, index=False)

    print(f"[EXTRACT] {len(extract):,} rows x {len(extract.columns)} cols -> {csv.name} + {pq.name} "
          f"in {time.time()-t0:.0f}s")
    print(f"[EXTRACT] columns: {list(extract.columns)}")
    print(extract[["tx_id", "acct_id", "amount", "velocity_score_1m", "proxy_flag",
                   "gnn_risk_score", "hop_distance"]].head(3).to_string(index=False))


if __name__ == "__main__":
    main()
