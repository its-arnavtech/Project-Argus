"""
Chunk 1: data validation / EDA report.

Reads the Parquet outputs of ring_injector.py and reports real-vs-synthetic
volume, class balance, and ring topology stats (ring count, avg ring size,
avg hop distance per ring type). Writes a markdown summary to
docs/architecture/chunk1_data_eda_summary.md and prints the same to stdout.
"""
from __future__ import annotations

import json
from pathlib import Path

import networkx as nx
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
SIM_DIR = REPO_ROOT / "data" / "simulated"
OUT_PATH = REPO_ROOT / "docs" / "architecture" / "chunk1_data_eda_summary.md"


def load_tables() -> dict[str, pd.DataFrame]:
    files = {
        "accounts": "vertices_account.parquet",
        "customers": "vertices_customer.parquet",
        "devices": "vertices_device.parquet",
        "ip_addresses": "vertices_ipaddress.parquet",
        "merchants": "vertices_merchant.parquet",
        "funds_transfer": "edges_funds_transfer.parquet",
        "accessed_from": "edges_accessed_from.parquet",
        "used_device": "edges_used_device.parquet",
        "settled_at": "edges_settled_at.parquet",
        "rings_manifest": "rings_manifest.parquet",
    }
    return {name: pd.read_parquet(SIM_DIR / fname) for name, fname in files.items()}


def provenance_counts(df: pd.DataFrame) -> dict[str, int]:
    return df["provenance"].value_counts().to_dict()


def hop_distance_stats(ft: pd.DataFrame, manifest: pd.DataFrame) -> dict[str, float | None]:
    """Avg shortest-path length within each ring's induced FUNDS_TRANSFER
    subgraph. Only meaningful for ring types with internal transfer edges
    (circular, smurfing) -- device_cluster rings connect only via a shared
    Device/IPAddress vertex, not FUNDS_TRANSFER, so hop distance there is
    reported as N/A.
    """
    results: dict[str, list[float]] = {}
    for ring_id, ring_type in manifest[["ring_id", "ring_type"]].itertuples(index=False):
        if ring_type == "device_cluster":
            continue
        sub = ft[ft["ring_id"] == ring_id]
        if sub.empty:
            continue
        g = nx.DiGraph()
        g.add_edges_from(zip(sub["src_acct_id"], sub["dst_acct_id"]))
        try:
            avg_len = nx.average_shortest_path_length(g.to_undirected())
        except (nx.NetworkXError, ZeroDivisionError):
            avg_len = float("nan")
        results.setdefault(ring_type, []).append(avg_len)
    return {rt: (sum(v) / len(v) if v else None) for rt, v in results.items()}


def main() -> None:
    tables = load_tables()
    metadata = json.loads((RAW_DIR / "acquisition_metadata.json").read_text(encoding="utf-8"))

    accounts = tables["accounts"]
    manifest = tables["rings_manifest"]
    ft = tables["funds_transfer"]

    total_accounts = len(accounts)
    ring_member_accounts = int(accounts["is_ring_member"].sum())
    ring_member_pct = 100 * ring_member_accounts / total_accounts if total_accounts else 0.0

    ring_counts = manifest["ring_type"].value_counts().to_dict()
    ring_sizes = manifest.groupby("ring_type")["size"].mean().to_dict()
    hop_stats = hop_distance_stats(ft, manifest)

    vertex_edge_provenance = {
        name: provenance_counts(df)
        for name, df in tables.items()
        if name != "rings_manifest"
    }

    lines: list[str] = []
    lines.append("# Chunk 1 — Data Acquisition & Ring Injection: EDA Summary\n")
    lines.append(f"Generated from `data/simulated/` (acquisition data_source: `{metadata['data_source']}`, seed: {metadata['seed']}).\n")

    lines.append("## Real vs. Synthetic Row Counts\n")
    lines.append("| Table | " + " | ".join(sorted({k for d in vertex_edge_provenance.values() for k in d})) + " | Total |")
    all_provs = sorted({k for d in vertex_edge_provenance.values() for k in d})
    lines.append("|---" * (len(all_provs) + 2) + "|")
    for name, counts in vertex_edge_provenance.items():
        row = [name] + [str(counts.get(p, 0)) for p in all_provs] + [str(sum(counts.values()))]
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    lines.append("## Class Balance (Account vertices)\n")
    lines.append(f"- Total accounts: **{total_accounts:,}**")
    lines.append(f"- Ring-member accounts: **{ring_member_accounts:,}** ({ring_member_pct:.2f}%)")
    lines.append(f"- Non-member (background) accounts: **{total_accounts - ring_member_accounts:,}** ({100 - ring_member_pct:.2f}%)\n")

    lines.append("## Ring Topology Stats\n")
    lines.append("| Ring type | Count | Avg size (accounts) | Avg hop distance |")
    lines.append("|---|---|---|---|")
    for ring_type in sorted(ring_counts):
        count = ring_counts[ring_type]
        avg_size = ring_sizes.get(ring_type, float("nan"))
        hop = hop_stats.get(ring_type)
        hop_str = f"{hop:.2f}" if hop is not None else "N/A (device-shared only)"
        lines.append(f"| {ring_type} | {count} | {avg_size:.2f} | {hop_str} |")
    lines.append(f"\n- Total rings injected: **{len(manifest):,}**")
    lines.append(f"- Total ring-member accounts: **{ring_member_accounts:,}**\n")

    report = "\n".join(lines)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
