"""
Chunk 2: exports Chunk 1's edges_funds_transfer table to JSONL for the Rust
ingestion engine to consume directly. Rust doesn't get a parquet dependency
this early -- Chunk 2 is scoped to the ingestion engine, not parquet parsing
in Rust, so a flat JSONL export is the loader boundary between the data
layer and the Rust crate (see context.md Architectural Decisions Log).

Chunk 1's simulated data is post-hoc graph structure (entity-resolved
accounts/devices/IPs), not a literal raw event log. This script reconstructs
a plausible "raw transaction event" shape matching POC_Blueprint.md section
2's RawTransaction struct (transaction_id, source_account, target_account,
amount, asset_type, device_id, ip_address) by joining each transfer with a
device/IP seen for the same source account. Circular and smurfing ring
accounts have no USED_DEVICE/ACCESSED_FROM edges of their own (only
device_cluster rings do, by design -- see ring_injector.py), so a
deterministic per-account fallback is synthesized for those rather than
going back to backfill Chunk 1, which is out of this chunk's scope.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
SIM_DIR = REPO_ROOT / "data" / "simulated"
OUT_PATH = SIM_DIR / "funds_transfer_raw.jsonl"


def _fallback_device(acct_id: str) -> str:
    return f"DEV-FALLBACK-{hashlib.md5(acct_id.encode()).hexdigest()[:10]}"


def _fallback_ip(acct_id: str) -> str:
    n = int(hashlib.md5(acct_id.encode()).hexdigest()[:4], 16) % 65536
    return f"10.{(n >> 8) & 0xFF}.{n & 0xFF}.1"


def main() -> None:
    ft = pd.read_parquet(SIM_DIR / "edges_funds_transfer.parquet")
    ud = pd.read_parquet(SIM_DIR / "edges_used_device.parquet")
    dev = pd.read_parquet(SIM_DIR / "vertices_device.parquet")
    af = pd.read_parquet(SIM_DIR / "edges_accessed_from.parquet")

    device_by_acct = (
        ud.merge(dev, left_on="dst_device_hash", right_on="device_hash", how="left")
        .drop_duplicates(subset="src_acct_id", keep="first")
        .set_index("src_acct_id")["hardware_signature"]
    )
    ip_by_acct = (
        af.drop_duplicates(subset="src_acct_id", keep="first").set_index("src_acct_id")["dst_ip_string"]
    )

    ft = ft.copy()
    ft["device_id"] = ft["src_acct_id"].map(device_by_acct)
    ft["ip_address"] = ft["src_acct_id"].map(ip_by_acct)

    missing_device = ft["device_id"].isna()
    missing_ip = ft["ip_address"].isna()
    ft.loc[missing_device, "device_id"] = ft.loc[missing_device, "src_acct_id"].map(_fallback_device)
    ft.loc[missing_ip, "ip_address"] = ft.loc[missing_ip, "src_acct_id"].map(_fallback_ip)

    ft = ft.sort_values("timestamp")

    SIM_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for row in ft.itertuples(index=False):
            record = {
                "transaction_id": row.tx_id,
                "source_account": row.src_acct_id,
                "target_account": row.dst_acct_id,
                "amount": float(row.amount),
                "asset_type": "USD",
                "device_id": row.device_id,
                "ip_address": row.ip_address,
            }
            f.write(json.dumps(record) + "\n")

    print(
        f"[EXPORT] {len(ft):,} rows -> {OUT_PATH} "
        f"(fallback device: {int(missing_device.sum()):,}, fallback ip: {int(missing_ip.sum()):,})"
    )


if __name__ == "__main__":
    main()
