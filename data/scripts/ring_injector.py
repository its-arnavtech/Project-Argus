"""
Chunk 1: synthetic mule-ring injection generator.

No public dataset labels multi-hop fraud syndicates -- IEEE-CIS labels
individual-transaction card fraud, not the ring/syndicate structures Argus's
GNN needs to learn. This script is the ground-truth source: it builds
realistic ring topologies on top of the real (or bundled-sample) account
universe derived by graph_schema.derive_account_universe(), and labels every
injected vertex/edge so Chunk 6's GNN training has something to supervise on.

Three ring archetypes, matching the fraud patterns called out in
docs/specs/POC_Blueprint.md section 1 and PDD_Production_Guide.md's Tableau
KPIs (Multi-Hop Risk Dispersion Factor, Device Sharing Density Ratio):

  1. circular    -- a closed FUNDS_TRANSFER chain (A->B->C->...->A) with one
                    entry edge from a real background account and one exit
                    edge back out, in tight time succession.
  2. smurfing     -- many "smurf" accounts fan small transfers (<$3,000, under
                    a typical CTR-style reporting threshold) into one
                    collector in a short window, which then fans out to 1-2
                    exit accounts -- classic layering.
  3. device_cluster -- several accounts (some reused from other rings, to
                    model syndicates that share infrastructure across
                    operations) share a single Device and/or IPAddress
                    vertex. No forced FUNDS_TRANSFER among them: the shared
                    Device/IP vertex itself is the multi-hop signal.

Every injected vertex/edge is stamped provenance="synthetic_ring" plus
ring_id/ring_type, exactly the same label columns the background rows carry
(see graph_schema.py's module docstring for why these labels exist and how
they're flagged as loader/training metadata rather than PDD-documented
Cosmos properties). Output lands in data/simulated/ as one Parquet file per
vertex/edge label -- directly loadable by Chunk 5's Cosmos loader.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

from graph_schema import (
    ACCOUNT_PROPS,
    CUSTOMER_PROPS,
    DEVICE_PROPS,
    IPADDRESS_PROPS,
    RING_LABEL_COLS_EDGE,
    RING_LABEL_COLS_VERTEX,
    GraphTables,
    _md5,
    _sha256,
    derive_account_universe,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
SIM_DIR = REPO_ROOT / "data" / "simulated"

SEED = 42
N_CIRCULAR = 20
N_SMURFING = 15
N_DEVICE_CLUSTERS = 10
DEVICE_CLUSTER_REUSE_PROB = 0.4


def _new_account_customer_rows(ids: list[str], ring_id: str, ring_type: str, rng: np.random.Generator):
    n = len(ids)
    cust_ids = [f"CUST-R-{ring_id}-{i}" for i in range(n)]
    accounts = pd.DataFrame(
        {
            "acct_id": ids,
            "risk_base": np.clip(rng.normal(0.75, 0.12, n), 0.0, 1.0),
            "balance": np.round(rng.lognormal(mean=7.5, sigma=0.8, size=n), 2),
            "open_date": rng.integers(1_650_000_000, 1_735_000_000, size=n),
            "cust_id": cust_ids,
            "provenance": "synthetic_ring",
            "is_ring_member": True,
            "ring_id": ring_id,
            "ring_type": ring_type,
        }
    )
    customers = pd.DataFrame(
        {
            "cust_id": cust_ids,
            "tax_hash": [_sha256(f"ring-{cid}") for cid in cust_ids],
            "KYC_status": rng.choice(["VERIFIED", "PENDING"], size=n, p=[0.6, 0.4]),
            "segment": "RETAIL",
            "provenance": "synthetic_ring",
        }
    )
    return accounts, customers


def inject_rings(tables: GraphTables, seed: int = SEED):
    rng = np.random.default_rng(seed)
    t_min, t_max = tables.funds_transfer["timestamp"].min(), tables.funds_transfer["timestamp"].max()
    background_accts = tables.accounts["acct_id"].to_numpy()

    new_accounts, new_customers, new_devices, new_ips = [], [], [], []
    new_ft, new_af, new_ud = [], [], []
    manifest_rows = []
    acct_counter = 0
    ring_account_pool: list[str] = []

    def next_ids(n):
        nonlocal acct_counter
        ids = [f"ACC-R{acct_counter + i:05d}" for i in range(n)]
        acct_counter += n
        return ids

    # 1. Circular transfer chains
    for r in range(N_CIRCULAR):
        ring_id = f"CIRC-{r:03d}"
        size = rng.integers(4, 9)
        ids = next_ids(size)
        accounts, customers = _new_account_customer_rows(ids, ring_id, "circular", rng)
        new_accounts.append(accounts)
        new_customers.append(customers)
        ring_account_pool.extend(ids)

        start_t = rng.integers(t_min, max(t_min + 1, t_max - 3600))
        amount = float(np.round(rng.uniform(2_000, 20_000), 2))
        edges = []
        for i in range(size):
            src, dst = ids[i], ids[(i + 1) % size]
            edges.append(
                {
                    "src_acct_id": src,
                    "dst_acct_id": dst,
                    "tx_id": str(uuid.uuid4()),
                    "amount": round(amount * rng.uniform(0.95, 1.05), 2),
                    "timestamp": int(start_t + i * rng.integers(30, 300)),
                    "trace_id": str(uuid.uuid4()),
                    "provenance": "synthetic_ring",
                    "is_synthetic": True,
                    "ring_id": ring_id,
                    "ring_type": "circular",
                }
            )
        entry_src = rng.choice(background_accts)
        edges.append(
            {
                "src_acct_id": entry_src,
                "dst_acct_id": ids[0],
                "tx_id": str(uuid.uuid4()),
                "amount": round(amount * rng.uniform(0.95, 1.05), 2),
                "timestamp": int(start_t - rng.integers(60, 600)),
                "trace_id": str(uuid.uuid4()),
                "provenance": "synthetic_ring",
                "is_synthetic": True,
                "ring_id": ring_id,
                "ring_type": "circular",
            }
        )
        exit_dst = rng.choice(background_accts)
        edges.append(
            {
                "src_acct_id": ids[-1],
                "dst_acct_id": exit_dst,
                "tx_id": str(uuid.uuid4()),
                "amount": round(amount * rng.uniform(0.9, 1.0), 2),
                "timestamp": int(start_t + size * 300 + rng.integers(60, 600)),
                "trace_id": str(uuid.uuid4()),
                "provenance": "synthetic_ring",
                "is_synthetic": True,
                "ring_id": ring_id,
                "ring_type": "circular",
            }
        )
        new_ft.append(pd.DataFrame(edges))
        manifest_rows.append({"ring_id": ring_id, "ring_type": "circular", "size": size, "members": ",".join(ids)})

    # 2. Smurfing fan-in / fan-out
    for r in range(N_SMURFING):
        ring_id = f"SMURF-{r:03d}"
        n_smurfs = int(rng.integers(5, 13))
        n_exits = int(rng.integers(1, 3))
        collector_id = next_ids(1)[0]
        smurf_ids = next_ids(n_smurfs)
        exit_ids = next_ids(n_exits)
        all_ids = [collector_id] + smurf_ids + exit_ids
        accounts, customers = _new_account_customer_rows(all_ids, ring_id, "smurfing", rng)
        new_accounts.append(accounts)
        new_customers.append(customers)
        ring_account_pool.extend(all_ids)

        start_t = rng.integers(t_min, max(t_min + 1, t_max - 3600))
        edges = []
        total_in = 0.0
        for s in smurf_ids:
            amt = round(float(rng.uniform(500, 2_950)), 2)  # under a $3K reporting-style threshold
            total_in += amt
            edges.append(
                {
                    "src_acct_id": s,
                    "dst_acct_id": collector_id,
                    "tx_id": str(uuid.uuid4()),
                    "amount": amt,
                    "timestamp": int(start_t + rng.integers(0, 1800)),
                    "trace_id": str(uuid.uuid4()),
                    "provenance": "synthetic_ring",
                    "is_synthetic": True,
                    "ring_id": ring_id,
                    "ring_type": "smurfing",
                }
            )
        remaining = total_in
        fanout_t = start_t + 1800
        for i, e in enumerate(exit_ids):
            share = remaining if i == len(exit_ids) - 1 else remaining * rng.uniform(0.4, 0.6)
            remaining -= share
            edges.append(
                {
                    "src_acct_id": collector_id,
                    "dst_acct_id": e,
                    "tx_id": str(uuid.uuid4()),
                    "amount": round(share, 2),
                    "timestamp": int(fanout_t + rng.integers(60, 900)),
                    "trace_id": str(uuid.uuid4()),
                    "provenance": "synthetic_ring",
                    "is_synthetic": True,
                    "ring_id": ring_id,
                    "ring_type": "smurfing",
                }
            )
        new_ft.append(pd.DataFrame(edges))
        manifest_rows.append(
            {"ring_id": ring_id, "ring_type": "smurfing", "size": len(all_ids), "members": ",".join(all_ids)}
        )

    # 3. Shared-device clusters -- synthetic identity ring signature
    for r in range(N_DEVICE_CLUSTERS):
        ring_id = f"DEVCLUST-{r:03d}"
        size = int(rng.integers(3, 7))
        reuse = rng.random() < DEVICE_CLUSTER_REUSE_PROB and len(ring_account_pool) >= size
        if reuse:
            ids = list(rng.choice(ring_account_pool, size=size, replace=False))
        else:
            ids = next_ids(size)
            accounts, customers = _new_account_customer_rows(ids, ring_id, "device_cluster", rng)
            new_accounts.append(accounts)
            new_customers.append(customers)
            ring_account_pool.extend(ids)

        device_hash = f"DEV-R{_md5(ring_id)[:10]}"
        new_devices.append(
            pd.DataFrame(
                {
                    "device_hash": [device_hash],
                    "os_type": [rng.choice(["desktop", "mobile"])],
                    "hardware_signature": [_md5(f"hw-ring-{ring_id}")[:16]],
                    "provenance": ["synthetic_ring"],
                }
            )
        )
        ip_string = f"{rng.integers(1,224)}.{rng.integers(0,255)}.{rng.integers(0,255)}.{rng.integers(0,255)}"
        new_ips.append(
            pd.DataFrame(
                {
                    "ip_string": [ip_string],
                    "geo_country": [rng.choice(["US", "RO", "VN", "NG"])],
                    "proxy_flag": [True],
                    "provenance": ["synthetic_ring"],
                }
            )
        )

        start_t = rng.integers(t_min, max(t_min + 1, t_max - 3600))
        ud_rows, af_rows = [], []
        for acct in ids:
            ud_rows.append(
                {
                    "src_acct_id": acct,
                    "dst_device_hash": device_hash,
                    "application_version": rng.choice(["3.1.0", "4.0.1"]),
                    "binding_flag": True,
                    "provenance": "synthetic_ring",
                    "is_synthetic": True,
                    "ring_id": ring_id,
                    "ring_type": "device_cluster",
                }
            )
            af_rows.append(
                {
                    "src_acct_id": acct,
                    "dst_ip_string": ip_string,
                    "session_id": str(uuid.uuid4()),
                    "login_timestamp": int(start_t + rng.integers(0, 3600)),
                    "provenance": "synthetic_ring",
                    "is_synthetic": True,
                    "ring_id": ring_id,
                    "ring_type": "device_cluster",
                }
            )
        new_ud.append(pd.DataFrame(ud_rows))
        new_af.append(pd.DataFrame(af_rows))
        manifest_rows.append(
            {
                "ring_id": ring_id,
                "ring_type": "device_cluster",
                "size": size,
                "members": ",".join(ids),
                "reused_existing_members": reuse,
            }
        )

    accounts_out = pd.concat([tables.accounts] + new_accounts, ignore_index=True)
    customers_out = pd.concat([tables.customers] + new_customers, ignore_index=True)
    devices_out = pd.concat([tables.devices] + new_devices, ignore_index=True) if new_devices else tables.devices
    ips_out = pd.concat([tables.ip_addresses] + new_ips, ignore_index=True) if new_ips else tables.ip_addresses
    ft_out = pd.concat([tables.funds_transfer] + new_ft, ignore_index=True)
    af_out = pd.concat([tables.accessed_from] + new_af, ignore_index=True) if new_af else tables.accessed_from
    ud_out = pd.concat([tables.used_device] + new_ud, ignore_index=True) if new_ud else tables.used_device

    manifest = pd.DataFrame(manifest_rows)
    return (
        GraphTables(
            accounts=accounts_out,
            customers=customers_out,
            devices=devices_out,
            ip_addresses=ips_out,
            merchants=tables.merchants,
            funds_transfer=ft_out,
            accessed_from=af_out,
            used_device=ud_out,
            settled_at=tables.settled_at,
        ),
        manifest,
    )


def main() -> None:
    metadata_path = RAW_DIR / "acquisition_metadata.json"
    if not metadata_path.exists():
        raise SystemExit("data/raw/acquisition_metadata.json not found -- run acquire_ieee_cis.py first.")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    provenance = metadata["data_source"]

    tx_df = pd.read_csv(RAW_DIR / "train_transaction.csv")
    identity_path = RAW_DIR / "train_identity.csv"
    identity_df = pd.read_csv(identity_path) if identity_path.exists() else None

    background = derive_account_universe(tx_df, identity_df, provenance=provenance, seed=SEED)
    full_tables, manifest = inject_rings(background, seed=SEED)

    SIM_DIR.mkdir(parents=True, exist_ok=True)
    full_tables.accounts.to_parquet(SIM_DIR / "vertices_account.parquet", index=False)
    full_tables.customers.to_parquet(SIM_DIR / "vertices_customer.parquet", index=False)
    full_tables.devices.to_parquet(SIM_DIR / "vertices_device.parquet", index=False)
    full_tables.ip_addresses.to_parquet(SIM_DIR / "vertices_ipaddress.parquet", index=False)
    full_tables.merchants.to_parquet(SIM_DIR / "vertices_merchant.parquet", index=False)
    full_tables.funds_transfer.to_parquet(SIM_DIR / "edges_funds_transfer.parquet", index=False)
    full_tables.accessed_from.to_parquet(SIM_DIR / "edges_accessed_from.parquet", index=False)
    full_tables.used_device.to_parquet(SIM_DIR / "edges_used_device.parquet", index=False)
    full_tables.settled_at.to_parquet(SIM_DIR / "edges_settled_at.parquet", index=False)
    manifest.to_parquet(SIM_DIR / "rings_manifest.parquet", index=False)

    print(
        f"[RING_INJECTOR] accounts={len(full_tables.accounts)} "
        f"(ring_members={int(full_tables.accounts['is_ring_member'].sum())}) "
        f"rings={len(manifest)} -> {SIM_DIR}"
    )


if __name__ == "__main__":
    main()
