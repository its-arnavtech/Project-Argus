"""
Shared graph schema definitions for Project Argus's data layer.

Vertex/edge LABELS and their PROPERTY columns below are frozen to the exact
ontology in docs/specs/PDD_Production_Guide.md section 1:
  Vertices: Account, Customer, Device, IPAddress, Merchant
  Edges:    FUNDS_TRANSFER, ACCESSED_FROM, USED_DEVICE, SETTLED_AT
Plus OWNS (Customer -> Account), added on top of the PDD's literal 4 -- see
note 1 below; this is a deliberate, flagged deviation, not an oversight.

The PDD table documents each edge's properties but not its endpoint vertex
types -- a property-graph schema doesn't need that (an edge just connects
two vertex IDs), but a flat Parquet edge-list does need explicit endpoint
columns to be loadable. This module adds those as structural necessities,
not new schema properties:
  - FUNDS_TRANSFER:  Account  -> Account
  - ACCESSED_FROM:   Account  -> IPAddress
  - USED_DEVICE:     Account  -> Device
  - SETTLED_AT:      Account  -> Merchant

Two more additions beyond the documented columns, both flagged here rather
than silently introduced:
  1. RESOLVED: Account<->Customer ownership is now a real edge, OWNS
     (Customer -> Account), added alongside the 4 PDD-defined edges below.
     The PDD lists Customer as a vertex but documents no Customer<->Account
     edge; Chunk 1 originally deferred this to a `cust_id` foreign key,
     flagged for Chunk 5 to decide. Kept `cust_id` on Account too (harmless,
     useful for quick pandas joins outside Gremlin) -- OWNS is what actually
     makes Customer traversable from Account in the graph.
  2. Every vertex/edge row carries `provenance` (real_kaggle | bundled_sample
     | synthetic_ring) and ring-labeling columns (`is_ring_member`,
     `ring_id`, `ring_type` on Account; `is_synthetic`, `ring_id` on edges).
     These are ground-truth/lineage labels for GNN training and EDA, not
     part of the documented Cosmos schema -- Chunk 5 decides how much of
     this travels into Cosmos vertex/edge properties vs. staying metadata.

IEEE-CIS models individual-transaction card-not-present fraud, not P2P
account-to-account transfers, and it does not carry real IP addresses.
derive_account_universe() bridges the real transaction rows onto this
graph schema: card1+addr1 groups become Account identities, IP/Merchant
vertices are synthesized (they don't exist in the source data either
way), and each transaction becomes a FUNDS_TRANSFER edge to a synthesized
counterparty account. This bridge is a modeling choice, not part of the
original Kaggle schema.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass

import numpy as np
import pandas as pd

ACCOUNT_PROPS = ["acct_id", "risk_base", "balance", "open_date"]
CUSTOMER_PROPS = ["cust_id", "tax_hash", "KYC_status", "segment"]
DEVICE_PROPS = ["device_hash", "os_type", "hardware_signature"]
IPADDRESS_PROPS = ["ip_string", "geo_country", "proxy_flag"]
MERCHANT_PROPS = ["merch_id", "mcc_code", "geographic_base"]

FUNDS_TRANSFER_PROPS = ["tx_id", "amount", "timestamp", "trace_id"]
ACCESSED_FROM_PROPS = ["session_id", "login_timestamp"]
USED_DEVICE_PROPS = ["application_version", "binding_flag"]
SETTLED_AT_PROPS = ["clearing_duration_ms", "terminal_id"]
OWNS_PROPS: list[str] = []  # structural only -- no PDD-defined properties for this edge

RING_LABEL_COLS_VERTEX = ["provenance", "is_ring_member", "ring_id", "ring_type"]
RING_LABEL_COLS_EDGE = ["provenance", "is_synthetic", "ring_id", "ring_type"]

PRODUCT_TO_MCC = {
    "W": "5411",  # grocery/e-commerce wallet
    "C": "6011",  # cash/ATM-like
    "R": "5732",  # electronics/retail
    "H": "7011",  # travel/hospitality
    "S": "4900",  # subscriptions/services
}


@dataclass
class GraphTables:
    accounts: pd.DataFrame
    customers: pd.DataFrame
    devices: pd.DataFrame
    ip_addresses: pd.DataFrame
    merchants: pd.DataFrame
    funds_transfer: pd.DataFrame
    accessed_from: pd.DataFrame
    used_device: pd.DataFrame
    settled_at: pd.DataFrame
    owns: pd.DataFrame


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def derive_account_universe(
    tx_df: pd.DataFrame,
    identity_df: pd.DataFrame | None,
    provenance: str,
    seed: int = 42,
) -> GraphTables:
    """Derive the background (non-ring) Account/Customer/Device/IPAddress/
    Merchant vertex tables and FUNDS_TRANSFER/ACCESSED_FROM/USED_DEVICE/
    SETTLED_AT/OWNS edge tables from real (or bundled-sample) transaction rows.

    `provenance` should be "real_kaggle" or "bundled_sample" and is stamped
    onto every row produced here so downstream EDA can separate real vs.
    synthetic volume.
    """
    rng = np.random.default_rng(seed)
    tx_df = tx_df.copy()
    tx_df["addr1"] = tx_df["addr1"].fillna(-1).astype(int)
    tx_df["acct_key"] = tx_df["card1"].astype(str) + "_" + tx_df["addr1"].astype(str)

    unique_keys = tx_df["acct_key"].unique()
    key_to_id = {k: f"ACC-{i:06d}" for i, k in enumerate(unique_keys)}
    tx_df["acct_id"] = tx_df["acct_key"].map(key_to_id)
    n_accounts = len(unique_keys)

    fraud_rate_by_acct = tx_df.groupby("acct_id")["isFraud"].mean()
    accounts = pd.DataFrame({"acct_id": list(key_to_id.values())})
    accounts["risk_base"] = np.clip(
        fraud_rate_by_acct.reindex(accounts["acct_id"]).fillna(0).to_numpy() * 3
        + rng.normal(0.05, 0.05, n_accounts),
        0.0,
        1.0,
    )
    accounts["balance"] = np.round(rng.lognormal(mean=8.5, sigma=1.1, size=n_accounts), 2)
    accounts["open_date"] = rng.integers(
        1_580_000_000, 1_735_000_000, size=n_accounts
    )  # ~2020-2024 unix seconds
    accounts["cust_id"] = [f"CUST-{i:06d}" for i in range(n_accounts)]
    accounts["provenance"] = provenance
    accounts["is_ring_member"] = False
    accounts["ring_id"] = None
    accounts["ring_type"] = None

    customers = pd.DataFrame({"cust_id": accounts["cust_id"]})
    customers["tax_hash"] = [_sha256(f"synthetic-ssn-{i}-{seed}") for i in range(n_accounts)]
    customers["KYC_status"] = rng.choice(
        ["VERIFIED", "PENDING", "REJECTED"], size=n_accounts, p=[0.85, 0.10, 0.05]
    )
    customers["segment"] = rng.choice(
        ["RETAIL", "PREMIER", "BUSINESS"], size=n_accounts, p=[0.70, 0.20, 0.10]
    )
    customers["provenance"] = provenance

    # Devices: real join sparsity -- only a subset of transactions have identity/device info.
    if identity_df is not None and len(identity_df):
        id_map = identity_df.set_index("TransactionID")[["DeviceType", "DeviceInfo"]]
        tx_df = tx_df.join(id_map, on="TransactionID")
    else:
        tx_df["DeviceType"] = None
        tx_df["DeviceInfo"] = None

    has_device = tx_df["DeviceInfo"].notna()
    device_info_pool = tx_df.loc[has_device, "DeviceInfo"].unique()
    device_hash_map = {d: f"DEV-{_md5(str(d))[:10]}" for d in device_info_pool}
    tx_df["device_hash"] = tx_df["DeviceInfo"].map(device_hash_map)

    devices = pd.DataFrame({"DeviceInfo": list(device_hash_map.keys())})
    devices["device_hash"] = devices["DeviceInfo"].map(device_hash_map)
    devices["os_type"] = rng.choice(["desktop", "mobile"], size=len(devices), p=[0.55, 0.45])
    devices["hardware_signature"] = [
        _md5(f"hw-{h}-{seed}")[:16] for h in devices["device_hash"]
    ]
    devices = devices.drop(columns=["DeviceInfo"])
    devices["provenance"] = provenance

    # IPAddress: fully synthetic regardless of provenance -- IEEE-CIS never ships real IPs.
    n_ips = max(1, int(n_accounts * 1.5))
    ip_pool = pd.DataFrame(
        {
            "ip_string": [
                f"{rng.integers(1,224)}.{rng.integers(0,255)}.{rng.integers(0,255)}.{rng.integers(0,255)}"
                for _ in range(n_ips)
            ]
        }
    )
    ip_pool["geo_country"] = rng.choice(
        ["US", "CA", "GB", "NG", "RO", "VN", "BR"], size=n_ips, p=[0.72, 0.08, 0.06, 0.05, 0.04, 0.03, 0.02]
    )
    ip_pool["proxy_flag"] = rng.choice([True, False], size=n_ips, p=[0.05, 0.95])
    ip_pool["provenance"] = provenance
    tx_df["ip_string"] = rng.choice(ip_pool["ip_string"].to_numpy(), size=len(tx_df))

    n_merchants = 50
    merchants = pd.DataFrame({"merch_id": [f"MERCH-{i:04d}" for i in range(n_merchants)]})
    merchants["mcc_code"] = rng.choice(list(PRODUCT_TO_MCC.values()), size=n_merchants)
    merchants["geographic_base"] = rng.choice(
        ["US-NE", "US-SE", "US-MW", "US-W", "US-SW"], size=n_merchants
    )
    merchants["provenance"] = provenance
    merchant_by_product = {
        pcd: merchants[merchants["mcc_code"] == mcc]["merch_id"].tolist() or merchants["merch_id"].tolist()
        for pcd, mcc in PRODUCT_TO_MCC.items()
    }

    # FUNDS_TRANSFER: each real transaction row becomes an edge to a synthesized
    # counterparty account (70% same addr1 region, else a random distinct account).
    all_acct_ids = accounts["acct_id"].to_numpy()
    acct_by_region: dict[int, np.ndarray] = {
        region: grp["acct_id"].to_numpy()
        for region, grp in tx_df.drop_duplicates("acct_id")[["acct_id", "addr1"]].groupby("addr1")
    }

    def pick_counterparty(src_acct: str, region: int) -> str:
        same_region = acct_by_region.get(region, all_acct_ids)
        pool = same_region if rng.random() < 0.7 and len(same_region) > 1 else all_acct_ids
        choice = rng.choice(pool)
        tries = 0
        while choice == src_acct and tries < 5:
            choice = rng.choice(pool)
            tries += 1
        return choice

    tx_df["dst_acct_id"] = [
        pick_counterparty(a, r) for a, r in zip(tx_df["acct_id"], tx_df["addr1"])
    ]

    funds_transfer = pd.DataFrame(
        {
            "src_acct_id": tx_df["acct_id"],
            "dst_acct_id": tx_df["dst_acct_id"],
            "tx_id": tx_df["TransactionID"].astype(str),
            "amount": tx_df["TransactionAmt"],
            "timestamp": tx_df["TransactionDT"],
            "trace_id": [str(uuid.uuid4()) for _ in range(len(tx_df))],
        }
    )
    funds_transfer["provenance"] = provenance
    funds_transfer["is_synthetic"] = False
    funds_transfer["ring_id"] = None
    funds_transfer["ring_type"] = None

    accessed_from = pd.DataFrame(
        {
            "src_acct_id": tx_df["acct_id"],
            "dst_ip_string": tx_df["ip_string"],
            "session_id": [str(uuid.uuid4()) for _ in range(len(tx_df))],
            "login_timestamp": tx_df["TransactionDT"],
        }
    )
    accessed_from["provenance"] = provenance
    accessed_from["is_synthetic"] = False
    accessed_from["ring_id"] = None
    accessed_from["ring_type"] = None

    device_rows = tx_df.loc[has_device].copy()
    used_device = pd.DataFrame(
        {
            "src_acct_id": device_rows["acct_id"],
            "dst_device_hash": device_rows["device_hash"],
            "application_version": rng.choice(
                ["3.1.0", "3.2.4", "4.0.1", "4.1.0"], size=len(device_rows)
            ),
        }
    )
    dup_counts = used_device.groupby(["src_acct_id", "dst_device_hash"]).cumcount()
    used_device["binding_flag"] = dup_counts.gt(0).to_numpy()
    used_device["provenance"] = provenance
    used_device["is_synthetic"] = False
    used_device["ring_id"] = None
    used_device["ring_type"] = None

    product_codes = tx_df["ProductCD"].fillna("W").to_numpy()
    dst_merchants = [
        rng.choice(merchant_by_product.get(pcd, merchants["merch_id"].tolist()))
        for pcd in product_codes
    ]
    settled_at = pd.DataFrame(
        {
            "src_acct_id": tx_df["acct_id"],
            "dst_merch_id": dst_merchants,
            "clearing_duration_ms": rng.integers(50, 4000, size=len(tx_df)),
            "terminal_id": [f"TERM-{rng.integers(1000,9999)}" for _ in range(len(tx_df))],
        }
    )
    settled_at["provenance"] = provenance
    settled_at["is_synthetic"] = False
    settled_at["ring_id"] = None
    settled_at["ring_type"] = None

    # OWNS: Customer -> Account, 1:1 for every background account (each has
    # exactly one owning customer here). See module docstring note 1.
    owns = pd.DataFrame(
        {
            "src_cust_id": accounts["cust_id"],
            "dst_acct_id": accounts["acct_id"],
        }
    )
    owns["provenance"] = provenance
    owns["is_synthetic"] = False
    owns["ring_id"] = None
    owns["ring_type"] = None

    accounts = accounts[ACCOUNT_PROPS + ["cust_id"] + RING_LABEL_COLS_VERTEX]
    customers = customers[CUSTOMER_PROPS + ["provenance"]]
    devices = devices[DEVICE_PROPS + ["provenance"]]
    ip_pool = ip_pool[IPADDRESS_PROPS + ["provenance"]]
    merchants = merchants[MERCHANT_PROPS + ["provenance"]]
    funds_transfer = funds_transfer[
        ["src_acct_id", "dst_acct_id"] + FUNDS_TRANSFER_PROPS + RING_LABEL_COLS_EDGE
    ]
    accessed_from = accessed_from[
        ["src_acct_id", "dst_ip_string"] + ACCESSED_FROM_PROPS + RING_LABEL_COLS_EDGE
    ]
    used_device = used_device[
        ["src_acct_id", "dst_device_hash"] + USED_DEVICE_PROPS + RING_LABEL_COLS_EDGE
    ]
    settled_at = settled_at[
        ["src_acct_id", "dst_merch_id"] + SETTLED_AT_PROPS + RING_LABEL_COLS_EDGE
    ]
    owns = owns[["src_cust_id", "dst_acct_id"] + OWNS_PROPS + RING_LABEL_COLS_EDGE]

    return GraphTables(
        accounts=accounts,
        customers=customers,
        devices=devices,
        ip_addresses=ip_pool,
        merchants=merchants,
        funds_transfer=funds_transfer,
        accessed_from=accessed_from,
        used_device=used_device,
        settled_at=settled_at,
        owns=owns,
    )
