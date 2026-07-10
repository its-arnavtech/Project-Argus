"""
Chunk 5: Cosmos DB Gremlin loader.

Reads Chunk 1's data/simulated/*.parquet output and writes vertices/edges
into the `argus-graph-container` Gremlin graph, stamping every vertex with
the shared low-cardinality `partitionKey` property per the resolved
single-partition strategy (docs/architecture/partition_key_strategy.md).
Edges are automatically co-located with their source vertex in Cosmos, so
they don't carry the property themselves.

Scope guard: loads a representative SUBSET, not the full ~590K corpus --
all 315 ring-flagged accounts plus a sample of legitimate ones, with edge
volume capped per legit account. The container shares the free tier's
1000 RU/s pool; a full load now would throttle for hours with no signal.
Full-scale load is Chunk 11's job.

Cosmos Gremlin constraints honored here (confirmed against Microsoft Learn
docs, 2026-07-09, not assumed):
  - No Gremlin bytecode -- string queries via client.submit() + bindings.
  - GraphSON v2 serializer only (GraphSONSerializersV2d0).
  - No native AAD data-plane auth for Gremlin: username is
    /dbs/{db}/colls/{graph}, password is the account key. The key is read
    from ARGUS_COSMOS_KEY or fetched at runtime via `az cosmosdb keys list`
    -- never hardcoded, never committed.
  - `null` property values are rejected -- None/NaN columns are skipped
    per-row.

Usage:
    python graph/loader.py            # load subset (drops existing data first)
    python graph/loader.py --validate # run traversal validation only, no load
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from gremlin_python.driver import client as gclient
from gremlin_python.driver import serializer
from gremlin_python.driver.protocol import GremlinServerError

REPO_ROOT = Path(__file__).resolve().parents[1]
SIM_DIR = REPO_ROOT / "data" / "simulated"

COSMOS_ACCOUNT = "cosmos-argus-dev-to614f"
RESOURCE_GROUP = "rg-argus-dev"
DATABASE = "argus-graph"
GRAPH = "argus-graph-container"
GREMLIN_URL = f"wss://{COSMOS_ACCOUNT}.gremlin.cosmos.azure.com:443/"
PARTITION_KEY_VALUE = "argus"  # single shared value -- see partition_key_strategy.md

SEED = 42
N_LEGIT_SAMPLE = 1500  # legit accounts sampled alongside all 315 ring members
MAX_RETRIES = 6


def get_cosmos_key() -> str:
    key = os.environ.get("ARGUS_COSMOS_KEY")
    if key:
        return key
    print("[LOADER] ARGUS_COSMOS_KEY not set -- fetching via az cosmosdb keys list")
    out = subprocess.run(
        [
            "az", "cosmosdb", "keys", "list",
            "--resource-group", RESOURCE_GROUP,
            "--name", COSMOS_ACCOUNT,
            "--type", "keys",
            "--query", "primaryMasterKey", "-o", "tsv",
        ],
        capture_output=True, text=True, check=True, shell=True,
    )
    return out.stdout.strip()


def make_client(key: str) -> gclient.Client:
    return gclient.Client(
        url=GREMLIN_URL,
        traversal_source="g",
        username=f"/dbs/{DATABASE}/colls/{GRAPH}",
        password=key,
        message_serializer=serializer.GraphSONSerializersV2d0(),
    )


def submit(c: gclient.Client, query: str, bindings: dict | None = None):
    """Submit with retry on 429 (RU throttling) -- the free-tier 1000 RU/s
    pool WILL throttle during a bulk load; waiting out the advised delay is
    expected behavior, not an error."""
    delay = 0.5
    for attempt in range(MAX_RETRIES):
        try:
            return c.submit(message=query, bindings=bindings).all().result()
        except GremlinServerError as e:
            msg = str(e)
            if "429" in msg or "TooManyRequests" in msg or "RequestRateTooLarge" in msg:
                time.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue
            raise
    raise RuntimeError(f"query still throttled after {MAX_RETRIES} retries: {query[:120]}")


def clean_props(row: dict) -> dict:
    """Drop None/NaN values (Cosmos rejects null properties) and convert
    numpy scalars to native Python types for GraphSON serialization."""
    out = {}
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, float) and np.isnan(v):
            continue
        if isinstance(v, np.bool_):
            v = bool(v)
        elif isinstance(v, np.integer):
            v = int(v)
        elif isinstance(v, np.floating):
            v = float(v)
        out[k] = v
    return out


def select_subset(rng: np.random.Generator):
    accounts = pd.read_parquet(SIM_DIR / "vertices_account.parquet")
    customers = pd.read_parquet(SIM_DIR / "vertices_customer.parquet")
    devices = pd.read_parquet(SIM_DIR / "vertices_device.parquet")
    ips = pd.read_parquet(SIM_DIR / "vertices_ipaddress.parquet")
    merchants = pd.read_parquet(SIM_DIR / "vertices_merchant.parquet")
    ft = pd.read_parquet(SIM_DIR / "edges_funds_transfer.parquet")
    af = pd.read_parquet(SIM_DIR / "edges_accessed_from.parquet")
    ud = pd.read_parquet(SIM_DIR / "edges_used_device.parquet")
    sa = pd.read_parquet(SIM_DIR / "edges_settled_at.parquet")
    owns = pd.read_parquet(SIM_DIR / "edges_owns.parquet")

    ring_ids = set(accounts.loc[accounts["is_ring_member"], "acct_id"])
    legit_pool = accounts.loc[~accounts["is_ring_member"], "acct_id"].to_numpy()
    legit_ids = set(rng.choice(legit_pool, size=N_LEGIT_SAMPLE, replace=False))

    # Force-include counterparties of ring FUNDS_TRANSFER edges (circular
    # rings have one entry and one exit edge touching background accounts) so
    # those edges resolve instead of dangling.
    ring_ft = ft[ft["ring_id"].notna()]
    counterparties = (set(ring_ft["src_acct_id"]) | set(ring_ft["dst_acct_id"])) - ring_ids
    selected = ring_ids | legit_ids | counterparties

    sel_accounts = accounts[accounts["acct_id"].isin(selected)]

    # FUNDS_TRANSFER: only edges with BOTH endpoints selected.
    sel_ft = ft[ft["src_acct_id"].isin(selected) & ft["dst_acct_id"].isin(selected)]

    # ACCESSED_FROM / USED_DEVICE / SETTLED_AT: ring members keep ALL their
    # edges (shared-device/IP structure IS the fraud signal); legit accounts
    # keep one edge each, enough to be realistically connected without
    # exploding vertex/edge counts against the shared RU/s pool.
    def cap_edges(df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["src_acct_id"].isin(selected)]
        is_ring = df["src_acct_id"].isin(ring_ids)
        return pd.concat(
            [df[is_ring], df[~is_ring].drop_duplicates(subset="src_acct_id", keep="first")],
            ignore_index=True,
        )

    sel_af, sel_ud, sel_sa = cap_edges(af), cap_edges(ud), cap_edges(sa)
    sel_owns = owns[owns["dst_acct_id"].isin(selected)]

    sel_customers = customers[customers["cust_id"].isin(set(sel_accounts["cust_id"]) | set(sel_owns["src_cust_id"]))]
    sel_devices = devices[devices["device_hash"].isin(set(sel_ud["dst_device_hash"]))]
    sel_ips = ips[ips["ip_string"].isin(set(sel_af["dst_ip_string"]))]
    sel_merchants = merchants[merchants["merch_id"].isin(set(sel_sa["dst_merch_id"]))]

    return {
        "vertices": {
            "Account": (sel_accounts, "acct_id"),
            "Customer": (sel_customers, "cust_id"),
            "Device": (sel_devices, "device_hash"),
            "IPAddress": (sel_ips, "ip_string"),
            "Merchant": (sel_merchants, "merch_id"),
        },
        "edges": {
            "FUNDS_TRANSFER": (sel_ft, "src_acct_id", "dst_acct_id"),
            "ACCESSED_FROM": (sel_af, "src_acct_id", "dst_ip_string"),
            "USED_DEVICE": (sel_ud, "src_acct_id", "dst_device_hash"),
            "SETTLED_AT": (sel_sa, "src_acct_id", "dst_merch_id"),
            "OWNS": (sel_owns, "src_cust_id", "dst_acct_id"),
        },
    }


def drop_existing(c: gclient.Client) -> None:
    """Batched drop -- a single g.V().drop() over a large graph times out or
    blows the RU budget in one request."""
    while True:
        remaining = submit(c, "g.V().count()")[0]
        if remaining == 0:
            return
        print(f"[LOADER] dropping existing data... {remaining} vertices remain")
        submit(c, "g.V().limit(500).drop()")


def load(c: gclient.Client, subset: dict) -> dict:
    counts = {"vertices": {}, "edges": {}}

    for label, (df, id_col) in subset["vertices"].items():
        n = 0
        for row in df.to_dict("records"):
            props = clean_props(row)
            vid = str(props.pop(id_col))
            query = f"g.addV('{label}').property('id', vid).property('{id_col}', vid).property('partitionKey', pk)"
            bindings = {"vid": vid, "pk": PARTITION_KEY_VALUE}
            for i, (k, v) in enumerate(props.items()):
                query += f".property('{k}', p{i})"
                bindings[f"p{i}"] = v
            submit(c, query, bindings)
            n += 1
            if n % 500 == 0:
                print(f"[LOADER]   {label}: {n}/{len(df)}")
        counts["vertices"][label] = n
        print(f"[LOADER] vertices {label}: {n}")

    for label, (df, src_col, dst_col) in subset["edges"].items():
        n = 0
        for row in df.to_dict("records"):
            props = clean_props(row)
            src = str(props.pop(src_col))
            dst = str(props.pop(dst_col))
            query = "g.V(srcId).has('partitionKey', pk).addE(elabel).to(g.V(dstId).has('partitionKey', pk))"
            bindings = {"srcId": src, "dstId": dst, "pk": PARTITION_KEY_VALUE, "elabel": label}
            for i, (k, v) in enumerate(props.items()):
                query += f".property('{k}', p{i})"
                bindings[f"p{i}"] = v
            submit(c, query, bindings)
            n += 1
            if n % 500 == 0:
                print(f"[LOADER]   {label}: {n}/{len(df)}")
        counts["edges"][label] = n
        print(f"[LOADER] edges {label}: {n}")

    return counts


def validate(c: gclient.Client) -> None:
    print("\n=== TRAVERSAL VALIDATION ===")
    v_total = submit(c, "g.V().count()")[0]
    e_total = submit(c, "g.E().count()")[0]
    v_by_label = submit(c, "g.V().groupCount().by(label)")[0]
    e_by_label = submit(c, "g.E().groupCount().by(label)")[0]
    print(f"vertices: {v_total} {v_by_label}")
    print(f"edges:    {e_total} {e_by_label}")

    # (a) Multi-hop: from a device_cluster ring member Account, hop to its
    # shared Device, then back out to other Accounts on the same Device --
    # exactly the traversal Chunk 8's Network Tracer agent depends on.
    ring_accts = submit(
        c,
        "g.V().hasLabel('Account').has('ring_type', 'device_cluster').limit(1).values('acct_id')",
    )
    if not ring_accts:
        print("(a) FAIL: no device_cluster ring member found in graph")
    else:
        start = ring_accts[0]
        shared_dev = submit(
            c,
            "g.V(aid).has('partitionKey', pk).out('USED_DEVICE')"
            ".in('USED_DEVICE').has('acct_id', neq(aid)).dedup().values('acct_id')",
            {"aid": start, "pk": PARTITION_KEY_VALUE},
        )
        shared_ip = submit(
            c,
            "g.V(aid).has('partitionKey', pk).out('ACCESSED_FROM')"
            ".in('ACCESSED_FROM').has('acct_id', neq(aid)).dedup().values('acct_id')",
            {"aid": start, "pk": PARTITION_KEY_VALUE},
        )
        print(f"(a) start={start}")
        print(f"    accounts sharing a Device: {shared_dev}")
        print(f"    accounts sharing an IP:    {shared_ip}")
        ok = len(shared_dev) > 0 or len(shared_ip) > 0
        print(f"    multi-hop connectivity: {'PASS' if ok else 'FAIL'}")

    # (b) Customer -> OWNS -> Account
    owns_count = submit(c, "g.E().hasLabel('OWNS').count()")[0]
    sample = submit(
        c,
        "g.V().hasLabel('Customer').limit(1).as('c')"
        ".out('OWNS').as('a').select('c','a').by(values('cust_id')).by(values('acct_id'))",
    )
    print(f"(b) OWNS edges: {owns_count}; sample Customer->OWNS->Account: {sample}")
    print(f"    OWNS traversal: {'PASS' if owns_count > 0 and sample else 'FAIL'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true", help="validation only, no load")
    args = parser.parse_args()

    key = get_cosmos_key()
    c = make_client(key)
    try:
        if not args.validate:
            rng = np.random.default_rng(SEED)
            subset = select_subset(rng)
            planned_v = sum(len(df) for df, _ in subset["vertices"].values())
            planned_e = sum(len(df) for df, _, _ in subset["edges"].values())
            print(f"[LOADER] planned: {planned_v} vertices, {planned_e} edges")
            drop_existing(c)
            start = time.time()
            counts = load(c, subset)
            elapsed = time.time() - start
            print(f"\n[LOADER] load complete in {elapsed:.0f}s: {json.dumps(counts)}")
        validate(c)
    finally:
        c.close()


if __name__ == "__main__":
    main()
