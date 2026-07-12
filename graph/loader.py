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

Cosmos Gremlin constraints/capabilities honored here (confirmed against
Microsoft Learn docs AND empirically tested against the live account,
2026-07-09 -- not assumed):
  - No Gremlin bytecode -- string queries via client.submit() + bindings.
  - GraphSON v2 serializer only (GraphSONSerializersV2d0).
  - SECURITY CORRECTION: an earlier session accepted "no AAD data-plane
    auth for Gremlin, password must be the account key" as a platform
    constraint without testing it. That was wrong. Gremlin-specific RBAC
    (`Microsoft.DocumentDB/databaseAccounts/gremlinRoleDefinitions` +
    `gremlinRoleAssignments`, built-in roles "Cosmos DB Gremlin Built-in
    Data Reader"/"Data Contributor") is real and its Entra ID token IS
    accepted by the actual wire-protocol connection as the password field
    -- verified empirically: created a role assignment via
    `az cosmosdb gremlin role assignment create`, then connected with a
    plain `DefaultAzureCredential` token and performed a real write/read/
    delete round-trip. No static key anywhere in this file anymore.
  - `null` property values are rejected -- None/NaN columns are skipped
    per-row.

Usage:
    python graph/loader.py                 # load subset (drops existing data first)
    python graph/loader.py --full          # load the FULL corpus (~2.07M elements;
                                            # post-Chunk-11 benchmark -- needs the
                                            # Cosmos RU bump, concurrent workers)
    python graph/loader.py --validate      # run traversal validation only, no load
    python graph/loader.py --add-ring-owns # add only the ring accounts' OWNS
                                            # edges (both endpoints already
                                            # loaded; targeted fix, no drop)
"""
from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
from azure.identity import DefaultAzureCredential
from gremlin_python.driver import client as gclient
from gremlin_python.driver import serializer
from gremlin_python.driver.protocol import GremlinServerError

# Post-Chunk-11: the original loader is strictly sequential (~17 elements/sec
# observed in Chunk 5), which would take DAYS for the 2.07M-element full
# corpus. Bounded worker threads each run the same retrying submit() against
# a shared client whose connection pool matches the worker count. 32 workers
# ~= the concurrency needed to saturate the single logical partition's
# 10,000 RU/s ceiling at ~10-15 RU and ~30-70ms per insert.
LOADER_CONCURRENCY = int(os.environ.get("ARGUS_LOADER_CONCURRENCY", "32"))

REPO_ROOT = Path(__file__).resolve().parents[1]
SIM_DIR = REPO_ROOT / "data" / "simulated"

COSMOS_ACCOUNT = "cosmos-argus-dev-to614f"
DATABASE = "argus-graph"
GRAPH = "argus-graph-container"
GREMLIN_URL = f"wss://{COSMOS_ACCOUNT}.gremlin.cosmos.azure.com:443/"
PARTITION_KEY_VALUE = "argus"  # single shared value -- see partition_key_strategy.md

SEED = 42
N_LEGIT_SAMPLE = 1500  # legit accounts sampled alongside all 315 ring members
MAX_RETRIES = 6


def get_cosmos_token() -> str:
    """Entra ID token, scoped to the Cosmos data-plane audience -- requires
    a Gremlin RBAC role assignment on the caller's identity (see
    infra/envs/dev/main.tf's dev_cosmos_gremlin_data_contributor). No
    static key, no ARGUS_COSMOS_KEY env var, nothing to leak or rotate."""
    return DefaultAzureCredential().get_token("https://cosmos.azure.com/.default").token


def make_client(token: str, pool_size: int = 4) -> gclient.Client:
    return gclient.Client(
        url=GREMLIN_URL,
        traversal_source="g",
        username=f"/dbs/{DATABASE}/colls/{GRAPH}",
        password=token,
        message_serializer=serializer.GraphSONSerializersV2d0(),
        pool_size=pool_size,
        max_workers=pool_size,
    )


class ClientHolder:
    """Shared client with PROACTIVE token refresh. An Entra token lives
    ~60-90 min; the full-corpus edge load runs ~90 min (the single logical
    partition caps at 10,000 RU/s => ~350 edges/sec regardless of worker
    count), so it WILL outlive one token. The first attempt relied on
    reactive refresh keyed on "401"/"Unauthorized" in the error string --
    but Cosmos closes the websocket on expiry, which surfaces as a
    connection/transport error, not a clean 401, so the match missed and
    every post-expiry insert failed into the failure list. Fix: rebuild
    the client on a time budget (`maybe_refresh`), called only between
    fully-drained chunks so no in-flight op is ever using a client being
    closed. Reactive detection is kept as a broadened backstop."""

    def __init__(self, pool_size: int, max_age_sec: float = 1800.0):
        self.pool_size = pool_size
        self.max_age_sec = max_age_sec
        self.lock = threading.Lock()
        self.client = make_client(get_cosmos_token(), pool_size)
        self.created_at = time.time()

    def _rebuild(self) -> None:
        old = self.client
        self.client = make_client(get_cosmos_token(), self.pool_size)
        self.created_at = time.time()
        try:
            old.close()
        except Exception:  # noqa: BLE001 -- best-effort
            pass

    def maybe_refresh(self) -> None:
        """Proactive: rebuild if the token is older than the budget. Safe to
        call ONLY when no futures are in flight (between drained chunks)."""
        if time.time() - self.created_at >= self.max_age_sec:
            print("[LOADER] proactive token refresh (client age exceeded budget)", flush=True)
            self._rebuild()

    def refresh(self, dead_client) -> None:
        """Reactive backstop: a thread hit an auth/connection error mid-chunk.
        Only the first thread to notice this particular dead client rebuilds."""
        with self.lock:
            if self.client is dead_client:
                print("[LOADER] reactive client rebuild after transport/auth error", flush=True)
                self._rebuild()


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


_AUTH_OR_TRANSPORT_MARKERS = (
    "401", "unauthorized", "authorization", "authentication",
    "connection", "closed", "transport", "websocket", "eof", "broken pipe",
)


def submit_via_holder(holder: ClientHolder, query: str, bindings: dict | None = None):
    """submit() plus auth/transport recovery, for the long-running full load.
    Detection is broadened well beyond '401' because token expiry manifests
    as a closed websocket / transport error, not a clean auth code (the
    lesson from the first full-load attempt)."""
    for _ in range(4):
        c = holder.client
        try:
            return submit(c, query, bindings)
        except Exception as e:  # noqa: BLE001 -- classify, re-raise if not recoverable
            msg = str(e).lower()
            if any(marker in msg for marker in _AUTH_OR_TRANSPORT_MARKERS):
                holder.refresh(c)
                time.sleep(0.5)
                continue
            raise
    raise RuntimeError("auth/transport retry exhausted for query: " + query[:120])


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


def select_full():
    """The ENTIRE corpus -- every vertex and edge table, no sampling, no
    caps. 142,395 vertices + 1,930,979 edges = 2,073,374 elements (counted
    from the parquet files, 2026-07-12). Only viable with the benchmark
    Cosmos RU bump + concurrent workers."""
    return {
        "vertices": {
            "Account": (pd.read_parquet(SIM_DIR / "vertices_account.parquet"), "acct_id"),
            "Customer": (pd.read_parquet(SIM_DIR / "vertices_customer.parquet"), "cust_id"),
            "Device": (pd.read_parquet(SIM_DIR / "vertices_device.parquet"), "device_hash"),
            "IPAddress": (pd.read_parquet(SIM_DIR / "vertices_ipaddress.parquet"), "ip_string"),
            "Merchant": (pd.read_parquet(SIM_DIR / "vertices_merchant.parquet"), "merch_id"),
        },
        "edges": {
            "FUNDS_TRANSFER": (pd.read_parquet(SIM_DIR / "edges_funds_transfer.parquet"), "src_acct_id", "dst_acct_id"),
            "ACCESSED_FROM": (pd.read_parquet(SIM_DIR / "edges_accessed_from.parquet"), "src_acct_id", "dst_ip_string"),
            "USED_DEVICE": (pd.read_parquet(SIM_DIR / "edges_used_device.parquet"), "src_acct_id", "dst_device_hash"),
            "SETTLED_AT": (pd.read_parquet(SIM_DIR / "edges_settled_at.parquet"), "src_acct_id", "dst_merch_id"),
            "OWNS": (pd.read_parquet(SIM_DIR / "edges_owns.parquet"), "src_cust_id", "dst_acct_id"),
        },
    }


# Rows are processed in fully-drained chunks: every future in a chunk
# resolves before the next chunk starts, which gives a safe point to
# proactively refresh the token (no op is ever mid-flight on a client being
# closed). ~20k rows/chunk at ~350/sec is ~1 min -- fine-grained progress
# and refresh checks without per-row lock overhead.
LOAD_CHUNK_ROWS = int(os.environ.get("ARGUS_LOAD_CHUNK_ROWS", "20000"))


def _run_group(pool, holder, label, insert_fn, rows, failures) -> int:
    """Load one vertex/edge group in drained chunks; returns success count."""
    n_ok = 0
    t0 = time.time()
    for start in range(0, len(rows), LOAD_CHUNK_ROWS):
        holder.maybe_refresh()  # safe: previous chunk fully drained
        chunk = rows[start : start + LOAD_CHUNK_ROWS]
        futs = [pool.submit(insert_fn, row) for row in chunk]
        for f in futs:
            try:
                f.result()
                n_ok += 1
            except Exception as e:  # noqa: BLE001 -- collected, surfaced loudly at end
                failures.append(f"{label}: {e}")
        rate = n_ok / max(time.time() - t0, 1e-9)
        print(f"[LOADER]   {label}: {n_ok}/{len(rows)} ok ({rate:.0f}/s)", flush=True)
    print(f"[LOADER] {label}: {n_ok}/{len(rows)} in {time.time()-t0:.0f}s", flush=True)
    return n_ok


def load_concurrent(holder: ClientHolder, tables: dict, edges_only: bool = False) -> dict:
    """Chunked concurrent load with drain-safe proactive token refresh.
    `edges_only` skips vertices (used when a prior run already landed all
    142,395 vertices and only edges need (re)loading)."""
    counts = {"vertices": {}, "edges": {}}
    failures: list[str] = []

    def insert_vertex(label, id_col, row):
        props = clean_props(row)
        vid = str(props.pop(id_col))
        query = f"g.addV('{label}').property('id', vid).property('{id_col}', vid).property('partitionKey', pk)"
        bindings = {"vid": vid, "pk": PARTITION_KEY_VALUE}
        for i, (k, v) in enumerate(props.items()):
            query += f".property('{k}', p{i})"
            bindings[f"p{i}"] = v
        submit_via_holder(holder, query, bindings)

    def insert_edge(label, src_col, dst_col, row):
        props = clean_props(row)
        src = str(props.pop(src_col))
        dst = str(props.pop(dst_col))
        query = "g.V(srcId).has('partitionKey', pk).addE(elabel).to(g.V(dstId).has('partitionKey', pk))"
        bindings = {"srcId": src, "dstId": dst, "pk": PARTITION_KEY_VALUE, "elabel": label}
        for i, (k, v) in enumerate(props.items()):
            query += f".property('{k}', p{i})"
            bindings[f"p{i}"] = v
        submit_via_holder(holder, query, bindings)

    with ThreadPoolExecutor(max_workers=LOADER_CONCURRENCY) as pool:
        if not edges_only:
            for label, (df, id_col) in tables["vertices"].items():
                rows = df.to_dict("records")
                counts["vertices"][label] = _run_group(
                    pool, holder, f"V:{label}", lambda r, la=label, ic=id_col: insert_vertex(la, ic, r), rows, failures
                )
        for label, (df, src_col, dst_col) in tables["edges"].items():
            rows = df.to_dict("records")
            counts["edges"][label] = _run_group(
                pool, holder, f"E:{label}",
                lambda r, la=label, sc=src_col, dc=dst_col: insert_edge(la, sc, dc, r), rows, failures
            )

    if failures:
        print(f"[LOADER] !!! {len(failures)} element failures; first 5: {failures[:5]}", flush=True)
    counts["failures"] = len(failures)
    return counts


def drop_all_edges(holder: ClientHolder) -> None:
    """Batched edge drop -- used before an edges-only reload so the 42k
    FUNDS_TRANSFER edges from the failed first attempt don't duplicate."""
    while True:
        remaining = submit_via_holder(holder, "g.E().count()")[0]
        if remaining == 0:
            return
        print(f"[LOADER] dropping existing edges... {remaining} remain", flush=True)
        submit_via_holder(holder, "g.E().limit(1000).drop()")


def add_ring_owns_edges(c: gclient.Client) -> int:
    """Targeted fix: ring-injected accounts originally had no OWNS edge
    (only background accounts did -- see context.md Audit Flag #3).
    ring_injector.py now emits one for every ring account too; both
    endpoints (the ring Account and its Customer) were already loaded in
    Chunk 5 (Account's cust_id FK column always included ring accounts),
    so this only needs to add the missing edges, not reload vertices.
    Idempotent: skips any (src, dst) pair that already has an OWNS edge,
    safe to rerun."""
    owns = pd.read_parquet(SIM_DIR / "edges_owns.parquet")
    ring_owns = owns[owns["provenance"] == "synthetic_ring"]
    n_added, n_skipped = 0, 0
    for row in ring_owns.to_dict("records"):
        props = clean_props(row)
        src = str(props.pop("src_cust_id"))
        dst = str(props.pop("dst_acct_id"))
        exists = submit(
            c,
            "g.V(srcId).has('partitionKey', pk).outE('OWNS').where(inV().has('acct_id', dstId)).count()",
            {"srcId": src, "dstId": dst, "pk": PARTITION_KEY_VALUE},
        )[0]
        if exists:
            n_skipped += 1
            continue
        query = "g.V(srcId).has('partitionKey', pk).addE('OWNS').to(g.V(dstId).has('partitionKey', pk))"
        bindings = {"srcId": src, "dstId": dst, "pk": PARTITION_KEY_VALUE}
        for i, (k, v) in enumerate(props.items()):
            query += f".property('{k}', p{i})"
            bindings[f"p{i}"] = v
        submit(c, query, bindings)
        n_added += 1
    print(f"[LOADER] ring OWNS edges added: {n_added} (skipped {n_skipped} already present)")
    return n_added


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

    # (b) Customer -> OWNS -> Account. Specifically on a RING member this
    # time -- the original Chunk 5 validation picked "the first Customer
    # found", which happened to be a background account, not a ring
    # account, so it never actually exercised the case this fix addresses.
    owns_count = submit(c, "g.E().hasLabel('OWNS').count()")[0]
    ring_owns_count = submit(
        c, "g.E().hasLabel('OWNS').where(inV().has('is_ring_member', true)).count()"
    )[0]
    sample = submit(
        c,
        "g.V().hasLabel('Account').has('is_ring_member', true).limit(1).as('a')"
        ".in('OWNS').as('c').select('c','a').by(values('cust_id')).by(values('acct_id'))",
    )
    print(
        f"(b) OWNS edges: {owns_count} total, {ring_owns_count} on ring accounts; "
        f"sample Customer->OWNS->Account (ring member): {sample}"
    )
    print(f"    OWNS traversal (ring member): {'PASS' if ring_owns_count > 0 and sample else 'FAIL'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true", help="validation only, no load")
    parser.add_argument("--full", action="store_true", help="load the FULL corpus (concurrent; needs the RU bump)")
    parser.add_argument("--edges-only", action="store_true", help="drop+reload edges only (vertices already present)")
    parser.add_argument(
        "--add-ring-owns",
        action="store_true",
        help="add only the ring accounts' OWNS edges (targeted fix, no drop/reload)",
    )
    args = parser.parse_args()

    if args.full or args.edges_only:
        holder = ClientHolder(pool_size=LOADER_CONCURRENCY)
        try:
            tables = select_full()
            planned_v = sum(len(df) for df, _ in tables["vertices"].values())
            planned_e = sum(len(df) for df, _, _ in tables["edges"].values())
            if args.edges_only:
                print(f"[LOADER] EDGES-ONLY reload: {planned_e} edges, {LOADER_CONCURRENCY} workers "
                      f"(assumes {planned_v} vertices already loaded)")
                drop_all_edges(holder)
            else:
                print(f"[LOADER] FULL load planned: {planned_v} vertices, {planned_e} edges, "
                      f"{LOADER_CONCURRENCY} workers")
                drop_existing(holder.client)
            start = time.time()
            counts = load_concurrent(holder, tables, edges_only=args.edges_only)
            elapsed = time.time() - start
            print(f"\n[LOADER] load complete in {elapsed:.0f}s: {json.dumps(counts)}")
            validate(holder.client)
        finally:
            holder.client.close()
        return

    token = get_cosmos_token()
    c = make_client(token)
    try:
        if args.add_ring_owns:
            add_ring_owns_edges(c)
        elif not args.validate:
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
