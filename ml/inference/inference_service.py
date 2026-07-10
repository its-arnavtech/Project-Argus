"""
Chunk 7: real-time GNN inference service.

Consumes enriched transaction events from the existing "transactions" Event
Hub (no new Azure infrastructure -- see the architecture note below),
incrementally maintains local graph state, scores affected accounts with the
Chunk 6 model, and writes `gnn_risk_score` back onto the corresponding
Account vertices in `argus-graph-container` via gremlinpython.

Architecture notes (logged in context.md):
  - Downstream discovery is CROSS-QUERY, NOT CROSS-QUEUE: Chunk 8's
    compliance agent finds high-risk nodes by querying Cosmos directly
    (g.V().has('gnn_risk_score', gt(threshold))), not via a separate
    messaging channel. No new Event Hub / queue was provisioned for this.
  - Local state is WARM-STARTED from data/simulated/'s parquet snapshot
    (the same aggregates training used), then updated incrementally per
    event. Cold-starting would make early scores meaningless -- features
    like tx_count would be near-zero for every account regardless of
    behavior, nothing like the training distribution.
  - Scoring runs a full-graph forward pass per batch rather than extracting
    per-node 2-hop subgraphs: at this scale (~40K nodes, 4 features, CPU)
    a full pass takes well under a second and is exactly equivalent for a
    2-layer model, with none of the subgraph-extraction bookkeeping.
  - The event's SHA-256 `device_hash` (Rust ingestion output) is a
    different hash space from the parquet snapshot's DEV-md5 hashes; for
    incremental device-association counting both are just opaque device
    identifiers, so mixing spaces only means an account's device set can
    grow, never collide -- acceptable for directional Chunk 7 validation.

Auth: both Event Hubs AND Cosmos Gremlin via azure.identity
DefaultAzureCredential (Azure AD tokens, no static secret anywhere). The
Cosmos side was corrected after an earlier session wrongly accepted "no
AAD data-plane auth for Gremlin, must use the account key" without testing
it -- Gremlin-specific RBAC (gremlinRoleDefinitions/gremlinRoleAssignments)
is real, and its token IS accepted by the wire-protocol connection,
verified empirically (see graph/loader.py's docstring for how it was
tested). Reuses the same role assignment as the loader.

Usage:
    python ml/inference/inference_service.py             # consume + score + write back
    python ml/inference/inference_service.py --validate  # post-run Cosmos sanity checks only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "ml"))
sys.path.insert(0, str(REPO_ROOT / "ml" / "training"))

from model_def import InstitutionalFraudSAGE  # noqa: E402
from features import build_features_and_graph  # noqa: E402

ARTIFACT_DIR = REPO_ROOT / "ml" / "artifacts"

EVENTHUB_NAMESPACE = os.environ.get(
    "ARGUS_EVENTHUB_NAMESPACE", "evhns-argus-dev-to614f.servicebus.windows.net"
)
EVENTHUB_NAME = os.environ.get("ARGUS_EVENTHUB_NAME", "transactions")

COSMOS_ACCOUNT = "cosmos-argus-dev-to614f"
DATABASE = "argus-graph"
GRAPH = "argus-graph-container"
PARTITION_KEY_VALUE = "argus"

MAX_EVENTS = int(os.environ.get("ARGUS_MAX_EVENTS", "500"))
IDLE_TIMEOUT_S = int(os.environ.get("ARGUS_IDLE_TIMEOUT", "45"))


def get_cosmos_token() -> str:
    """Entra ID token, scoped to the Cosmos data-plane audience -- requires
    the same Gremlin RBAC role assignment graph/loader.py uses. No static
    key, no ARGUS_COSMOS_KEY env var."""
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential().get_token("https://cosmos.azure.com/.default").token


def make_gremlin_client(token: str):
    from gremlin_python.driver import client as gclient
    from gremlin_python.driver import serializer

    return gclient.Client(
        url=f"wss://{COSMOS_ACCOUNT}.gremlin.cosmos.azure.com:443/",
        traversal_source="g",
        username=f"/dbs/{DATABASE}/colls/{GRAPH}",
        password=token,
        message_serializer=serializer.GraphSONSerializersV2d0(),
    )


class GraphState:
    """Warm-started incremental graph state: feature arrays maintained
    in-place as events arrive, edge set extended with new transfers."""

    def __init__(self):
        print("[INFERENCE] warm-starting state from data/simulated/ ...")
        acct_ids, x, edge_index, y, ring_id, meta = build_features_and_graph()
        self.acct_ids = acct_ids
        self.idx = {a: i for i, a in enumerate(acct_ids)}
        self.y = y  # ground truth, used only for post-hoc sanity reporting
        self.edge_set = set(map(tuple, edge_index.T.tolist()))

        self.tx_count = x[:, 0].astype(np.float64).copy()
        self.uniq_cp = x[:, 1].astype(np.float64).copy()
        self.var = x[:, 2].astype(np.float64).copy()
        self.dev_count = x[:, 3].astype(np.float64).copy()

        # Welford state for incremental variance, seeded from the snapshot:
        # M2 = var * (n - 1), using tx_count as n.
        self.w_n = self.tx_count.copy()
        amounts_mean = np.zeros_like(self.tx_count)
        # per-account mean isn't in the feature vector; approximate seed mean
        # as 0-centered update base is wrong -- recompute cheaply from parquet:
        import pandas as pd
        ft = pd.read_parquet(REPO_ROOT / "data" / "simulated" / "edges_funds_transfer.parquet")
        amt = pd.concat([
            ft[["src_acct_id", "amount"]].rename(columns={"src_acct_id": "a"}),
            ft[["dst_acct_id", "amount"]].rename(columns={"dst_acct_id": "a"}),
        ])
        mean_s = amt.groupby("a")["amount"].mean()
        for a, m in mean_s.items():
            if a in self.idx:
                amounts_mean[self.idx[a]] = m
        self.w_mean = amounts_mean
        self.w_m2 = np.where(self.w_n > 1, self.var * np.maximum(self.w_n - 1, 0), 0.0)

        # sets for uniqueness checks
        self.cp_sets: dict[int, set] = {}
        for s, d in zip(ft["src_acct_id"], ft["dst_acct_id"]):
            si, di = self.idx.get(s), self.idx.get(d)
            if si is not None and di is not None:
                self.cp_sets.setdefault(si, set()).add(di)
                self.cp_sets.setdefault(di, set()).add(si)
        ud = pd.read_parquet(REPO_ROOT / "data" / "simulated" / "edges_used_device.parquet")
        self.dev_sets: dict[int, set] = {}
        for a, h in zip(ud["src_acct_id"], ud["dst_device_hash"]):
            ai = self.idx.get(a)
            if ai is not None:
                self.dev_sets.setdefault(ai, set()).add(h)
        print(f"[INFERENCE] state ready: {meta['n_nodes']} nodes, {meta['n_edges_directed']} edges")

    def apply_event(self, evt: dict) -> list[int]:
        """Update state for one enriched transaction; returns affected node indices."""
        affected = []
        src = self.idx.get(evt.get("source_account"))
        dst = self.idx.get(evt.get("target_account"))
        amount = float(evt.get("amount", 0.0))
        dev = evt.get("device_hash")

        for node, other in ((src, dst), (dst, src)):
            if node is None:
                continue
            affected.append(node)
            self.tx_count[node] += 1
            # Welford update
            self.w_n[node] += 1
            delta = amount - self.w_mean[node]
            self.w_mean[node] += delta / self.w_n[node]
            self.w_m2[node] += delta * (amount - self.w_mean[node])
            if self.w_n[node] > 1:
                self.var[node] = self.w_m2[node] / (self.w_n[node] - 1)
            if other is not None:
                cps = self.cp_sets.setdefault(node, set())
                if other not in cps:
                    cps.add(other)
                    self.uniq_cp[node] += 1

        if src is not None and dev:
            devs = self.dev_sets.setdefault(src, set())
            if dev not in devs:
                devs.add(dev)
                self.dev_count[src] += 1

        if src is not None and dst is not None and src != dst:
            self.edge_set.add((src, dst))
            self.edge_set.add((dst, src))
        return affected

    def tensors(self):
        x = np.stack([self.tx_count, self.uniq_cp, self.var, self.dev_count], axis=1).astype(np.float32)
        edge_index = np.array(sorted(self.edge_set), dtype=np.int64).T
        return torch.tensor(x), torch.tensor(edge_index)


class InferenceService:
    def __init__(self):
        cfg = json.loads((ARTIFACT_DIR / "model_config.json").read_text())
        stats = json.loads((ARTIFACT_DIR / "feature_stats.json").read_text())
        self.mu = np.array(stats["mean"], dtype=np.float32)
        self.sigma = np.array(stats["std"], dtype=np.float32)
        self.model = InstitutionalFraudSAGE(cfg["in_channels"], cfg["hidden_channels"], cfg["out_channels"])
        self.model.load_state_dict(torch.load(ARTIFACT_DIR / "model.pt", weights_only=True))
        self.model.eval()

        self.state = GraphState()
        self.gclient = make_gremlin_client(get_cosmos_token())
        loaded = self.gclient.submit(
            "g.V().hasLabel('Account').values('acct_id')"
        ).all().result()
        self.cosmos_accounts = set(loaded)
        print(f"[INFERENCE] {len(self.cosmos_accounts)} Account vertices present in Cosmos")

        self.events_seen = 0
        self.latencies: list[float] = []
        self.scored_total = 0
        self.stop_event = threading.Event()
        self.last_event_time = time.time()

    def score_and_write(self, affected: set[int], batch_t0: float) -> None:
        x, edge_index = self.state.tensors()
        x = (x - torch.tensor(self.mu)) / torch.tensor(self.sigma)
        with torch.no_grad():
            prob1 = self.model(x, edge_index).exp()[:, 1].numpy()
        t_forward = time.time()

        writes = 0
        for node in affected:
            aid = self.state.acct_ids[node]
            if aid not in self.cosmos_accounts:
                continue
            score = float(prob1[node])
            self.gclient.submit(
                message=(
                    "g.V(aid).has('partitionKey', pk)"
                    ".property('gnn_risk_score', s).property('gnn_scored_at', ts)"
                ),
                bindings={"aid": aid, "pk": PARTITION_KEY_VALUE, "s": score, "ts": int(time.time())},
            ).all().result()
            writes += 1
        t_done = time.time()
        self.scored_total += writes
        n_events = max(len(affected) // 2, 1)
        per_event_ms = (t_done - batch_t0) / n_events * 1000
        self.latencies.append(per_event_ms)
        print(
            f"[INFERENCE] batch: {n_events} events, {len(affected)} affected nodes, "
            f"{writes} Cosmos writes | forward={t_forward - batch_t0:.2f}s "
            f"write={t_done - t_forward:.2f}s | amortized {per_event_ms:.0f} ms/event"
        )

    def on_event_batch(self, partition_context, events) -> None:
        if not events:
            if time.time() - self.last_event_time > IDLE_TIMEOUT_S and self.events_seen > 0:
                self.stop_event.set()
            return
        self.last_event_time = time.time()
        t0 = time.time()
        affected: set[int] = set()
        for event in events:
            try:
                evt = json.loads(event.body_as_str())
            except Exception as e:  # noqa: BLE001 -- skip malformed, keep consuming
                print(f"[INFERENCE] skipping malformed event: {e}")
                continue
            for node in self.state.apply_event(evt):
                affected.add(node)
            self.events_seen += 1
        if affected:
            self.score_and_write(affected, t0)
        if self.events_seen >= MAX_EVENTS:
            self.stop_event.set()

    def run(self) -> None:
        from azure.eventhub import EventHubConsumerClient
        from azure.identity import DefaultAzureCredential

        consumer = EventHubConsumerClient(
            fully_qualified_namespace=EVENTHUB_NAMESPACE,
            eventhub_name=EVENTHUB_NAME,
            consumer_group="$Default",
            credential=DefaultAzureCredential(),
        )
        print(f"[INFERENCE] consuming from {EVENTHUB_NAMESPACE}/{EVENTHUB_NAME} (new events only)...")
        worker = threading.Thread(
            target=consumer.receive_batch,
            kwargs={
                "on_event_batch": self.on_event_batch,
                "max_batch_size": 100,
                "max_wait_time": 5,
                "starting_position": "@latest",
            },
            daemon=True,
        )
        self.last_event_time = time.time()
        worker.start()
        # main thread waits for stop signal or global idle timeout
        while not self.stop_event.is_set():
            time.sleep(1)
            if time.time() - self.last_event_time > IDLE_TIMEOUT_S * 4 and self.events_seen == 0:
                print("[INFERENCE] no events arrived at all -- stopping")
                break
        consumer.close()
        self.gclient.close()

        if self.latencies:
            lat = np.array(self.latencies)
            print(
                f"\n[INFERENCE] DONE: {self.events_seen} events, {self.scored_total} scores written | "
                f"amortized latency mean={lat.mean():.0f}ms p95={np.percentile(lat, 95):.0f}ms per event "
                f"(PDD directional target <300ms; formal gate is Chunk 11)"
            )


def validate() -> None:
    """Post-run sanity checks against Cosmos."""
    c = make_gremlin_client(get_cosmos_token())
    try:
        scored = c.submit(
            "g.V().hasLabel('Account').has('gnn_risk_score').count()"
        ).all().result()[0]
        means = c.submit(
            "g.V().hasLabel('Account').has('gnn_risk_score')"
            ".group().by('is_ring_member').by(values('gnn_risk_score').mean())"
        ).all().result()
        sample = c.submit(
            "g.V().hasLabel('Account').has('gnn_risk_score').limit(3)"
            ".project('acct_id','gnn_risk_score','is_ring_member')"
            ".by(values('acct_id')).by(values('gnn_risk_score')).by(values('is_ring_member'))"
        ).all().result()
        print("\n=== COSMOS POST-INFERENCE VALIDATION ===")
        print(f"Account vertices with gnn_risk_score: {scored}")
        print(f"mean gnn_risk_score by is_ring_member: {means}")
        print(f"sample scored vertices: {sample}")
    finally:
        c.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    if args.validate:
        validate()
    else:
        InferenceService().run()
        validate()
