# Project Argus

Graph-based fraud-syndicate detection and AML auditing — real-time Rust
ingestion, a GraphSAGE GNN for structural anomaly detection, and an autonomous
LangGraph compliance loop, in one pipeline on Azure. Built end to end, run
against real infrastructure, and benchmarked honestly.

> **Deep dive:** [`docs/ENGINEERING_JOURNEY.md`](docs/ENGINEERING_JOURNEY.md) —
> a curated record of every assumption that turned out wrong, bug found, and
> spec conflict resolved while building this. It's the most useful thing in
> the repo if you want to know what actually happened.

## The Problem

Relational databases and rule-based transaction monitoring miss multi-hop
structural fraud — circular transfers, synthetic identity rings, layered mule
networks — because they evaluate transactions as independent rows rather than
as a connected graph. A syndicate can pass every per-transaction rule while its
network topology screams fraud.

## What Argus Does

Argus ingests transactions through a high-throughput Rust pipeline, runs a
Graph Neural Network (GraphSAGE, PyTorch Geometric) over the transaction graph
to surface structural patterns per-transaction rules miss, and when the GNN
flags an elevated-risk account, hands off to an autonomous LangGraph
multi-agent loop that traces the network, checks behavioural signals, and
drafts a SAR-style (Suspicious Activity Report) narrative — with a
deterministic groundedness guardrail so the LLM can't invent facts. Scores and
SAR drafts land on the Cosmos DB graph and surface to fraud ops through a
Tableau dashboard.

## Architecture

Full diagram (the **as-built** system, not the aspirational spec):
[`docs/architecture/architecture_diagram.md`](docs/architecture/architecture_diagram.md).

```
IEEE-CIS real data + synthetic ring injector (Python)
        │  JSONL
        ▼
Rust ingestion engine (Tokio) ── SHA-256 salted PII masking, 60s velocity,
        │                          internal batching, dead-letter, Key Vault salt
        ▼  Entra token (no keys)
Azure Event Hubs "transactions"  [Standard 1 TU/2 part  ↔  Premium 4 PU/32 part]
        │
        ▼  consume @latest
Python GNN inference (PyTorch Geometric GraphSAGE) ── warm-start, incremental scoring
        │
        ▼  Gremlin RBAC token
Azure Cosmos DB (Gremlin)  [1,000 RU/s  ↔  10,000 RU/s]  ── gnn_risk_score + sar_draft on vertices
        │  cross-query: gnn_risk_score > 0.5
        ▼
LangGraph compliance loop ── NetworkTracer → BehavioralAnalyst → SARGenerator (gpt-5-mini)
        │                     → groundedness guardrail (deterministic, max 1 regen)
        ▼
Tableau (flattened extract, 3 PDD calculated fields)
```

Master specs: [POC_Blueprint.md](docs/specs/POC_Blueprint.md) and
[PDD_Production_Guide.md](docs/specs/PDD_Production_Guide.md). Where the build
deviates from them, it's flagged in `context.md`.

## Results — SLO scorecard (real measured numbers)

Measured against the PDD's four SLOs, at both the budget-capped **dev** tier
and a bounded **enterprise** benchmark (Premium Event Hubs 4 PU / 32 partitions,
Cosmos 10,000 RU/s, full 40,289-account graph). **Misses are marked as misses.**

| PDD SLO | Dev-tier | Enterprise-tier | Verdict |
|---|---|---|---|
| Ingestion throughput ≥ 10,000 evt/s | ~1,000/s (1-TU cap); 15,151/s @ 12 TU | **21,037 evt/s** | ✅ **PASS** (enterprise) |
| Ingestion latency < 45 ms | batch RTT ~170 ms | batch RTT ~305 ms; paced arrival 813 ms | ❌ **FAIL** (both tiers) |
| GNN inference latency < 300 ms | 3.64 s/batch | **2.21 s/batch** (was 21.96 s pre-fix) | ❌ **FAIL** (both tiers) |
| False-positive rate < 2.5 % | 0.000 % | **0.000 %** (full 40,289-node graph) | ✅ **PASS** (both tiers) |

GNN model quality (full graph, all 315 known ring labels): precision 1.000,
recall 0.9016, FP-rate 0.000. See [Known Limitations](#known-limitations) for
why the latency SLOs fail and why the model numbers must be read with care.

## Tech Stack

| Layer | Technology | Notes (as built) |
|---|---|---|
| Ingestion | Rust + Tokio | Async engine; internal Event Hubs batching → 15,000–21,000 evt/s measured. (Not "sub-millisecond" — latency SLO not met; see scorecard.) |
| Streaming broker | Azure Event Hubs | Standard 1 TU / 2 partitions (dev) ↔ Premium 4 PU / 32 partitions (enterprise). Entra-token auth, no connection strings. |
| Graph ML | Python + PyTorch Geometric (GraphSAGE) | 2-layer, 40,289 nodes / 2.68M edges, full-graph forward per batch (CPU; no GPU available). |
| Agent orchestration | LangGraph + **Azure OpenAI gpt-5-mini** (via AI Foundry) | Tracer → analyst → SAR generator + deterministic groundedness guardrail. gpt-5-mini, **not** Claude/GPT-4o — subscription quota (see limitations). |
| Graph storage | Azure Cosmos DB (Gremlin API) | Single container, single partition key; scores + SAR drafts stored on Account vertices. |
| Metadata / SAR store | *Cosmos vertices* | The PDD's separate Azure SQL Warehouse was **not** provisioned — no budget justification; SARs live on the graph. |
| Compute | Azure Container Apps (managed identity, ACR pull) | Ingestion deployed with a KEDA scale rule. (Known issue: the rule scales the producer, not the consumer — see journey doc.) |
| IaC | Terraform (azurerm + azapi) | Dev/enterprise tiers behind one `tier` variable + benchmark overrides. |
| BI | Tableau | Flattened CSV/parquet extract (Gremlin + transaction computation), not a live Synapse connector. |
| Security | Key Vault, salted SHA-256, **TLS 1.2** | PII masked at ingestion. TLS **1.2** is the enforceable floor on Event Hubs/Cosmos/Key Vault — the PDD's "TLS 1.3" is not independently configurable; reported honestly in `docs/architecture/observability_queries.md`. |

## Repo Structure

```
docs/
  ENGINEERING_JOURNEY.md   # curated assumptions-corrected / bugs-found record
  architecture/            # as-built diagram, design notes, final inventory, example SAR
  specs/                   # master specs (POC blueprint, PDD guide)
data/            scripts/ (acquisition + ring injection); raw/ & simulated/ gitignored
ingestion/       Rust engine: src/{lib,main,event_hub_sink}.rs, Dockerfile, 11 tests
ml/              training/ (GNN + full-scale eval), inference/ (real-time service + batch scorer), artifacts/
agents/          LangGraph compliance loop + orchestrator
graph/           Cosmos Gremlin loader (subset + --full)
infra/           Terraform: modules/ + envs/dev (tier-switchable)
dashboards/      Tableau workbook + extract exporter
tests/           unit/ (26 pytest cases), load/ (marker isolation); integration/ scaffolded
.github/workflows/  CI: test-gated image build/push
```

**Testing scope, honestly:** the Rust engine has 11 unit/integration tests
(PII masking, velocity window, dead-letter, throughput). The Python side has
26 pytest unit tests covering the *pure, network-free* logic — hashing,
row sanitisation, deterministic fallbacks, ring-injection invariants. The
cloud-touching paths (Gremlin loads, Event Hubs, inference against live
Cosmos) are exercised by the validation scripts and the load-test harness, not
by mocked unit tests — so "26 tests" is real but targeted, not full coverage.

## Status

**Complete (v1.0).** Built across 12 chunks plus several fix sessions; the full
history — every decision, correction, and honest miss — is in
[`context.md`](context.md) at the repo root. That file is the project's
continuity log: it's what let the build proceed coherently across many separate
sessions, and it's worth reading if you want the real story rather than the
polished one.

**The live Azure deployment is torn down** as of 2026-07-12 (the credit grant
expired). A final snapshot of what was running is in
[`docs/architecture/final_resource_inventory.md`](docs/architecture/final_resource_inventory.md).
Everything is redeployable from `infra/` with fresh credentials:
`cd infra/envs/dev && terraform init && terraform apply` (set
`subscription_id` and `alert_email` in a `terraform.tfvars`), then load the
graph with `python graph/loader.py`.

## Known Limitations

Stated plainly, because a portfolio piece that hides these is worth less than
one that names them:

- **Synthetic ring labels, not real fraud.** No public dataset labels multi-hop
  syndicates. IEEE-CIS labels per-transaction card fraud; the ring/mule
  structures are injected by `ring_injector.py`. The model's near-perfect
  precision/FP-rate reflect rings that are **structurally conspicuous by
  construction** — they validate the *pipeline*, not real-world fraud
  performance.
- **LLM is gpt-5-mini, not Claude or full GPT-5.** Every flagship model (all
  Claude tiers, full gpt-5/5.1/5.2/5.4/5.5/5.6) has a hard 0-TPM quota on this
  credit-grant subscription. gpt-5-mini is the accepted, permanent
  configuration — not a temporary placeholder.
- **Two SLOs are not met, at any tier.** Ingestion latency (<45 ms) fails
  because of the batch-pipeline depth that gets the throughput; inference
  latency (<300 ms) fails because the full-graph GNN forward pass alone is
  ~0.68 s on CPU with no GPU available. Both are understood, not mysterious:
  root causes and the real (unimplemented) levers are in
  [`docs/ENGINEERING_JOURNEY.md`](docs/ENGINEERING_JOURNEY.md) and `context.md`.
- **One open design issue:** the KEDA autoscale rule scales the ingestion
  *producer* on consumer-side backlog (confirmed with live traffic). The
  correct fix — a containerised inference consumer scaling on its own
  checkpointed lag — needs new infrastructure and is documented rather than
  rushed.
- **Graph state after the enterprise benchmark** holds the full 40,289-account
  graph but was only partially GNN-scored before teardown; the 315 ring members
  were re-scored and a fresh 3-draft SAR batch generated (see
  [`docs/architecture/example_sar_draft.md`](docs/architecture/example_sar_draft.md))
  so the repo has a real, current example artifact.

## Data Sources

Real dataset: [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection)
(~590K labeled transactions), pulled via `data/scripts/acquire_ieee_cis.py`.
`ring_injector.py` builds an account universe from it (matching the PDD schema)
and injects three labeled ring archetypes: circular chains, smurfing
fan-in/fan-out, and shared-device clusters. Current run: **40,289 accounts**
(315 ring members, 0.78%), **45 injected rings**. Full stats:
[docs/architecture/chunk1_data_eda_summary.md](docs/architecture/chunk1_data_eda_summary.md).

## License

[MIT](LICENSE)
