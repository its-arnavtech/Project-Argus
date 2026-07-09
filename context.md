# Project Argus — Context & Continuity Log

## Mission

Relational, rule-based transaction monitoring misses multi-hop structural fraud patterns — circular transfers, synthetic mule rings — because it evaluates transactions row-by-row instead of as a graph. Argus closes that gap: a Rust ingestion pipeline streams transactions in real time, a GraphSAGE GNN (PyTorch Geometric) flags structural anomalies across the transaction graph, and an autonomous LangGraph multi-agent loop investigates flagged nodes and drafts SAR-style compliance reports without human intervention, surfaced through a live-connected Tableau dashboard.

## Master Roadmap
- [x] Chunk 0 — Repo bootstrap, context.md engine, README
- [x] Chunk 1 — Real data acquisition + synthetic ring simulator
- [ ] Chunk 2 — Rust ingestion engine v1 (local)  ← IN PROGRESS
- [ ] Chunk 3 — Azure infra via Terraform
- [ ] Chunk 4 — Wire ingestion to real Azure Event Hubs
- [ ] Chunk 5 — Cosmos DB graph schema + loader
- [ ] Chunk 6 — GNN training pipeline
- [ ] Chunk 7 — Real-time GNN inference service
- [ ] Chunk 8 — LangGraph agentic compliance loop
- [ ] Chunk 9 — Tableau dashboard
- [ ] Chunk 10 — Production hardening
- [ ] Chunk 11 — Load testing & SLO validation
- [ ] Chunk 12 — Docs polish & demo packaging

## Current State
- Active chunk: 2
- Exact next action: run Chunk 2 prompt (Rust ingestion engine v1, local — Tokio async pipeline per POC_Blueprint.md section 2, reading from data/simulated/ instead of a live feed for now)

## Architectural Decisions Log
- 2026-07-09 — Scaled down PDD's enterprise Azure tiers (Premium Event Hubs,
  100K RU/s Cosmos, multi-region) to student-credit-friendly tiers,
  parameterized in Terraform so the enterprise tier is a variable change away
  — done to preserve the interview story without burning the full Azure
  grant. See docs/specs/PDD_Production_Guide.md note in section 2.
- 2026-07-09 — Repo bootstrapped, structure follows the chunk roadmap below.
- 2026-07-09 — Chunk 1: the installed `kaggle` package (v2.2.3) no longer uses
  the classic `~/.kaggle/kaggle.json` file most tutorials describe — it's
  OAuth-based (`kaggle auth login`) or a `KAGGLE_API_TOKEN` env var /
  `~/.kaggle/access_token` file. `acquire_ieee_cis.py` detects the current
  mechanism (plus the legacy kaggle.json path in case an older package
  version is installed) and prints accurate setup instructions accordingly.
  No credentials were present on this machine, so the current `data/raw/`
  snapshot is a generated bundled sample (15,000 background transactions),
  not the real ~590K-row dataset — rerun the script after configuring
  credentials to replace it.
- 2026-07-09 — Chunk 1: PDD_Production_Guide.md section 1 documents
  FUNDS_TRANSFER, ACCESSED_FROM, USED_DEVICE, and SETTLED_AT edges but never
  states their endpoint vertex types, and lists no Customer<->Account edge
  at all. Modeled as: FUNDS_TRANSFER/ACCESSED_FROM/USED_DEVICE/SETTLED_AT all
  originate from Account (to Account/IPAddress/Device/Merchant respectively);
  Customer<->Account ownership is a `cust_id` foreign key on Account rather
  than a 5th edge type, since the task scoped the edge set to exactly those
  4 labels. Chunk 5's Cosmos loader should decide whether `cust_id` becomes a
  real edge or stays a vertex property — flagged, not silently decided.
- 2026-07-09 — Chunk 1: output format is Parquet (one file per vertex/edge
  label) rather than JSON/CSV — columnar, typed, and directly loadable by
  pandas/Cosmos bulk-import tooling in Chunk 5 without a parsing step, and
  far smaller/faster than JSON for the edge-list volumes here (~15K-320K
  rows per table).
- 2026-07-09 — Chunk 1: three ring archetypes chosen to match the fraud
  patterns named in POC_Blueprint.md section 1 and the PDD's Tableau KPIs
  (Multi-Hop Risk Dispersion Factor, Device Sharing Density Ratio): circular
  transfer chains (closed FUNDS_TRANSFER loops with one entry/exit edge),
  smurfing (many-to-one fan-in under a $3,000 reporting-style threshold, then
  fan-out), and shared-device clusters (multiple accounts on one Device/IP,
  no forced transfer edges — the shared vertex itself is the signal). Device
  clusters reuse existing ring accounts 40% of the time to model syndicates
  that share infrastructure across operations, giving the GNN cross-ring
  signal to learn from.

## Environment & Resource Reference
(none provisioned yet — filled in starting Chunk 3)

## Known Issues / TODO
- `data/raw/` currently holds a generated bundled sample (15,000 background
  transactions), not the real IEEE-CIS dataset — no Kaggle credentials are
  configured on this machine yet. Run `kaggle auth login` (or set
  `KAGGLE_API_TOKEN`) and rerun `data/scripts/acquire_ieee_cis.py` to pull
  the real ~590K-row dataset, then rerun `ring_injector.py` and
  `eda_report.py` to regenerate `data/simulated/` and the EDA doc against
  real data.
- Bundled sample's device diversity is low (6 unique DeviceInfo strings) —
  a known limitation of the fallback generator, not the real dataset.
- No Rust/ML/agent code yet — this and Chunk 0 are data/scaffolding only.

## File Map
- `docs/` — `specs/` holds the two master specs (POC_Blueprint.md, PDD_Production_Guide.md); `architecture/` scaffolded, empty
- `data/` — `scripts/` holds `graph_schema.py` (shared vertex/edge schema + real-data derivation), `acquire_ieee_cis.py` (Kaggle acquisition + bundled-sample fallback), `ring_injector.py` (synthetic ring injection), `eda_report.py` (validation/EDA); `raw/` and `simulated/` are gitignored but currently populated (bundled sample + 45 injected rings) — regenerate anytime via the three scripts in order
- `ingestion/` — `src/` scaffolded, empty (Rust ingestion engine lands Chunk 2)
- `ml/` — `training/`, `inference/` scaffolded, empty
- `agents/` — scaffolded, empty (LangGraph compliance loop lands Chunk 8)
- `graph/` — scaffolded, empty (Cosmos DB schema + loader lands Chunk 5)
- `infra/` — `modules/`, `envs/` scaffolded, empty (Terraform lands Chunk 3)
- `dashboards/` — scaffolded, empty (Tableau lands Chunk 9)
- `tests/` — `unit/`, `integration/`, `load/` scaffolded, empty
- `.github/workflows/` — scaffolded, empty (CI lands later chunks)
- Root — `README.md`, `LICENSE` (MIT), `.gitignore`, `context.md` (this file)

## Session Log
- 2026-07-09 — Claude Code — Chunk 0 — bootstrapped repo structure, .gitignore, LICENSE, README, context.md.
- 2026-07-09 — Claude Code — Chunk 0 fixes — renamed Context.md to context.md
  (git mv, history preserved), fixed hardcoded references, confirmed commit
  identity (arnavk174@gmail.com) correct, confirmed zero ArgusMesh residue
  repo-wide.
- 2026-07-09 — Claude Code — Chunk 1 — real data acquisition + synthetic ring
  injection. No Kaggle credentials on this machine, so acquisition fell back
  to a bundled sample (15,000 background transactions, seed 42). Derived
  14,912 accounts from the transaction data, injected 45 synthetic rings
  (20 circular, avg size 6.05, avg hop distance 2.15; 15 smurfing, avg size
  11.60, avg hop distance 1.82; 10 device-cluster, avg size 4.20) totaling
  315 ring-member accounts (2.11% of all accounts). Full EDA in
  docs/architecture/chunk1_data_eda_summary.md. Files: data/scripts/
  graph_schema.py, acquire_ieee_cis.py, ring_injector.py, eda_report.py;
  data/requirements.txt; README Data Sources section updated with real
  numbers.

Last updated: 2026-07-09 by Claude Code
