# Project Argus — Context & Continuity Log

## Mission

Relational, rule-based transaction monitoring misses multi-hop structural fraud patterns — circular transfers, synthetic mule rings — because it evaluates transactions row-by-row instead of as a graph. Argus closes that gap: a Rust ingestion pipeline streams transactions in real time, a GraphSAGE GNN (PyTorch Geometric) flags structural anomalies across the transaction graph, and an autonomous LangGraph multi-agent loop investigates flagged nodes and drafts SAR-style compliance reports without human intervention, surfaced through a live-connected Tableau dashboard.

## Master Roadmap
- [x] Chunk 0 — Repo bootstrap, context.md engine, README
- [x] Chunk 1 — Real data acquisition + synthetic ring simulator
- [x] Chunk 2 — Rust ingestion engine v1 (local)
- [ ] Chunk 3 — Azure infra via Terraform  ← IN PROGRESS
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
- Active chunk: 3
- Exact next action: run Chunk 3 prompt (Azure infra via Terraform — parameterized tiers per the Chunk 0 decision to scale down PDD's enterprise specs)

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
- 2026-07-09 — Chunk 2: `ingestion/` restructured as a real Cargo crate
  (`src/lib.rs` core engine + `src/main.rs` thin binary), replacing
  POC_Blueprint.md section 2's single-file mock. Core addition: an async
  `Sink` trait (`LocalFileSink`, `StdoutSink` implemented now) that
  `IngestionEngine` depends on exclusively — never on a concrete transport.
  Chunk 4 adds `EventHubSink` wrapping the real `azure_messaging_eventhubs`
  crate as a third implementation with zero changes to engine logic in
  `lib.rs`, because the engine only ever calls `sink.send()` through the
  trait object (`Arc<dyn Sink>`), never anything Event-Hubs-specific.
- 2026-07-09 — Chunk 2 SECURITY CORRECTION: POC_Blueprint.md section 2's
  example hashes `device_id` with MD5, which is cryptographically broken.
  The Rust engine uses `sha2` (SHA-256) salted with an env var
  (`ARGUS_PII_SALT`, placeholder — becomes an Azure Key Vault secret in
  Chunk 10), per PDD_Production_Guide.md section 5 ("SHA-256 salted
  tokens"), which is the authoritative spec for security controls over the
  POC snippet's illustrative shortcut.
- 2026-07-09 — Chunk 2: Rust doesn't get a parquet dependency this early.
  `data/scripts/export_ingestion_jsonl.py` mirrors Chunk 1's
  edges_funds_transfer table (joined with a device/IP per source account)
  to `data/simulated/funds_transfer_raw.jsonl`, matching
  `RawTransaction`'s exact field shape. Circular/smurfing ring accounts
  have no USED_DEVICE/ACCESSED_FROM edges of their own (only
  device_cluster rings do — see Chunk 1's ring_injector.py), so those get
  a deterministic per-account fallback device/IP rather than going back to
  backfill Chunk 1.
- 2026-07-09 — Chunk 2 fix (incidental, while re-verifying Chunk 1's
  acquisition path with now-working Kaggle credentials): the installed
  `kaggle` package actually stores OAuth credentials at
  `~/.kaggle/credentials.json`, a third path beyond the two
  `acquire_ieee_cis.py` already checked (`access_token` file,
  `KAGGLE_API_TOKEN` env var). Fixed detection to include it. Also
  switched the real-download path from `competition_download_files`
  (pulls all 5 competition files, ~1.3GB) to per-file
  `competition_download_file` calls for just train_transaction.csv +
  train_identity.csv (~710MB), since we never use the unlabeled test
  files.

## Environment & Resource Reference
(none provisioned yet — filled in starting Chunk 3)

## Known Issues / TODO
- `data/raw/` still holds the generated bundled sample (15,000 background
  transactions), not the real IEEE-CIS dataset. Update: Kaggle credentials
  now work (`python -m kaggle` is authenticated — note the `kaggle` command
  itself isn't on PATH yet, use `python -m kaggle ...`), but the real
  download 403s with "Forbidden" because this Kaggle account hasn't
  accepted the ieee-fraud-detection competition rules yet. That's a
  one-time manual step only doable in a browser:
  https://www.kaggle.com/competitions/ieee-fraud-detection/rules — after
  accepting, rerun `data/scripts/acquire_ieee_cis.py`, then
  `ring_injector.py` and `eda_report.py` to regenerate `data/simulated/`
  and the ingestion JSONL export against real data.
- Bundled sample's device diversity is low (6 unique DeviceInfo strings) —
  a known limitation of the fallback generator, not the real dataset.
- Ingestion throughput (LocalFileSink, this dev machine): ~11,300
  events/sec release / ~7,300 events/sec debug — directional toward the
  PDD's 15,000 events/sec target, not yet at it. Expected to improve with
  the real `azure_messaging_eventhubs` async client in Chunk 4 and formal
  load testing in Chunk 11; not a gate for this chunk.
- No ML/agent code yet.

## File Map
- `docs/` — `specs/` holds the two master specs (POC_Blueprint.md, PDD_Production_Guide.md); `architecture/` scaffolded, empty
- `data/` — `scripts/` holds `graph_schema.py` (shared vertex/edge schema + real-data derivation), `acquire_ieee_cis.py` (Kaggle acquisition + bundled-sample fallback), `ring_injector.py` (synthetic ring injection), `eda_report.py` (validation/EDA); `raw/` and `simulated/` are gitignored but currently populated (bundled sample + 45 injected rings) — regenerate anytime via the three scripts in order
- `ingestion/` — real Cargo crate: `src/lib.rs` (RawTransaction/EnrichedTransaction, `Sink` trait, `LocalFileSink`/`StdoutSink`, SHA-256 PII masking, 8 passing unit/integration tests), `src/main.rs` (binary entrypoint reading `data/simulated/funds_transfer_raw.jsonl`); Chunk 4 adds `EventHubSink`
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
- 2026-07-09 — Claude Code — Chunk 2 — real Rust ingestion engine.
  Restructured ingestion/ as a Cargo crate (lib.rs + main.rs), added an
  async Sink trait (LocalFileSink, StdoutSink), fixed the POC's MD5 device
  hash to salted SHA-256 (ARGUS_PII_SALT env var placeholder), wired real
  input via data/scripts/export_ingestion_jsonl.py (15,320 rows). 8 unit
  tests pass (PII masking, hash determinism/salt-dependence, enrichment
  field mapping, throughput). Measured throughput (LocalFileSink): ~11,340
  events/sec release build, ~7,259 events/sec debug build (20,000
  synthetic events) — directional toward the PDD's 15,000/sec target, not
  a hard gate yet. End-to-end run against the real 15,320-row Chunk 1
  corpus: drained in 1.7s. Incidental: fixed acquire_ieee_cis.py's Kaggle
  credential detection (missed ~/.kaggle/credentials.json) and download
  scope (was pulling ~1.3GB of unneeded test files); real download still
  blocked by a 403 because this Kaggle account hasn't accepted the
  competition rules yet (manual browser step, logged above).

Last updated: 2026-07-09 by Claude Code
