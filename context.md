# Project Argus — Context & Continuity Log

## Mission

Relational, rule-based transaction monitoring misses multi-hop structural fraud patterns — circular transfers, synthetic mule rings — because it evaluates transactions row-by-row instead of as a graph. Argus closes that gap: a Rust ingestion pipeline streams transactions in real time, a GraphSAGE GNN (PyTorch Geometric) flags structural anomalies across the transaction graph, and an autonomous LangGraph multi-agent loop investigates flagged nodes and drafts SAR-style compliance reports without human intervention, surfaced through a live-connected Tableau dashboard.

## Master Roadmap
- [ ] Chunk 0 — Repo bootstrap, context.md engine, README  ← IN PROGRESS
- [ ] Chunk 1 — Real data acquisition + synthetic ring simulator
- [ ] Chunk 2 — Rust ingestion engine v1 (local)
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
- Active chunk: 0
- Exact next action: run Chunk 1 prompt (data acquisition + synthetic ring simulation)

## Architectural Decisions Log
- 2026-07-09 — Scaled down PDD's enterprise Azure tiers (Premium Event Hubs,
  100K RU/s Cosmos, multi-region) to student-credit-friendly tiers,
  parameterized in Terraform so the enterprise tier is a variable change away
  — done to preserve the interview story without burning the full Azure
  grant. See docs/specs/PDD_Production_Guide.md note in section 2.
- 2026-07-09 — Repo bootstrapped, structure follows the chunk roadmap below.

## Environment & Resource Reference
(none provisioned yet — filled in starting Chunk 3)

## Known Issues / TODO
- Nothing implemented yet beyond scaffolding.

## File Map
- `docs/` — `specs/` holds the two master specs (POC_Blueprint.md, PDD_Production_Guide.md); `architecture/` scaffolded, empty
- `data/` — `raw/`, `simulated/`, `scripts/` scaffolded, empty (raw/simulated are gitignored)
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

Last updated: 2026-07-09 by Claude Code
