# Project Argus

Graph-based fraud syndicate detection and AML auditing — real-time ingestion, GNN-driven structural anomaly detection, and an autonomous compliance loop, in one pipeline.

## The Problem

Relational database structures and isolated, rule-based transaction monitoring heuristics fail to catch multi-hop structural fraud patterns — rapid circular transfers, synthetic identity rings, layered mule networks — because they evaluate transactions as independent rows rather than as a connected graph. This is a real gap at institutions running transaction monitoring today: a syndicate can pass every per-transaction rule check while its network topology is screaming fraud.

## What Argus Does

Argus ingests transaction streams in real time through a high-throughput Rust pipeline, then runs a Graph Neural Network (GraphSAGE, via PyTorch Geometric) over the resulting transaction graph to surface structural fraud patterns that per-transaction rules miss. When the GNN flags an elevated-risk node group, an autonomous multi-agent compliance loop (built on LangGraph) takes over: it traces the network, verifies behavioral risk signals, and drafts a SAR-style (Suspicious Activity Report) narrative without human intervention. Findings and graph mutations land in Cosmos DB and Azure SQL, surfaced to fraud ops through a live-connected Tableau dashboard.

## Architecture

```
[Data Source: Public API / Python Simulators]
          │
          ▼ (Raw JSON JSON-RPC / WebSocket Events)
[Rust Ingestion Driver: Tokio Async Runtime & Zero-Copy Tokenizer]
          │
          ▼ (PII Masked & Encrypted Stream via SASL/OAuth2)
[Azure Event Hubs: Dedicated Partition Clusters]
          │
          ▼ (AMQP Parallel Stream Multi-threaded Pull)
[Python Graph ML Engine: PyTorch Geometric GCN/GraphSAGE Inference]
          │
          ▼ (Anomalous Edge Matrices & Structural Embeddings)
[Agentic AI Compliance Loop: LangGraph Multi-Agent Orchestration]
    ├── Network Tracer Agent (Cosmos DB Gremlin Traversal)
    ├── Behavioral Risk Analyst Agent (Feature Deviation Check)
    └── Compliance SAR Generator Agent (Azure OpenAI GPT-4o Model)
          │
          ▼ (Gremlin Graph Mutations / Relational Metadata Updates)
[Azure Cosmos DB (Graph API) & Azure SQL Warehouse]
          │
          ▼ (Live Connector Extract Execution)
[Tableau Analytics Dashboard: Executive Fraud UI]
```

Full detail: [docs/specs/POC_Blueprint.md](docs/specs/POC_Blueprint.md) (architecture & implementation) and [docs/specs/PDD_Production_Guide.md](docs/specs/PDD_Production_Guide.md) (schema, production infra, SLOs, compliance).

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion | Rust + Tokio | Sub-millisecond, zero-copy async ingestion with no GC pauses; guarantees zero backpressure on incoming transaction APIs |
| Streaming broker | Azure Event Hubs | Immutable, partitioned ledger that decouples ingestion from the ML tier and cushions volume spikes |
| Graph ML | Python + PyTorch Geometric (GraphSAGE) | Learns structural/topological embeddings across multi-hop paths — catches synthetic identity rings and layered laundering that row-wise models miss |
| Agent orchestration | LangGraph + Azure OpenAI (GPT-4o) | Autonomous multi-agent state machine (tracer → behavioral analyst → SAR generator) that investigates flags and drafts compliance narratives |
| Graph storage | Azure Cosmos DB (Graph API / Gremlin) | Property graph natively models accounts, customers, devices, IPs, and merchants as traversable entities and edges |
| Relational metadata | Azure SQL Warehouse | Structured metadata and audit trail alongside graph mutations |
| Compute | Azure Container Apps (KEDA autoscaling) | Independently scales ingestion pods and GNN inference pods by queue depth / CPU |
| Infrastructure as Code | Terraform | All Azure tiers parameterized, so scaled-down (student-credit) and full enterprise-spec tiers are a variable change apart |
| BI / Dashboard | Tableau | Live-connected executive fraud dashboard with custom risk KPIs |
| Security | Azure Key Vault, AES-256, TLS 1.3, salted hashing | PII masked/scrubbed at the ingestion tier before it reaches the ML layer; encrypted at rest and in transit |

## Repo Structure

```
docs/
  architecture/   # architecture diagrams and design notes
  specs/          # master specs (POC blueprint, production/PDD guide) — do not deviate without flagging
data/
  raw/            # real dataset(s), gitignored
  simulated/      # synthetic ring-injection output, gitignored
  scripts/        # data acquisition & simulation scripts
ingestion/
  src/            # Rust ingestion engine (Tokio)
ml/
  training/       # GNN training pipeline
  inference/      # real-time GNN inference service
agents/           # LangGraph compliance agent loop
graph/            # Cosmos DB graph schema & loader
infra/
  modules/        # reusable Terraform modules
  envs/           # per-environment Terraform configs (dev/prod tier variables)
dashboards/       # Tableau workbooks / extract configs
tests/
  unit/
  integration/
  load/
.github/workflows/  # CI pipelines
```

## Status

This is an active build. Roadmap and progress are tracked in [context.md](context.md), not here — check it for the current chunk, next action, and session history.

## Data Sources

Real dataset: IEEE-CIS Fraud Detection dataset, layered with a synthetic ring-injection simulator to generate labeled circular-transfer and mule-ring patterns. Acquisition and simulation details land in Chunk 1 (`data/scripts/`).

## License

[MIT](LICENSE)
