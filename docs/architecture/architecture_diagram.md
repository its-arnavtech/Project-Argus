# Argus — Architecture (as actually built)

This reflects the system that was really built and run, not the PDD's
original aspirational diagram. Differences from that diagram are deliberate
and documented in [`../../context.md`](../../context.md) (e.g. SARs are stored
on Cosmos vertices, not a separate Azure SQL Warehouse; the LLM is gpt-5-mini,
not GPT-4o/Claude, due to a subscription quota constraint; Tableau reads a
flattened extract, not a live Synapse connector).

Boxes marked **[dev / enterprise]** are the points where the Chunk 3 `tier`
Terraform variable (and the Chunk-12 benchmark overrides) swap scale — both
sides were actually run and measured (see the SLO scorecard in the README).

```mermaid
flowchart TB
    subgraph SRC[Data source]
        A[IEEE-CIS real transactions<br/>+ ring_injector.py synthetic rings<br/>40,289 accounts / 315 ring members]
    end

    subgraph INGEST[Rust ingestion engine · Tokio]
        B[process_stream_event<br/>SHA-256 salted PII masking<br/>IP /24 masking · velocity 60s window]
        C[EventHubSink<br/>internal batching + bounded<br/>concurrent dispatch · dead-letter file]
    end

    subgraph BROKER[Azure Event Hubs 'transactions']
        D{{Standard 1 TU / 2 partitions<br/>dev / enterprise<br/>Premium 4 PU / 32 partitions}}
    end

    subgraph INFER[Python GNN inference service]
        E[warm-start graph state<br/>from parquet snapshot]
        F[GraphSAGE forward pass<br/>PyTorch Geometric<br/>40,289 nodes / 2.68M edges]
        G[incremental scoring<br/>cached edge tensor]
    end

    subgraph GRAPH[Azure Cosmos DB · Gremlin API]
        H[(argus-graph-container<br/>single partition key<br/>1,000 RU/s dev / 10,000 enterprise<br/>gnn_risk_score + sar_draft on vertices)]
    end

    subgraph AGENT[LangGraph compliance loop]
        I[NetworkTracer<br/>live Gremlin traversals]
        J[BehavioralAnalyst<br/>real transaction metrics]
        K[SARGenerator<br/>gpt-5-mini via Azure AI Foundry]
        L{groundedness guardrail<br/>deterministic · max 1 retry}
    end

    subgraph BI[Tableau]
        M[flattened CSV/parquet extract<br/>3 PDD calculated fields]
    end

    A -->|JSONL replay| B
    B --> C
    C -->|Entra token · Data Sender| D
    D -->|consume @latest · Data Receiver| E
    E --> F --> G
    G -->|Entra token · Gremlin RBAC| H
    H -->|"cross-query: gnn_risk_score gt 0.5"| I
    I --> J --> K --> L
    L -->|grounded| H
    L -.->|ungrounded: 1 regen| K
    H -->|export_tableau_extract.py| M

    subgraph OPS[Platform · Terraform IaC]
        N[Container Apps env + argus-ingestion app<br/>system-assigned managed identity]
        O[Azure Container Registry<br/>AcrPull via managed identity]
        P[Key Vault · PII salt secret]
        Q[Log Analytics · console logs + KQL]
        R[Consumption budget alert $100]
    end

    O -.->|image pull| N
    P -.->|salt fetch| B
    N -.->|logs| Q
```

## Auth model (no static secrets anywhere)

Every hop authenticates with a Microsoft Entra token, never a key or
connection string:

- **Local dev** → the az-CLI identity (`DeveloperToolsCredential` in Rust,
  `DefaultAzureCredential` in Python) with dev-only RBAC grants.
- **Deployed service** → the Container App's system-assigned managed identity
  (Event Hubs Data Sender/Receiver, Key Vault Secrets User, AcrPull, Storage
  Blob Data Reader, Cosmos Gremlin Data Contributor).

The two paths are deliberately separate and both documented in `context.md`.

## Enterprise-tier swap points (all real, all measured in Chunk 12)

| Component | Dev | Enterprise | Mechanism |
|---|---|---|---|
| Event Hubs | Standard, 1 TU, 2 partitions | Premium, 4 PU, 32 partitions | parallel namespace (Standard→Premium is not in-place) |
| Cosmos throughput | 1,000 RU/s (free tier) | 10,000 RU/s | in-place `benchmark_cosmos_throughput` override |
| Graph loaded | 5.4K-vertex subset | full 142,395 vertices / 1.93M edges | `loader.py --full` (concurrent) |
| Container App replicas | 0–1 | temp 0–5 | `load_test_max_replicas` override |
