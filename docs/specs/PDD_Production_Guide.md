# Fraud Syndicate Hunter & Graph-Based AML Auditor
### Product Development Document (PDD) & Production Deployment Guide
**Codename: Project Argus**

> Confidential Institutional Framework Document — Production Scaling, Verification Schema, Deployment Operations & Compliance Safeguards

---

## 1. Institutional Graph Ontology & Schema Specification

To accurately model systemic fraud patterns, the property graph must capture both entity attributes and relational interactions. The database schema below details the structural design implemented within Azure Cosmos DB's Graph API:

| Element Class | Label / Type | Property Attributes | Indexing Strategy / Partition Key |
|---|---|---|---|
| Vertex (Node) | Account | acct_id (String), risk_base (Float), balance (Decimal), open_date (Long) | Partition Key: `/acct_id`. Range indexing enabled on `risk_base`. |
| Vertex (Node) | Customer | cust_id (String), tax_hash (String), KYC_status (String), segment (String) | Partition Key: `/cust_id`. Composite hash indexing for multi-parameter lookup. |
| Vertex (Node) | Device | device_hash (String), os_type (String), hardware_signature (String) | Partition Key: `/device_hash`. Exact spatial matching index layout. |
| Vertex (Node) | IPAddress | ip_string (String), geo_country (String), proxy_flag (Boolean) | Partition Key: `/ip_string`. Spatial indexing enabled for lookup routing. |
| Vertex (Node) | Merchant | merch_id (String), mcc_code (String), geographic_base (String) | Partition Key: `/merch_id`. Range index mapped on `mcc_code`. |
| Edge (Link) | FUNDS_TRANSFER | tx_id (String), amount (Float), timestamp (Long), trace_id (String) | Out-bound relational mapping traversal, optimized natively. |
| Edge (Link) | ACCESSED_FROM | session_id (String), login_timestamp (Long) | Graph connection schema with physical directional indexing. |
| Edge (Link) | USED_DEVICE | application_version (String), binding_flag (Boolean) | Bi-directional property link mapping topology pattern context. |
| Edge (Link) | SETTLED_AT | clearing_duration_ms (Integer), terminal_id (String) | Terminal destination tracking edge optimization layer. |

---

## 2. Production Azure Infrastructure Architecture

The cloud architecture uses enterprise Azure infrastructure to guarantee horizontal scalability, high availability, and secure processing.

**Azure Event Hubs (Kafka Surface Cluster Layer):**
- Tier Specification: Premium Dedicated Cluster Configuration.
- Partition Allocation Topology: 32 explicit partitions assigned across processing availability zones.
- Retention Window Strategy: 7 days continuous storage immutable historical snapshot log buffer.
- Throughput Sizing Capacity: Scaled to consistently support up to 45 MB/s ingestion without throttling limits.

**Azure Cosmos DB (Graph API - Gremlin Backend Engine):**
- Throughput Allocation Model: Autopilot scaling mode configured between 10,000 RU/s baseline and 100,000 RU/s peak elasticity.
- Indexing Policy Configuration: Custom explicit JSON layout overriding default settings to ignore string text fields except matching structural network ID properties.
- Replication Topography: Multi-region writes enabled across primary and secondary processing centers, maintaining <10ms regional consistency.

**Azure Container Apps Execution Clustering:**
- Ingestion Pod Resource Allocation: 1 vCPU and 2 GB RAM per active instance container runtime instance.
- AI/ML Inference Pod Resource Allocation: 4 vCPU and 16 GB RAM per node with dedicated compute acceleration arrays.
- Autoscaling Trigger Logic: Monitored via KEDA targeting CPU thresholds >75% or active un-drained Event Hubs queue depths >5,000 items.

> **Note for implementation:** the resource tiers above (Premium Event Hubs cluster, 100,000 RU/s Cosmos DB autopilot, multi-region writes, 4 vCPU/16GB inference nodes) are enterprise-scale numbers written for a bank's production environment. Given Arca's Azure credits are a student/personal grant, the build should implement the **same architecture and schema** at a scaled-down tier (Standard Event Hubs, single-digit-thousand RU/s Cosmos DB, single-region, Basic/Consumption Container Apps), and document in `context.md` and the README that the Terraform is parameterized so the exact PDD-spec tiers are a one-variable change away from "true" production. This preserves the interview story ("I designed it to run at bank-scale, here's the Terraform variable that proves it") without burning the entire credit grant in a week.

---

## 3. Tableau Analytics Framework & BI Engineering Specs

The analytics tier surfaces network risk metrics through a live-connected Tableau visualization dashboard. Rather than sending raw database records to the presentation layer, the system utilizes a curated semantic structure to maintain sub-second dashboard rendering times.

**Core Calculated Fields & Structural KPI Formula Metrics:**

1. **Syndicate Cascade Index Calculation:**
   `COUNTD([tx_id]) * SUM([amount]) * AVG([velocity_score_1m]) * IF [proxy_flag] THEN 1.5 ELSE 1.0 END`
   Purpose: Highlights rapidly propagating high-value transactions moving through proxy connections.

2. **Multi-Hop Risk Dispersion Factor:**
   `SUM([gnn_risk_score]) / (1.0 + LN(AVG([hop_distance])))`
   Purpose: Discounts the downstream propagation risk score as a function of the distance from the flagged source entity.

3. **Device Sharing Density Ratio:**
   `{FIXED [device_hash]: COUNTD([acct_id])}`
   Purpose: Identifies instances where an identical hardware footprint is shared across distinct consumer profiles, a common indicator of synthetic identity rings.

The production layout connects Tableau directly to the Azure Cosmos DB data layer using custom extract refreshes scheduled every 15 minutes, with real-time alerts driven by Azure Event Hubs passing directly to the fraud analytics response desk.

---

## 4. Software Development Lifecycle (SDLC) Roadmap & Enterprise KPIs

8-week engineering path from architecture blueprint to full production deployment:

**Phase 1: Ingestion Pipeline & Schema Foundations (Sprints 1–2 / Weeks 1–4)**
- Implement core Rust driver stream decoders and parallel pipeline routing patterns.
- Provision Azure Event Hub clusters and deploy primary database tables for Cosmos DB Graph APIs.

**Phase 2: GNN Model Optimization & Agentic Orchestration (Sprints 3–4 / Weeks 5–8)**
- Train PyTorch Geometric network models on verified historical transaction datasets.
- Implement LangGraph agent state-machine flow logic and link automated prompts into Azure OpenAI instances.
- Build out the Tableau dashboard metrics and run full integration testing across the production cluster.

**Service Level Objectives (SLOs) & Production Operational Requirements:**
- Sustainable Pipeline Ingestion Throughput: minimum 10,000 events/sec baseline capability.
- End-to-End Processing Ingestion Latency: under 45ms at ingestion, under 300ms for GNN inference loops.
- Target Model False Positive Performance Ratio: strictly under 2.5% across tested synthetic transaction sets.

---

## 5. Data Privacy, Security Controls, and Regulatory Compliance

**Data Protection & Cryptographic Privacy Engineering Controls:**
- PII Sanitation Masking: PII (full names, SSNs, specific geolocation details) is cryptographically scrubbed and masked using SHA-256 salted tokens inside the Rust data tier before data is transmitted to the downstream AI model layer.
- Encryption Matrix: all data states maintain AES-256 cryptographic compliance for storage states, with active transport paths secured by TLS 1.3 tunnels, managed through credentials isolated within Azure Key Vault instances.

**Regulatory Standard Alignments & Structural Filing Compliance:**
- Bank Secrecy Act (BSA) & USA PATRIOT Act: the automated agent architecture directly satisfies suspicious reporting mandates by ensuring narrative texts explicitly fulfill FinCEN formatting parameters without omissions.
- Fair Lending Act Mitigations (Algorithmic Non-Bias): the graph feature extraction layer omits consumer demographics (age, race, zip codes), restricting GNN structural embeddings to network behavior data to prevent disparate impact and algorithmic bias.
