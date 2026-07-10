# Project Argus — Context & Continuity Log

## Mission

Relational, rule-based transaction monitoring misses multi-hop structural fraud patterns — circular transfers, synthetic mule rings — because it evaluates transactions row-by-row instead of as a graph. Argus closes that gap: a Rust ingestion pipeline streams transactions in real time, a GraphSAGE GNN (PyTorch Geometric) flags structural anomalies across the transaction graph, and an autonomous LangGraph multi-agent loop investigates flagged nodes and drafts SAR-style compliance reports without human intervention, surfaced through a live-connected Tableau dashboard.

## Master Roadmap
- [x] Chunk 0 — Repo bootstrap, context.md engine, README
- [x] Chunk 1 — Real data acquisition + synthetic ring simulator
- [x] Chunk 2 — Rust ingestion engine v1 (local)
- [x] Chunk 3 — Azure infra via Terraform
- [x] Chunk 4 — Wire ingestion to real Azure Event Hubs
- [x] Chunk 5 — Cosmos DB graph schema + loader
- [x] Chunk 6 — GNN training pipeline
- [ ] Chunk 7 — Real-time GNN inference service  ← IN PROGRESS
- [ ] Chunk 8 — LangGraph agentic compliance loop
- [ ] Chunk 9 — Tableau dashboard
- [ ] Chunk 10 — Production hardening
- [ ] Chunk 11 — Load testing & SLO validation
- [ ] Chunk 12 — Docs polish & demo packaging

## Current State
- Active chunk: 7
- Exact next action: run the real-time inference service end-to-end
  (ml/inference/ — service code written: prepare_validation_events.py →
  Rust binary sends events → inference_service.py consumes, scores,
  writes gnn_risk_score to Cosmos → post-run sanity validation).

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
- 2026-07-09 — Chunk 3: real Azure infra provisioned via Terraform, dev
  tier, into "Azure subscription 1" (REDACTED-SUBSCRIPTION-ID,
  confirmed with the user as the ~$75 credit grant subscription before
  touching anything). All resources locked to East US 2: Claude Opus 4.8
  on Azure AI Foundry (needed in Chunk 8) is only Hosted-on-Azure in East
  US 2 / Sweden Central, and co-locating everything now avoids cross-
  region egress charges later.
- 2026-07-09 — Chunk 3: added a top-level `tier` variable
  (infra/envs/dev/variables.tf) switching Event Hubs SKU/partitions/
  retention and Cosmos throughput between "dev" (this build, budget-
  capped) and "enterprise" (PDD_Production_Guide.md section 2's literal
  bank-scale spec). Multi-region writes and Cosmos autopilot are captured
  in the tier map for documentation but not yet wired into modules/
  cosmos_db (single-region/manual-throughput only) -- real module
  behavior changes, deferred to whichever chunk actually runs at
  "enterprise" tier rather than built speculatively now. This is the
  variable that makes the scale-down (logged in the first entry above)
  concrete and reversible in one change.
- 2026-07-09 — Chunk 3: added an azurerm_consumption_budget_resource_group
  scoped to rg-argus-dev, $75/month, alert notifications at 50/75/90% of
  budget to redacted@example.com -- a hard ceiling on real spend
  while iterating, not just documentation of intent.
- 2026-07-09 — Chunk 3: verified all Terraform resource schemas against
  the live hashicorp/azurerm v4.80.0 provider docs (fetched from the
  provider's GitHub source, since the Terraform Registry site itself is
  JS-rendered and unfetchable) rather than relying on training-data
  memory, given real budget was on the line. Caught two v4 renames that
  would have failed apply: Cosmos free tier is `free_tier_enabled` (not
  `enable_free_tier`), Key Vault RBAC is `rbac_authorization_enabled` (not
  `enable_rbac_authorization`).
- 2026-07-09 — Chunk 3: first `terraform apply` failed on the Container
  Apps Environment with `MissingSubscriptionRegistration` for
  `Microsoft.App` -- this subscription had never used Container Apps
  before. Registered the resource provider (one-time, non-destructive)
  and re-planned/re-applied; the second plan showed exactly the one
  remaining resource, reviewed and approved before applying, consistent
  with the "always show plan, never blind-apply" rule for this chunk.
- 2026-07-09 — Incidental (discovered via `git status` at the start of
  this session, not something this session did): between the Chunk 2 and
  Chunk 3 sessions, the real IEEE-CIS dataset was pulled (competition
  rules evidently accepted) and Chunk 1's ring_injector.py/eda_report.py
  were rerun against it -- 590,540 real transactions, 40,289 accounts (315
  ring members, 0.78%). This resolved the "bundled sample, not real data"
  Known Issue below. It left one inconsistency: Chunk 2's
  funds_transfer_raw.jsonl export predated that refresh and still
  reflected the old bundled-sample data. Reran
  data/scripts/export_ingestion_jsonl.py (590,860 rows) and re-verified
  the Rust ingestion engine against the full real corpus: 590,860 events
  drained in ~64s (~9,200 events/sec, LocalFileSink release build) with no
  errors -- consistent with Chunk 2's synthetic-load throughput numbers.
  Cosmos DB throughput bumped 400→1000 RU/s post-apply, still $0 under
  free tier -- 1000 RU/s is the free tier's max and no container/graph
  exists yet to split it with (Chunk 5), so this just uses headroom that
  was already free rather than leaving it unused ahead of loading the
  real 590K-account graph.
- 2026-07-09 — Chunk 4: `EventHubSink` authenticates via Azure AD
  (`DeveloperToolsCredential`, which tries `AzureCliCredential` first) with
  RBAC role assignments scoped to the namespace, not a static connection
  string. No secret to leak, rotate, or accidentally commit -- this is the
  same reasoning Key Vault (Chunk 3) and PII salt (Chunk 10) already follow,
  now extended to the transport layer. The dev-only RBAC grants (below) are
  an explicit bridge, not the production path: Chunk 10 points the same
  `Sink` trait at the Container App's managed identity instead, with zero
  changes to `EventHubSink` itself, since both credential types implement
  the same `azure_core::credentials::TokenCredential` trait.
- 2026-07-09 — Chunk 4: confirmed azure_messaging_eventhubs v0.15.0's real
  API before writing any code, per the task's own instruction -- docs.rs's
  rendered pages didn't resolve via automated fetch (redirect-only
  content), so verified against the crate's own tests/examples in the
  azure-sdk-for-rust GitHub repo instead. Two things differed from
  training-data expectations: (1) the credential type is
  `DeveloperToolsCredential` (tries `AzureCliCredential` then
  `AzureDeveloperCliCredential`), not `DefaultAzureCredential` -- that name
  doesn't exist in this SDK generation. (2) `ProducerClient::builder().open()`
  takes `eventhub: &str`, but `ConsumerClient::builder().open()` takes
  `eventhub: String` (owned) -- an inconsistency between the two builders,
  not a typo on our part; confirmed directly against both functions' source.
  Confirmed API otherwise: `ProducerClient::send_event(impl Into<EventData>,
  Option<SendEventOptions>)`, `ConsumerClient::open_receiver_on_partition(...)`
  + `EventReceiver::stream_events()` (a `futures::Stream`).
- 2026-07-09 — Chunk 4: the dev-only RBAC bridge needed both "Azure Event
  Hubs Data Sender" AND "Azure Event Hubs Data Receiver" roles, not just
  Sender -- Event Hubs' AMQP claim model separates "Send" and "Listen"
  claims, and Chunk 4's own validation step (read events back to confirm
  delivery) needs Listen. First apply (Sender only) let 3,000 events send
  successfully but failed to read them back with `UnauthorizedAccess`
  ('Listen' claims required); added the Receiver role in a second
  reviewed/approved plan+apply rather than silently over-granting Owner
  upfront.
- 2026-07-09 — Chunk 4: retry is layered, not singular. The SDK's own
  `RetryOptions` (passed via `.with_retry_options(...)` on the producer
  builder) governs internal AMQP link/connection recovery; `EventHubSink`
  additionally wraps its own `send_event` call in a small explicit retry
  (3 attempts, exponential backoff starting at 200ms) at the Sink layer,
  since the task asked for retry "around the send call" specifically, and
  the SDK's internal retry helpers (`recover_with_backoff`) are
  `pub(crate)` -- not something a crate consumer can hook into directly.
- 2026-07-09 — Chunk 5 prep: resolved the Gremlin partition key model.
  Cosmos DB Gremlin graphs are one container with one partition key --
  not one container per vertex label, despite PDD_Production_Guide.md
  section 1's table having a per-row "Partition Key" column. That table is
  reinterpreted as indexing policy guidance instead (applied via
  `index_policy`'s `composite_index` blocks on `argus-graph-container`,
  covering risk_base, mcc_code, hop_distance, gnn_risk_score). The
  container uses a single low-cardinality shared key (`/partitionKey`),
  not a high-cardinality key like `acct_id`: data volume (~40K accounts,
  ~590K edges) is well under one physical partition's capacity, every
  fraud-detection traversal is inherently cross-entity (multi-hop tracing,
  shared-device clustering), and a shared key keeps every traversal
  same-partition rather than fanning out cross-partition on nearly every
  query. Trades away horizontal write scaling we don't need at this scale
  for query performance we do need. Full reasoning:
  docs/architecture/partition_key_strategy.md. The container has no
  `throughput`/`autoscale_settings` block -- it draws from `argus-graph`
  database's existing 1000 RU/s shared pool. Confirmed against Microsoft
  Learn docs (not assumed): free tier covers a shared-throughput database
  up to 25 containers at $0; this is the 1st of 25, so it's still $0.
- 2026-07-09 — Chunk 5 prep: added a 5th edge, OWNS (Customer -> Account),
  on top of PDD_Production_Guide.md section 1's literal 4-edge list
  (FUNDS_TRANSFER, ACCESSED_FROM, USED_DEVICE, SETTLED_AT) -- a deliberate,
  flagged deviation, not an oversight. This resolves the question Chunk 1
  flagged and deferred (whether Account's `cust_id` foreign key should
  become a real edge). Kept `cust_id` on Account too (harmless, useful for
  quick pandas joins outside Gremlin). Updated `data/scripts/graph_schema.py`
  (`derive_account_universe` now returns an `owns` table; `GraphTables`
  gained an `owns` field) and `ring_injector.py` (passes `tables.owns`
  through unchanged, writes `edges_owns.parquet`). Ring-injected accounts
  do NOT get their own OWNS edges yet -- out of scope for this narrow fix;
  their Customer records exist but aren't edge-linked. Data not
  regenerated in this session (this is schema/infra prep, not a pipeline
  rerun); takes effect next time `ring_injector.py` actually runs.
- 2026-07-09 — Chunk 5: gremlinpython API constraints confirmed against
  Microsoft Learn docs before writing the loader (same discipline as
  Chunk 4's Rust crate checks): Cosmos Gremlin supports NO bytecode (string
  queries via client.submit() + bindings only), requires the GraphSON v2
  serializer (GraphSONSerializersV2d0 -- v3 unsupported), rejects null
  property values (loader skips None/NaN per row), and has NO native AAD
  data-plane auth -- username is /dbs/{db}/colls/{graph}, password is the
  account key. The key is fetched at runtime via `az cosmosdb keys list`
  (or ARGUS_COSMOS_KEY env var), never hardcoded or committed. Note:
  Microsoft's compatibility table recommends the 3.4.13 driver and flags
  3.5/3.6 issues; gremlinpython 3.8.1 was verified working against the
  live container (connectivity, writes, traversals) before the bulk load.
- 2026-07-09 — Chunk 5: loader loads a representative SUBSET (~5.4K
  vertices / ~6.9K edges), not the full ~590K corpus: all 315 ring
  accounts + their ring-edge counterparties + 1,500 sampled legit accounts.
  Ring members keep ALL their ACCESSED_FROM/USED_DEVICE/SETTLED_AT edges
  (shared-device/IP structure IS the fraud signal); legit accounts keep one
  of each, enough to be realistically connected without blowing the shared
  1000 RU/s pool. FUNDS_TRANSFER edges require both endpoints selected.
  Full-scale load is Chunk 11's job. Edges carry no partitionKey property
  themselves -- Cosmos co-locates an edge with its source vertex.
- 2026-07-09 — Chunk 6: node features are exactly POC_Blueprint.md section
  3's four (tx_count, unique_counterparties, value_variance,
  device_assoc_count), computed as real per-account aggregates over the
  full 590K-row corpus. LEAKAGE GUARD: `risk_base` is deliberately NOT a
  feature -- graph_schema.py derives it partly from the isFraud label, so
  using it would leak target signal; all four features are purely
  behavioral. Message-passing graph is homogeneous Account-only:
  FUNDS_TRANSFER edges both directions, plus pairwise shared-Device and
  shared-IP edges capped at devices/IPs shared by <=10 accounts -- IEEE-CIS
  DeviceInfo strings are coarse ("Windows" = one hash shared by thousands;
  pairwise-connecting those would add millions of meaningless edges, and a
  device shared by 5,000 accounts is an OS string, not a hardware
  fingerprint). Result: 40,289 nodes, 2,682,934 directed edges.
- 2026-07-09 — Chunk 6: train/val/test split holds out ENTIRE RINGS --
  rings sharing members (device_cluster reuse) are first merged into
  components via union-find, and components get split 60/20/20 (205/59/51
  positives). Splitting individual nodes within a ring would leak
  structural signal from train-set neighbors into test predictions via
  GraphSAGE aggregation. Class imbalance (~0.78% positive): inverse-
  frequency weighted NLL loss (weight ~117:1) -- chosen over resampling
  because transductive full-graph training has no natural minibatch to
  resample, and discarding legit nodes would starve the negative class of
  the diversity the FP-rate target depends on. Model selection by best
  val PR-AUC with early stopping (best at epoch 5; training past ~epoch 20
  degraded val PR-AUC).
- 2026-07-09 — Chunk 6 environment fixes worth knowing about: (1) global
  pip torch install fails on this machine -- Microsoft Store Python's deep
  site-packages path + torch 2.13's nested license dirs exceed Windows
  MAX_PATH. ML stack lives in `.venv` at the repo root
  (--system-site-packages); use `.venv/Scripts/python.exe` for anything
  importing torch. The half-installed global torch was left broken
  ("torch None" in pip list) -- harmless, shadowed by the venv copy.
  (2) MLflow 3.14 hard-deprecated the ./mlruns filesystem backend --
  tracking uses sqlite:///ml/training/mlflow.db (gitignored). (3)
  mlflow.pytorch.log_model defaults to 'pt2' traced-graph serialization
  requiring an input example -- brittle for dynamic-graph GNNs, so the
  model is logged as plain artifacts (state_dict + config + feature
  stats), which is what Chunk 7 loads anyway.

## Environment & Resource Reference

Azure subscription: "Azure subscription 1" (REDACTED-SUBSCRIPTION-ID), confirmed with the user 2026-07-09 as the ~$75 credit grant subscription. Region: East US 2 (eastus2) for all resources. Provisioned via infra/envs/dev (tier=dev):

- Resource group: `rg-argus-dev`
- Event Hubs namespace: `evhns-argus-dev-to614f` (Standard, 1 TU, event hub `transactions`, 2 partitions, 1-day retention). RBAC: current az CLI identity has "Azure Event Hubs Data Sender" + "Azure Event Hubs Data Receiver" on this namespace (dev-only bridge, Chunk 4 -- Chunk 10 replaces with the Container App's managed identity)
- Cosmos DB (Gremlin API) account: `cosmos-argus-dev-to614f` (free tier, single region, database `argus-graph` @ 1000 RU/s shared; endpoint `https://cosmos-argus-dev-to614f.documents.azure.com:443/`). Graph container: `argus-graph-container`, partition key `/partitionKey` (single shared low-cardinality key, value "argus" on every vertex -- see docs/architecture/partition_key_strategy.md), shares the database's 1000 RU/s (still $0). LOADED as of Chunk 5: 5,387 vertices (1,853 Account / 1,853 Customer / 102 Device / 1,530 IPAddress / 49 Merchant) + 6,867 edges (1,380 FT / 1,580 AF / 831 UD / 1,538 SA / 1,538 OWNS) -- the representative subset, not the full corpus
- Key Vault: `kv-argus-dev-to614f` (RBAC authorization, soft-delete 7 days, purge protection off; `https://kv-argus-dev-to614f.vault.azure.net/`) -- still empty; Chunk 4 authenticated to Event Hubs via Azure AD/RBAC instead of a connection string, so no secret was needed here yet. Chunk 10 will use it for whatever genuinely needs a stored secret in production.
- Container Apps environment: `argus-dev-cae` (Consumption/scale-to-zero) + Log Analytics workspace `argus-dev-law` -- no container deployed yet (still pending; not this chunk's scope either)
- Budget alert: `argus-dev-budget`, $75/month, 50/75/90% notifications to redacted@example.com

Connection strings, keys, and the random suffix's source are in Terraform state (`infra/envs/dev/terraform.tfstate`, gitignored) -- never in this file.

## Known Issues / TODO
- RESOLVED 2026-07-09: `data/raw/` now holds the real IEEE-CIS dataset
  (590,540 transactions) — Kaggle competition rules were accepted and
  `data/scripts/acquire_ieee_cis.py`/`ring_injector.py`/`eda_report.py`
  reran against it. Note the `kaggle` command still isn't on PATH on this
  machine — use `python -m kaggle ...`.
- Real dataset's device diversity is much higher than the old bundled
  sample (1,786 unique devices vs. 6), as expected.
- Ingestion throughput (LocalFileSink, this dev machine): ~9,200-11,300
  events/sec across synthetic-load and real-590K-row runs — directional
  toward the PDD's 15,000 events/sec target, not yet at it. Expected to
  improve with the real `azure_messaging_eventhubs` async client in
  Chunk 4 and formal load testing in Chunk 11; not a gate for either
  chunk.
- Azure resource providers may need one-time registration on first use in
  a fresh subscription (hit this with `Microsoft.App` in Chunk 3) — if a
  future chunk's `terraform apply` fails with
  `MissingSubscriptionRegistration`, run
  `az provider register -n <Namespace>` and re-plan/re-apply.
- Chunk 4's validation left ~6,000 test events sitting in the `transactions`
  hub (two 3,000-event validation runs). Harmless — 1-day retention means
  they age out on their own — but Chunk 5's Cosmos loader should expect
  this test traffic if it ever reads directly from the hub instead of
  `data/simulated/`.
- No ML/agent code yet.

## File Map
- `docs/` — `specs/` holds the two master specs (POC_Blueprint.md, PDD_Production_Guide.md); `architecture/` holds chunk1_data_eda_summary.md and partition_key_strategy.md (Gremlin partition key + indexing policy reasoning)
- `data/` — `scripts/` holds `graph_schema.py` (shared vertex/edge schema + real-data derivation), `acquire_ieee_cis.py` (Kaggle acquisition + bundled-sample fallback), `ring_injector.py` (synthetic ring injection), `eda_report.py` (validation/EDA); `raw/` and `simulated/` are gitignored but currently populated (bundled sample + 45 injected rings) — regenerate anytime via the three scripts in order
- `ingestion/` — real Cargo crate: `src/lib.rs` (RawTransaction/EnrichedTransaction, `Sink` trait, `LocalFileSink`/`StdoutSink`, SHA-256 PII masking, 8 passing unit/integration tests), `src/event_hub_sink.rs` (`EventHubSink` -- Azure AD auth via `DeveloperToolsCredential`, retry w/ backoff), `src/main.rs` (binary entrypoint; `ARGUS_SINK=eventhub` targets real Event Hubs, `ARGUS_EVENT_LIMIT` caps volume), `examples/eventhub_validate.rs` (send+read-back round-trip check); Chunk 5 is next
- `ml/` — `model_def.py` (shared InstitutionalFraudSAGE class), `requirements.txt`; `training/` holds `features.py` (POC section 3 features + Account graph construction) and `train_gnn.py` (real training loop, MLflow sqlite tracking, honest eval, artifact export); `artifacts/` holds model.pt + model_config.json + feature_stats.json (committed -- Chunk 7 loads these); `inference/` holds `inference_service.py` + `prepare_validation_events.py` (Chunk 7, in progress). NOTE: run ML code with `.venv/Scripts/python.exe` (torch lives in the repo venv, not global Python)
- `agents/` — scaffolded, empty (LangGraph compliance loop lands Chunk 8)
- `graph/` — `loader.py` (Cosmos Gremlin subset loader + traversal validation, `--validate` for checks only) + `requirements.txt` (gremlinpython)
- `infra/` — real Terraform: `modules/{event_hubs,cosmos_db,container_apps,key_vault,budget_alert}` (5 modules; `cosmos_db` now includes the actual Gremlin graph container, not just account/database), `envs/dev` (wires them together, tier-switchable "dev"/"enterprise"); provisioned and live in Azure (see Environment & Resource Reference)
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
- 2026-07-09 — Claude Code — Chunk 3 — real Azure infra via Terraform.
  Confirmed subscription (Azure subscription 1) and budget alert email
  with the user before writing anything. Verified all resource schemas
  against live azurerm v4.80.0 docs first (real budget on the line),
  which caught two v4 attribute renames that would have broken apply.
  Wrote 5 modules (event_hubs, cosmos_db, container_apps, key_vault,
  budget_alert) + envs/dev wiring them with a tier "dev"/"enterprise"
  switch. Ran init/validate/plan, printed the full plan, and stopped for
  explicit go-ahead before applying (per the standing rule for this
  chunk). First apply hit MissingSubscriptionRegistration for
  Microsoft.App (never used Container Apps on this subscription before);
  registered the provider, re-planned (1 resource remaining), got
  re-approval, applied. All 11 resources live: rg-argus-dev,
  evhns-argus-dev-to614f, cosmos-argus-dev-to614f, kv-argus-dev-to614f,
  argus-dev-cae, argus-dev-budget. Incidentally discovered (via git
  status, not done by this session) that the real IEEE-CIS dataset had
  been pulled since Chunk 2 — refreshed the now-stale ingestion JSONL
  export and re-verified the Rust engine against the full 590,860-row
  real corpus (~9,200 events/sec, no errors). README Data Sources and
  Chunk 1 EDA doc now reflect real numbers throughout.
- 2026-07-09 — Claude Code — Cosmos DB throughput bumped 400→1000 RU/s
  (infra/modules/cosmos_db, infra/envs/dev tier config), applied cleanly
  as a single in-place update, still $0 under free tier.
- 2026-07-09 — Claude Code — Chunk 4 — wired the Rust ingestion engine to
  real Azure Event Hubs. Confirmed azure_messaging_eventhubs v0.15.0's
  actual API against the SDK's own GitHub source first (docs.rs didn't
  resolve via fetch); found `DeveloperToolsCredential` replaces
  `DefaultAzureCredential`, and `ConsumerClient::open()` takes an owned
  `String` for eventhub name where `ProducerClient::open()` takes `&str`.
  Added `EventHubSink` (src/event_hub_sink.rs) with Azure AD auth (no
  connection string), layered retry (SDK RetryOptions + a 3-attempt
  exponential-backoff wrapper at the Sink layer), wired into main.rs behind
  `ARGUS_SINK=eventhub`. Terraform: added two dev-only role assignments
  (Data Sender, then Data Receiver once validation revealed Sender alone
  can't read events back) on evhns-argus-dev-to614f, each shown as a
  1-resource plan and approved before applying. Validation
  (examples/eventhub_validate.rs): sent 3,000 real transactions through the
  full engine, read all 3,000 back (1,500/partition) after fixing the
  receiver RBAC gap -- confirmed round-trip delivery.
- 2026-07-09 — Claude Code — Chunk 5 prep (not complete -- loader and
  traversal validation still ahead). Resolved the Gremlin partition key
  model: one container (`argus-graph-container`) with one shared
  low-cardinality partition key (`/partitionKey`), not per-vertex-label
  containers; PDD section 1's per-row "Partition Key" column reinterpreted
  as indexing guidance, applied via composite indexes on risk_base,
  mcc_code, hop_distance, gnn_risk_score. Confirmed against Microsoft Learn
  docs that this container (no dedicated throughput) shares the database's
  existing free-tier 1000 RU/s at $0 (1st of 25 allowed containers).
  Reasoning documented in docs/architecture/partition_key_strategy.md.
  Terraform: added azurerm_cosmosdb_gremlin_graph to modules/cosmos_db,
  shown as a 1-resource plan, approved, applied. Also added the OWNS
  (Customer -> Account) edge to data/scripts/graph_schema.py -- a flagged
  deviation from the PDD's literal 4-edge list, resolving Chunk 1's
  deferred question about `cust_id`. Code only; data/simulated/ not
  regenerated this session.
- 2026-07-09 — Claude Code — Chunk 5 — Cosmos Gremlin loader + traversal
  validation. Reran ring_injector/eda_report/export so data/simulated/
  includes edges_owns.parquet (39,974 OWNS rows; also added owns to
  eda_report.py's table list, which the prep session missed). Confirmed
  Cosmos Gremlin constraints against Microsoft docs (no bytecode, GraphSON
  v2 only, no null props, key-based auth only), verified gremlinpython
  3.8.1 against the live container despite Microsoft's table recommending
  3.4.13. graph/loader.py loaded the representative subset in 720s:
  5,387 vertices (1,853 Account incl. all 315 ring members, 1,853
  Customer, 102 Device, 1,530 IPAddress, 49 Merchant) + 6,867 edges
  (1,380 FUNDS_TRANSFER, 1,580 ACCESSED_FROM, 831 USED_DEVICE, 1,538
  SETTLED_AT, 1,538 OWNS), with 429-throttling retry. Traversal
  validation: (a) multi-hop from ring member ACC-R00295 via shared
  Device AND via shared IP each reached its 4 fellow device_cluster ring
  members — PASS; (b) Customer->OWNS->Account resolves (1,538 OWNS edges,
  sample CUST-000047->ACC-000047) — PASS.
- 2026-07-09 — Claude Code — Chunk 6 — GNN training pipeline on the full
  real corpus (40,289 nodes / 2.68M directed edges incl. capped
  shared-device/IP edges). InstitutionalFraudSAGE per POC section 3
  (2-layer SAGEConv max-aggr, hidden 64), weighted NLL (~117:1),
  ring-component 60/20/20 holdout, MLflow (sqlite) tracked, early stop
  (best val PR-AUC 0.997 @ epoch 5). TEST (held-out rings, 51 positives):
  precision 1.000, recall 0.824 (42 TP / 9 FN), F1 0.903, PR-AUC 0.997,
  FP-rate 0.000 (0 FP / 7,996 TN). Honesty: NOT a hollow result -- FP-rate
  meets the PDD <2.5% target while recall stays at 82.4%, so the model is
  genuinely catching held-out rings, not under-flagging. Big caveat: the
  positives are synthetic rings that are structurally conspicuous by
  construction; these numbers validate the pipeline, they do NOT claim
  real-world-fraud performance. Artifacts exported to ml/artifacts/ for
  Chunk 7. Environment: ML runs via repo .venv (Windows MAX_PATH broke
  global torch), MLflow on sqlite (filesystem backend deprecated in 3.14).

Last updated: 2026-07-09 by Claude Code
