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
- [x] Chunk 7 — Real-time GNN inference service
- [x] Chunk 8 — LangGraph agentic compliance loop
- [x] Chunk 9 — Tableau dashboard
- [x] Chunk 10 — Production hardening
- [ ] Chunk 11 — Load testing & SLO validation  ← IN PROGRESS
- [ ] Chunk 12 — Docs polish & demo packaging

## Current State
- Active chunk: 11
- Exact next action: run Chunk 11 prompt (load testing & SLO validation —
  the PDD's 15,000 events/sec ingestion and <300ms inference targets get
  their formal measurement; Event Hubs TU count may need a temporary bump;
  full-corpus Cosmos load was also deferred to this chunk).

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
  tier, into "Azure subscription 1" (subscription ID redacted -- see local
  terraform.tfvars / az account show; not committed, see 2026-07-11 security
  pass), confirmed with the user as the ~$75 credit grant subscription before
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
  budget to the alert email configured in the (gitignored) terraform.tfvars
  -- a hard ceiling on real spend while iterating, not just documentation
  of intent.
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
- 2026-07-09 — Chunk 7: streaming inference is CROSS-QUERY, NOT
  CROSS-QUEUE. No new Azure infrastructure was provisioned (hard
  constraint this session): the service consumes the existing
  `transactions` hub and writes gnn_risk_score back onto Account vertices
  in the existing argus-graph-container. Chunk 8's compliance agent
  discovers high-risk nodes by querying Cosmos directly
  (g.V().has('gnn_risk_score', gt(threshold))) -- no separate
  notification hub/queue for flagged accounts. The composite index on
  gnn_risk_score (provisioned in Chunk 5 prep) is what makes that query
  cheap.
- 2026-07-09 — Chunk 7: inference architecture -- local graph state is
  WARM-STARTED from data/simulated/'s parquet snapshot (the same
  aggregates training used; cold-start would produce scores from a
  distribution the model never saw), then updated incrementally per event
  (Welford variance, counterparty/device sets, new transfer edges).
  Scoring runs a full-graph forward per batch instead of per-node 2-hop
  subgraph extraction -- exactly equivalent for a 2-layer model and
  simpler; at 40K nodes/2.68M edges CPU it costs ~9-12s per batch, which
  is the main latency driver (see Known Issues). Python Event Hubs SDK
  confirmed against Microsoft docs before writing: EventHubConsumerClient
  + receive_batch + starting_position="@latest" (skips Chunk 4's leftover
  test events), and azure.identity's DefaultAzureCredential DOES exist in
  the Python SDK (unlike Rust's DeveloperToolsCredential) -- Chunk 4's
  RBAC grants cover it, no new secrets.
- 2026-07-09 — Fix: ring-injected accounts now get OWNS edges too. Chunk
  5's OWNS addition only wired background accounts (`derive_account_universe`);
  ring accounts (`ring_injector.py`'s `_new_account_customer_rows`, used by
  all 3 ring archetypes) got a Customer vertex but no edge connecting it.
  Fixed at the source: `_new_account_customer_rows` now returns the OWNS
  edge alongside the account/customer rows it already creates (one place,
  used by circular/smurfing/device_cluster-non-reuse; the device_cluster
  reuse path needs nothing new since it reuses accounts already OWNS-linked
  by an earlier ring). Reran the full pipeline: 40,289 total OWNS edges
  (39,974 background + 315 ring, i.e. exactly 1:1 with every account, no
  gaps). Cosmos already had all 315 ring accounts' Account AND Customer
  vertices loaded (Chunk 5's subset selection follows Account.cust_id,
  which always included ring accounts) -- only the 315 missing edges
  needed adding, not a full reload: `graph/loader.py --add-ring-owns`,
  idempotent (checks for an existing edge before adding), 0 skipped on
  this run since none existed. Re-validated Customer->OWNS->Account
  specifically on a RING member this time (the original Chunk 5
  validation's `g.V().hasLabel('Customer').limit(1)` happened to land on
  a background customer, never exercising the case this fix addresses) --
  confirmed PASS: CUST-R-CIRC-000-0 -> ACC-R00000.
- 2026-07-09 — SECURITY CORRECTION: Chunk 5's claim that "Cosmos Gremlin
  has no native AAD data-plane auth, password must be the account key"
  was WRONG -- it was accepted from a documentation page's general
  statement without testing the actual current capability. Gremlin-
  specific RBAC is real:
  `Microsoft.DocumentDB/databaseAccounts/gremlinRoleDefinitions` +
  `gremlinRoleAssignments` (confirmed present in azurerm's underlying ARM
  API surface through 2026-04-01-preview; built-in roles "Cosmos DB
  Gremlin Built-in Data Reader"/"Data Contributor" exist and are listable
  via `az cosmosdb gremlin role definition list`). Verified empirically,
  not just from docs: created a role assignment
  (`az cosmosdb gremlin role assignment create`, principal = the same az
  CLI identity used throughout this project, scope = argus-graph-container,
  role = Data Contributor), then connected gremlinpython using a plain
  `DefaultAzureCredential().get_token("https://cosmos.azure.com/.default")`
  token as the wire-protocol password -- worked for read, write, AND
  delete (tested a full add/read-back/drop cycle on a throwaway vertex).
  Migrated both `graph/loader.py` and `ml/inference/inference_service.py`
  off the account key entirely -- no more `ARGUS_COSMOS_KEY`, no more
  `az cosmosdb keys list` subprocess calls. Both now authenticate Cosmos
  Gremlin the same way Chunk 4 authenticates Event Hubs: a Microsoft
  Entra token via `azure-identity`, no static secret anywhere. One caveat
  worth being honest about: Microsoft's own "Secure your Azure Cosmos DB
  for Apache Gremlin account" page still states plainly that "Cosmos DB
  does not natively support authentication via managed identity" -- that
  statement is either stale relative to this RBAC feature, or refers
  specifically to a different (managed-identity-only) path than the
  RBAC-role-assignment + user/service-principal-token path actually
  tested here. Either way, what was tested is what's now running in this
  repo's code, empirically confirmed against the live account, not
  assumed from either the old claim or the new docs.
- 2026-07-09 — Known gap, flagged not hidden: the Gremlin RBAC role
  assignment just created (`fea56381-280f-4482-8619-1eb6e0933ed1`,
  Data Contributor, scoped to argus-graph-container) exists ONLY via az
  CLI -- it is NOT tracked in Terraform state. `azurerm` has no native
  resource for `gremlinRoleAssignments`/`gremlinRoleDefinitions` yet (this
  is a preview API surface); formalizing it would need the `azapi`
  provider (a new Terraform dependency), which wasn't added in this
  session since it wasn't asked for and adding a new provider is a real
  decision, not a drive-by. Practical implication: if `cosmos-argus-dev-to614f`
  is ever destroyed/recreated via Terraform, this role assignment won't
  come back automatically -- rerun the `az cosmosdb gremlin role
  assignment create` command logged above (or formalize via `azapi` first).
- 2026-07-10 — Chunk 8: Foundry/LLM deployment saga, in order. (1) Verified
  the current mechanism for Claude "Hosted on Azure": azurerm CANNOT express
  it (azurerm_cognitive_deployment lacks `modelProviderData`, issue #31140;
  azurerm_cognitive_account lacks `allowProjectManagement`) -- the official
  Azure-Samples/claude starter kit uses azapi, so the azapi provider (~>2.0)
  was added to our Terraform, approved as part of the deployment plan.
  (2) The approved plan (claude-opus-4-8 version 2 "Hosted on Azure",
  GlobalStandard, East US 2, $5/$25 per MTok via Marketplace CCU billing,
  with the explicit caveat that Marketplace charges hit the payment card,
  not Azure credits) FAILED at the deployment step: InsufficientQuota, and
  `az cognitiveservices usage list` confirmed EVERY Claude model has a hard
  0-TPM quota limit on this subscription -- the credit-grant subscription
  classification Microsoft's docs warn about. Subscription-level; no
  capacity value or Claude model swap fixes it; remedy is a Microsoft quota
  request form with uncertain outcome for credit subscriptions. (3) With
  explicit user approval, switched to gpt-5-mini (Azure OpenAI first-party):
  500K TPM quota available, native azurerm_cognitive_deployment, bills as
  NORMAL Azure consumption (draws from the $75 credit AND falls under the
  rg budget alert, unlike the Marketplace path), same Entra ID auth, and
  arguably closer to the PDD's literal "Azure OpenAI via Foundry" wording.
  Found empirically: the deployment 400s (DeploymentModelNotSupported) if
  `version` is omitted -- pinned to 2025-08-07. The Foundry account
  (argus-dev-foundry-to614f), project, and Cognitive Services User role
  assignment from the original attempt were kept (all $0 idle) -- the
  account hosts OpenAI deployments identically. Module renamed
  foundry_claude -> foundry_llm with a terraform `moved` block (3 resources
  kept in state, zero destroyed). Smoke-tested end-to-end with
  DefaultAzureCredential (scope https://cognitiveservices.azure.com/.default,
  endpoint https://<account>.services.ai.azure.com/openai/v1/): PASS.
- 2026-07-10 — Chunk 8: groundedness guardrail design (mandatory, per this
  chunk's requirements -- a compliance report that invents a detail is a
  real failure mode). Deterministic post-generation validation, not
  LLM-self-grading: (a) every entity matching ACC-*/CUST-*/DEV-*/IPv4
  patterns in the draft must appear in the serialized evidence bundle;
  (b) every number in the draft must match an evidence number exactly or be
  a faithful rounding/percentage form of one (list-enumeration prefixes
  exempt); violations => draft marked ungrounded, ONE regeneration with the
  violations fed back into the prompt, then hard-FAILED (never silently
  accepted). The result is persisted alongside the draft (sar_grounded).
  Orchestrated as a real LangGraph StateGraph: network_tracer ->
  behavioral_analyst -> sar_generator -> groundedness_guardrail, with a
  conditional edge from the guardrail back to the generator (max 1 retry).
- 2026-07-10 — Chunk 8: SAR storage is ON THE COSMOS VERTEX (sar_draft,
  sar_generated_at, sar_grounded, sar_model properties on the flagged
  Account), not Azure SQL Warehouse -- the PDD's pipeline diagram mentions
  one, but Chunk 3 deliberately never provisioned it and there's no budget
  justification to add one for this build. Same category of deliberate
  scope-down as the partition-key and Tableau-extract decisions. The
  enterprise path (SQL Warehouse for SAR archives + audit trail) remains a
  Terraform module away if ever needed.
- 2026-07-10 — Chunk 9: Tableau connects to a FLATTENED FILE EXTRACT, not
  the live graph. Tableau can't query Gremlin natively; the enterprise
  path is Cosmos Analytical Store + Synapse Link (Tableau's Synapse
  connector reading the auto-synced columnar store on the PDD's 15-minute
  refresh), which isn't budget-justified here (Synapse workspace + storage
  for a demo). dashboards/export_tableau_extract.py runs the real Gremlin
  queries (scored accounts + SAR flags from Cosmos) plus transaction-level
  computation (per-transfer 60s velocity_score_1m via two-pointer scan;
  hop_distance via multi-source BFS from all 158 flagged accounts over the
  undirected FUNDS_TRANSFER graph) and writes one 590,860-row extract
  (CSV for the .twb + parquet twin, both gitignored). Re-running the job
  is the "refresh". Enterprise migration path: set analytical_storage_ttl
  on the container, add Synapse Link, repoint the workbook's connection --
  the calculated fields wouldn't change.
- 2026-07-11 — Chunk 10: PII salt moved from the Chunk 2 env-var placeholder
  to the Key Vault secret `argus-pii-salt` (cryptographically random,
  generated locally, never committed/printed). The Rust engine fetches it
  at startup via azure_security_keyvault_secrets 1.0 (`SecretClient::new`
  + `get_secret(...).into_model()` -- API verified against the SDK's own
  README/source, same discipline as Chunk 4) using a shared credential
  chain (`azure_credential()` in lib.rs): ManagedIdentityCredential when
  IDENTITY_ENDPOINT/MSI_ENDPOINT is present (Container Apps), else
  DeveloperToolsCredential (az CLI). `ARGUS_PII_SALT` survives ONLY as a
  loud, explicit local/offline override; the silent insecure default salt
  is gone -- no vault and no override now fails startup, which is correct
  for a production credential path.
- 2026-07-11 — Chunk 10: velocity_score_1m stub (open since Chunk 2)
  closed with a REAL per-account trailing-60s sliding window
  (VelocityTracker, in-process HashMap<account, VecDeque<timestamps>>).
  Verified real: a 6-event single-account burst scored 1->2->3->4->5->6.
  Deliberate tradeoffs, documented not hidden: in-process (Redis isn't
  budget-justified) => state resets on restart, is per-replica if KEDA
  ever scales >1, and windows over INGESTION ARRIVAL time (RawTransaction
  carries no event timestamp) -- batch replays measure replay rate, which
  is the correct semantic for the live-stream service the deployment
  exists for.
- 2026-07-11 — Chunk 10: dead-letter path for sends that exhaust retries --
  a local JSONL file ({timestamp, error, payload} per line), NOT a second
  Event Hub / Service Bus queue (explicitly not budget-justified;
  ephemeral in the container, adequate because every dead-letter also
  hits stderr -> Log Analytics). Real bug found while testing it: tokio's
  File::write_all buffers internally and records could evaporate before
  reaching the OS (reproducible ~1-in-5 test flake) -- every dead-letter
  write now flushes through; 8/8 repeat runs green after the fix.
- 2026-07-11 — Chunk 10: TLS posture verified against the live resources,
  reported honestly: Event Hubs minimumTlsVersion=1.2 (its ceiling as a
  configurable floor), Cosmos minimalTlsVersion=Tls12 (no Tls13 option
  exists), Key Vault exposes no TLS property at all (platform-managed
  >=1.2). The PDD's "TLS 1.3 tunnels" is NOT enforceable as a floor on
  any of the three -- the true posture is "1.2 enforced everywhere, 1.3
  negotiated opportunistically", documented in
  docs/architecture/observability_queries.md. Claiming literal TLS-1.3
  compliance would be false.
- 2026-07-11 — Chunk 10: containerization + registry. Multi-stage
  Dockerfile (rust:1.96-slim-trixie builder -> debian:trixie-slim
  runtime, 141MB, non-root) -- image size kept small deliberately for
  Container Apps cold-start. Two facts found empirically: (1) the Azure
  Rust crates link OpenSSL on Linux (reqwest native-tls inside
  azure_core) even though they use rustls for AMQP -- the initial
  rustls-only image failed to build; (2) the repo is PRIVATE, which broke
  the planned anonymous-GHCR pull; per user choice the image builds in
  GitHub Actions (GITHUB_TOKEN has packages:write natively) and the
  package is flipped public via one manual UI step -- $0, vs an ACR at
  ~$5/mo. The public artifact is the compiled binary only.
- 2026-07-11 — Chunk 10: KEDA scale rule (azure-eventhub, managed-identity
  auth via custom_scale_rule.identity_id="System" -- attribute verified in
  the azurerm provider SOURCE; the website doc misspells it). PDD's
  ">5,000 undrained items" scaled 10x down to
  unprocessedEventThreshold=500 / activation=500, min 0 / max 1 replica.
  Two verified-not-assumed facts: the scaler REQUIRES a blob checkpoint
  container even with MI auth (hence the stargusdev* storage account,
  ~$0/mo, sole purpose KEDA checkpoints), and since NOTHING in this build
  commits checkpoints (the inference consumer reads @latest), KEDA counts
  all retained (<24h) events as unprocessed: a >500 burst activates the
  replica, which stays up until those events age out of the 1-day
  retention, then returns to 0. That is the literal semantics of
  "undrained" on a hub nobody drains; a checkpointing consumer
  (enterprise path) would make it lag-accurate. Honest architectural
  oddity, carried from the PDD itself: the deployed app is a PRODUCER
  being scaled on consumer-side lag -- exactly what the PDD's trigger
  describes, demonstrative rather than load-bearing in this build.
- 2026-07-11 — Chunk 10: two-identity model, both paths kept deliberately.
  DEV (local work): the az CLI identity's grants from Chunks 4/8 --
  Event Hubs Sender+Receiver, Gremlin Data Contributor (now
  Terraform-tracked via azapi after a state import -- the untracked-grant
  gap is closed), Cognitive Services User, Key Vault Secrets Officer.
  PROD (deployed service): the Container App's system-assigned managed
  identity -- Event Hubs Data Sender (it produces) + Data Receiver (the
  KEDA scaler reads lag under the app identity), Key Vault Secrets User
  (salt), Storage Blob Data Reader (KEDA checkpoints), and Gremlin Data
  Contributor via azapi (ANTICIPATORY: today's service has no Gremlin
  code path -- granted per the chunk instruction, flagged as unused
  rather than silently over-provisioned).
- 2026-07-10 — Chunk 9 SPEC CORRECTION: PDD section 3's Syndicate Cascade
  Index formula (`COUNTD([tx_id]) * SUM([amount]) * AVG([velocity_score_1m])
  * IF [proxy_flag] THEN 1.5 ELSE 1.0 END`) mixes aggregates with a
  row-level [proxy_flag] -- Tableau rejects that ("cannot mix aggregate and
  non-aggregate arguments"). Implemented as `IF MAX([proxy_flag]) THEN 1.5
  ELSE 1.0 END`, the minimal correction preserving the intent (any proxy
  exposure in scope applies the 1.5x multiplier). The other two fields
  (Multi-Hop Risk Dispersion Factor, Device Sharing Density Ratio) are
  verbatim. Caveat noted in the workbook itself: hand-authored XML,
  structure/fields verified (well-formed, correct formulas), but visual
  rendering needs a check in actual Tableau Desktop, which this
  environment doesn't have.

## Environment & Resource Reference

Azure subscription: "Azure subscription 1" (subscription ID redacted -- see local terraform.tfvars / az account show), confirmed with the user 2026-07-09 as the ~$75 credit grant subscription. Region: East US 2 (eastus2) for all resources. Provisioned via infra/envs/dev (tier=dev):

- Resource group: `rg-argus-dev`
- Event Hubs namespace: `evhns-argus-dev-to614f` (Standard, 1 TU, event hub `transactions`, 2 partitions, 1-day retention). RBAC: current az CLI identity has "Azure Event Hubs Data Sender" + "Azure Event Hubs Data Receiver" on this namespace (dev-only bridge, Chunk 4 -- Chunk 10 replaces with the Container App's managed identity)
- Cosmos DB (Gremlin API) account: `cosmos-argus-dev-to614f` (free tier, single region, database `argus-graph` @ 1000 RU/s shared; endpoint `https://cosmos-argus-dev-to614f.documents.azure.com:443/`). Graph container: `argus-graph-container`, partition key `/partitionKey` (single shared low-cardinality key, value "argus" on every vertex -- see docs/architecture/partition_key_strategy.md), shares the database's 1000 RU/s (still $0). LOADED: 5,387 vertices (1,853 Account / 1,853 Customer / 102 Device / 1,530 IPAddress / 49 Merchant) + 7,182 edges (1,380 FT / 1,580 AF / 831 UD / 1,538 SA / 1,853 OWNS, now 1:1 with every loaded account) -- the representative subset, not the full corpus. RBAC: "Cosmos DB Gremlin Built-in Data Contributor" on `argus-graph-container` for the current az CLI identity (role assignment id `fea56381-280f-4482-8619-1eb6e0933ed1`) -- **not Terraform-tracked** (azurerm has no native resource for this preview API yet; see Architectural Decisions Log). Both `graph/loader.py` and `ml/inference/inference_service.py` authenticate via this grant + `DefaultAzureCredential`, no account key.
- Key Vault: `kv-argus-dev-to614f` (RBAC authorization, soft-delete 7 days, purge protection off; `https://kv-argus-dev-to614f.vault.azure.net/`) -- holds `argus-pii-salt` (Chunk 10), the only secret this build genuinely needs stored (everything else is Entra-token auth).
- Container Apps environment: `argus-dev-cae` (Consumption/scale-to-zero) + Log Analytics workspace `argus-dev-law`. DEPLOYED (Chunk 10, image source updated in the addendum): Container App `argus-ingestion` (image `acrargusdevto614f.azurecr.io/argus-ingestion:chunk10`, pulled via the app's own managed identity/AcrPull -- built+pushed to GHCR by CI, then `az acr import`'d into ACR; 0.25 vCPU/0.5Gi, min 0 / max 1 replicas, KEDA azure-eventhub rule `eventhub-lag` @ 500-event threshold, MI-authenticated). System-assigned MI principal `19b38309-28e1-4e2c-8bf2-2092f9fd8bcd` with: EH Data Sender + Receiver (namespace), Key Vault Secrets User, Storage Blob Data Reader (checkpoints), AcrPull (registry), Gremlin Data Contributor (anticipatory, unused today). Console logs flow to `argus-dev-law` via the environment's log-analytics binding; saved KQL queries in docs/architecture/observability_queries.md
- Container Registry: `acrargusdevto614f` (Basic SKU, ~$5/mo, admin_enabled=false -- RBAC/MI pull only). Sole current image: `argus-ingestion:chunk10`.
- Storage account `stargusdevto614f` (Standard LRS, ~$0/mo) -- sole purpose: KEDA azure-eventhub checkpoint container `keda-checkpoints`
- Key Vault secret `argus-pii-salt` -- the production PII salt (Chunk 10); fetched at startup by the ingestion service via MI, by local dev via az CLI identity
- TWO-IDENTITY MODEL (Chunk 10): dev = az CLI identity (EH Sender/Receiver, Gremlin Data Contributor, Cognitive Services User, KV Secrets Officer) for local scripts/agents; prod = argus-ingestion's system MI (grants above). Both deliberate, neither replaces the other; all Gremlin grants now Terraform-tracked via azapi (untracked-grant gap closed by state import)
- Foundry (AIServices) account: `argus-dev-foundry-to614f` (S0, $0 fixed cost) + project `argus-dev-proj`. LLM deployment: `gpt-5-mini-argus` (gpt-5-mini v2025-08-07, GlobalStandard, 50K TPM of the subscription's 500K quota, version_upgrade_option=NoAutoUpgrade; PAYG token billing as normal Azure consumption). Endpoint `https://argus-dev-foundry-to614f.services.ai.azure.com/openai/v1/`, called with the deployment name as `model`. RBAC: current az CLI identity has "Cognitive Services User" on the account (dev-only bridge, same caveat as the other grants). NOTE: originally planned as claude-opus-4-8 -- blocked by subscription-level 0-TPM Claude quota; see Architectural Decisions Log.
- Budget alert: `argus-dev-budget`, $75/month, 50/75/90% notifications to the alert email in the (gitignored) terraform.tfvars

Connection strings, keys, the subscription ID, the alert email, and the random suffix's source are in Terraform state / terraform.tfvars (both gitignored) -- never in this file. (2026-07-11 security pass: subscription ID and alert email were previously written out in full here and in terraform.tfvars/variables.tf; redacted and purged from git history -- see Architectural Decisions Log.)

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
  hub (two 3,000-event validation runs), plus 400 more from Chunk 7's
  validation. Harmless — 1-day retention ages them out — and Chunk 7's
  consumer used starting_position="@latest" specifically to skip them.
- Chunk 7 inference latency (amortized per event: mean 326ms, p95 468ms)
  is ABOVE the PDD's <300ms directional target. Dominant costs: the
  full-graph forward pass (~9-12s/batch over 2.68M edges, CPU) and
  sequential Gremlin score writes (~55-90ms each). Obvious paths down:
  affected-subgraph-scoped forward, parallel/batched writes, GPU. Formal
  latency work is Chunk 11's job — flagged, not hidden.
- Chunk 6/7 model caveat: near-perfect metrics reflect synthetic rings
  that are structurally conspicuous by construction — they validate the
  pipeline, not real-world fraud performance.
- SETTLED, PERMANENT CONSTRAINT (not an open TODO): Claude models are
  quota-blocked (hard 0 TPM, subscription-level) on this subscription --
  as are ALL flagship OpenAI tiers (full gpt-5, gpt-5.1, 5.2, 5.4, 5.5,
  5.6 -- confirmed in the Chunk 8 follow-up); only mini/small tiers carry
  quota. This is the credit-grant subscription classification and no
  in-project action changes it. The Chunk 8 agents run gpt-5-mini
  (reasoning_effort=medium) as the accepted final configuration for this
  build. Only if a Microsoft quota-increase request were ever filed AND
  granted (outside this project's control) would a swap back be relevant;
  modules/foundry_llm's git history contains the azapi Claude deployment
  pattern for that hypothetical. The Chunk 3 East-US-2 region rationale
  is unaffected -- everything lives there anyway.
- Unrelated-to-Argus budget drain, surfaced during the Chunk 10 budget
  check: a Databricks workspace (rg-dp750, Germany West Central) burns
  ~$1.15/day (NAT Gateway + VNet), invisible to the rg-argus-dev budget
  alert. Not touched (not this project's resource) -- user informed.
- RESOLVED 2026-07-11 (Chunk 10): the Gremlin RBAC grant is now
  Terraform-tracked -- `azapi_resource.dev_gremlin_data_contributor`
  in infra/envs/dev/ingestion_app.tf, brought under state with
  `terraform import` (zero changes to the live resource). azurerm still
  has no native resource for gremlinRoleAssignments; azapi is the
  mechanism, same as the Foundry account.
- No agent code yet (Chunk 8 next).
- KEDA scaling limitation (Chunk 10, honest, not hidden): the deployed
  service has no checkpointing consumer, so the azure-eventhub scale
  rule's "unprocessedEventThreshold" counts ALL events retained in the
  hub's 1-day window, not true consumer lag. Practical effect: activation
  behaves like "a backlog of >500 events exists somewhere in the last
  24h" rather than "the consumer is falling behind." Correct given the
  hub has no draining consumer to lag behind; would need a checkpointing
  reader to become lag-accurate. Verified behavior end-to-end at small
  scale (600-event burst -> reactivation in ~25s); full-scale behavior
  under sustained load is Chunk 11's job.
- The Container App's managed identity was granted Cosmos Gremlin Data
  Contributor per the Chunk 10 instruction, but the deployed service has
  NO Gremlin code path today (produce-only, to Event Hubs) -- the grant
  is unused, anticipatory, and flagged rather than silently
  over-provisioned. Revisit if/when the ingestion engine ever writes to
  the graph directly.
- RESOLVED 2026-07-11 (Chunk 10 addendum): the ingestion service now
  pulls from Azure Container Registry (acrargusdevto614f) via its own
  managed identity (AcrPull RBAC, no admin credentials) instead of the
  public-GHCR workaround -- verified live (provisioningState Succeeded,
  clean startup logs). GHCR's `argus-ingestion` package now appears
  private (anonymous pull returns 404 as of this session, not
  independently confirmed who/what changed it) and is no longer the
  deployed app's pull source either way; the CI workflow still pushes
  there as a build artifact/cache, which is fine now that nothing
  depends on it being public.
- The CI workflow (build-ingestion-image.yml) builds and pushes the
  image but does NOT run `cargo test`/`cargo clippy` first -- those were
  run manually this session (11/11 passing, clean) but aren't yet a CI
  gate. A future chunk could add a test job before the build-push step;
  not done here since it wasn't in this chunk's explicit scope.
- The Container App was validated at small scale only (a single
  600-event manual burst, one scale-to-zero/scale-up cycle observed) --
  consistent with the standing rule that full-scale load validation is
  Chunk 11's job, not this one's. Sustained throughput, concurrent
  replica behavior (KEDA's max_replicas=1 here means no concurrency to
  test yet), and the deployed container's actual events/sec under load
  are all unmeasured.

## File Map
- `docs/` — `specs/` holds the two master specs (POC_Blueprint.md, PDD_Production_Guide.md); `architecture/` holds chunk1_data_eda_summary.md, partition_key_strategy.md, and observability_queries.md (KQL saved queries + verified TLS posture)
- `data/` — `scripts/` holds `graph_schema.py` (shared vertex/edge schema + real-data derivation), `acquire_ieee_cis.py` (Kaggle acquisition + bundled-sample fallback), `ring_injector.py` (synthetic ring injection), `eda_report.py` (validation/EDA); `raw/` and `simulated/` are gitignored but currently populated (bundled sample + 45 injected rings) — regenerate anytime via the three scripts in order
- `ingestion/` — real Cargo crate (11 passing tests): `src/lib.rs` (structs, `Sink` trait, SHA-256 PII masking, `azure_credential()` MI/dev chain, `fetch_pii_salt()` Key Vault fetch, `VelocityTracker` real trailing-60s window, `DeadLetter` flushed JSONL), `src/event_hub_sink.rs` (Entra auth, retry w/ backoff), `src/main.rs` (`ARGUS_MODE=service` for the deployed container; `ARGUS_SINK=eventhub`, `ARGUS_EVENT_LIMIT`), `examples/eventhub_validate.rs`, `Dockerfile` + `.dockerignore` (multi-stage, 141MB, non-root)
- `ml/` — `model_def.py` (shared InstitutionalFraudSAGE class), `requirements.txt`; `training/` holds `features.py` (POC section 3 features + Account graph construction) and `train_gnn.py` (real training loop, MLflow sqlite tracking, honest eval, artifact export); `artifacts/` holds model.pt + model_config.json + feature_stats.json (committed -- inference loads these); `inference/` holds `inference_service.py` (Event Hubs consumer -> incremental state -> GNN scoring -> Cosmos write-back, `--validate` for post-run checks) + `prepare_validation_events.py`. NOTE: run ML code with `.venv/Scripts/python.exe` (torch lives in the repo venv, not global Python)
- `agents/` — `compliance_graph.py` (real LangGraph StateGraph: NetworkTracer w/ live Gremlin traversals, BehavioralAnalyst w/ real transaction metrics, SARGenerator w/ real Foundry LLM call, groundedness guardrail w/ conditional retry edge), `orchestrator.py` (Cosmos cross-query discovery of flagged accounts -> pipeline -> SAR stored on vertex), `requirements.txt`. Runs on global Python (no torch needed)
- `graph/` — `loader.py` (Cosmos Gremlin subset loader + traversal validation; `--validate` for checks only, `--add-ring-owns` for the targeted OWNS-edge fix; auth via `DefaultAzureCredential` + Gremlin RBAC, no account key) + `requirements.txt` (gremlinpython, azure-identity)
- `infra/` — real Terraform: `modules/{event_hubs,cosmos_db,container_apps,key_vault,budget_alert}` (5 modules; `cosmos_db` now includes the actual Gremlin graph container, not just account/database), `envs/dev` (wires them together, tier-switchable "dev"/"enterprise"); provisioned and live in Azure (see Environment & Resource Reference)
- `dashboards/` — `export_tableau_extract.py` (Gremlin + transaction queries flattened to `extracts/argus_tableau_extract.csv|parquet`, gitignored; rerun = refresh), `argus_fraud_dashboard.twb` (hand-authored workbook, three PDD section 3 calculated fields, needs visual check in Tableau Desktop)
- `tests/` — `unit/`, `integration/`, `load/` scaffolded, empty
- `.github/workflows/` — `build-ingestion-image.yml` (CI image build+push to GHCR with the workflow's GITHUB_TOKEN; triggers on ingestion/** changes or manual dispatch)
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
- 2026-07-09 — Claude Code — Chunk 7 — real-time GNN inference service.
  ml/inference/inference_service.py: consumes the existing `transactions`
  hub (EventHubConsumerClient, DefaultAzureCredential, @latest), warm-
  starts local graph state from parquet, applies events incrementally,
  full-graph forward per batch, writes gnn_risk_score + gnn_scored_at to
  Cosmos Account vertices via gremlinpython. NO new Azure infrastructure.
  End-to-end validation: 400 real transactions (biased toward ring-member
  endpoints, all among Chunk 5's loaded accounts) sent through the actual
  Rust ingestion binary -> Event Hubs -> service; 400 consumed, 600 score
  writes across batches, 475 distinct Account vertices scored. Sanity
  check PASSED emphatically: mean gnn_risk_score 0.844 for ring members
  vs 3.9e-14 for legit accounts. Latency (honest): amortized mean 326ms,
  p95 468ms per event -- ABOVE the <300ms directional target; drivers and
  remediation paths logged in Known Issues, formal gate is Chunk 11.
- 2026-07-09 — Claude Code — Pre-Chunk-8 fixes (both requested because
  Chunk 8's Network Tracer agent depends on them). (1) Ring-injected
  accounts now get OWNS edges: fixed at the source in
  ring_injector.py's `_new_account_customer_rows`, reran the full
  pipeline (40,289 total OWNS edges, exactly 1:1 with every account),
  added only the 315 missing edges to Cosmos via a new
  `graph/loader.py --add-ring-owns` (idempotent, no full reload needed --
  both endpoints were already loaded), re-validated Customer->OWNS->Account
  specifically on a ring member this time (CUST-R-CIRC-000-0 ->
  ACC-R00000 -- PASS), since the original Chunk 5 validation happened to
  test a background account instead. (2) Re-investigated the "Gremlin has
  no AAD data-plane auth" claim from Chunk 5 -- it was wrong, accepted
  from documentation without testing. Gremlin-specific RBAC
  (gremlinRoleDefinitions/gremlinRoleAssignments) is real; got explicit
  user approval, then empirically confirmed a Data Contributor role
  assignment's Entra token IS accepted by the actual wire-protocol
  connection (read/write/delete all tested). Migrated graph/loader.py and
  ml/inference/inference_service.py off the account key entirely onto
  DefaultAzureCredential -- consistent with Chunk 4's no-static-secrets
  pattern. Flagged transparently: the role assignment isn't
  Terraform-tracked (azurerm has no native resource for this preview API
  yet).
- 2026-07-10 — Claude Code — Chunk 8 — LangGraph agentic compliance loop.
  Researched the current Claude-on-Foundry mechanism (azapi required;
  azurerm can't express modelProviderData), presented the deployment plan
  ($5/$25 per MTok, Marketplace CCU billing, card-not-credits caveat), got
  the one approved go-ahead -- then hit the documented eligibility wall:
  InsufficientQuota, all Claude models hard-limited to 0 TPM on this
  credit-grant subscription. Stopped, presented verified alternatives
  (gpt-5-mini 500K TPM / o4-mini 100K), user approved gpt-5-mini. Deployed
  gpt-5-mini-argus into the already-created argus-dev-foundry-to614f
  account (module renamed foundry_claude->foundry_llm via terraform moved
  block, zero destroys; explicit model version 2025-08-07 required --
  omitting it 400s). Smoke test via DefaultAzureCredential: PASS. Built
  agents/: real Gremlin-traversal NetworkTracer, real-metrics
  BehavioralAnalyst (velocity, inbound/outbound ratio, pass-through
  symmetry, burst, amount_std from the actual 590K-row corpus), real-LLM
  SARGenerator, deterministic groundedness guardrail, wired as a LangGraph
  StateGraph with a conditional retry edge. Orchestrator run: 6 flagged
  accounts discovered via Cosmos cross-query (all 6 ring members, score
  1.0), 6 SAR drafts generated, 6/6 passed groundedness on first attempt
  (0 regenerations), all 6 stored on their Account vertices (sar_draft/
  sar_generated_at/sar_grounded/sar_model). Total LLM cost: ~$0.02.
  ADDENDUM (2026-07-10 follow-up): (1) attempted upgrade to full gpt-5 --
  it is quota-blocked exactly like Claude (0 TPM at GlobalStandard, along
  with gpt-5.1/5.2/5.4/5.5/5.6; only mini/small tiers carry quota on this
  subscription), so per the prescribed fallback, reasoning_effort was
  raised on gpt-5-mini instead. HIGH effort backfired: hidden reasoning
  consumed the entire 6K completion budget on 3 of 6 accounts and returned
  EMPTY drafts -- which the guardrail passed VACUOUSLY (no text = no
  entities = no violations). Two fixes: the guardrail now hard-fails any
  draft under 200 chars ("empty or truncated"), and effort settled at
  MEDIUM with a 10K completion budget -- rerun produced 6/6 non-empty
  (1,209-1,735 chars), 6/6 grounded. Honest quality read: medium-effort
  drafts are marginally better than the originals (recommendation now
  names the linked counterparty accounts; neighbor GNN scores woven into
  the narrative) but substantially similar -- not a step change; latency
  roughly doubled (~11-16s -> ~22-32s per draft). (2) Deployment
  version_upgrade_option locked to NoAutoUpgrade (was
  OnceNewDefaultVersionAvailable) -- the pinned model version can no
  longer silently change; prior session's Audit Flag #12 resolved.
- 2026-07-10 — Claude Code — Chunk 9 — Tableau analytics layer via
  flattened extract. dashboards/export_tableau_extract.py: real Gremlin
  queries (475 scored accounts, 158 flagged, 6 grounded SARs) + full-corpus
  transaction computation (per-transfer 60s velocity, multi-source-BFS
  hop_distance from all flagged accounts) -> 590,860-row extract in 7s
  (CSV + parquet, gitignored). argus_fraud_dashboard.twb implements PDD
  section 3's three calculated fields (Syndicate Cascade Index with the
  MAX([proxy_flag]) spec correction -- the PDD formula is invalid Tableau
  as written; the other two verbatim) over that extract, one worksheet per
  field. XML validated well-formed; visual layout still needs a Tableau
  Desktop check (not available in this environment).
- 2026-07-11 — Claude Code — Chunk 10 — Production hardening. Budget check
  first: subscription MTD spend $12.43/$75, but $11.40 of that is an
  unrelated Databricks resource group (germanywestcentral) burning
  ~$1.15/day, invisible to the rg-argus-dev alert -- true Argus spend
  ~$1.03, headroom ~$74 (flagged to user, not touched, not this
  project's resource). Rust: PII salt now fetched from the real Key
  Vault secret argus-pii-salt via azure_security_keyvault_secrets 1.0 +
  a shared MI/dev credential chain (azure_credential() in lib.rs);
  velocity_score_1m stub (open since Chunk 2) closed with a real
  per-account trailing-60s sliding window (verified: a 6-event burst
  from one account scored 1->2->3->4->5->6); dead-letter JSONL added
  for exhausted-retry sends, and a real buffering bug was found and
  fixed while testing it (tokio's File::write_all doesn't guarantee the
  write reached disk -- fixed with an explicit flush, went from a
  ~1-in-5 test flake to 8/8 green). 11/11 tests passing, clippy clean.
  Container: multi-stage rust:1.96-slim-trixie -> debian:trixie-slim,
  141MB; found empirically that the Azure crates need OpenSSL on Linux
  (an initial rustls-only image failed to build) -- corrected, not
  silently worked around. Registry: the repo is PRIVATE, which broke
  the originally-assumed anonymous-GHCR-pull path; user chose GHCR +
  a manual one-time "make package public" step over an ACR at ~$5/mo.
  CI workflow (.github/workflows/build-ingestion-image.yml) builds and
  pushes via the run's own GITHUB_TOKEN (packages:write) -- no local
  token scope needed. Terraform (infra/envs/dev/ingestion_app.tf, plan
  shown and approved before apply, 8 resources added/0 destroyed):
  Container App argus-ingestion deployed into the EXISTING argus-dev-cae
  (min 0/max 1 replicas), system-assigned MI granted EH Data
  Sender+Receiver, Key Vault Secrets User, Storage Blob Data Reader,
  and (anticipatory, flagged as currently unused) Gremlin Data
  Contributor; a small storage account was added solely because KEDA's
  azure-eventhub scaler requires a blob checkpoint container even under
  MI auth (verified in KEDA docs, not assumed) -- ~$0/mo. The
  previously-untracked dev-identity Gremlin RBAC grant was brought into
  Terraform state via `terraform import` (zero live-resource change),
  closing that known gap. KEDA rule verified end-to-end at small scale
  (this chunk's validation ceiling; full load testing is Chunk 11's
  job): confirmed scale-to-zero after ~7 idle minutes, sent a 600-event
  burst via the local binary against the real hub, replica reactivated
  within ~25 seconds and started cleanly (salt fetched from Key Vault,
  correct sink). Documented honestly: with no checkpointing consumer in
  this build, KEDA counts ALL retained (<24h) events as "unprocessed,"
  so activation is closer to "any backlog exists" than true consumer
  lag -- a correct-but-limited reading of the PDD's own
  ">5,000 undrained items" trigger, scaled to 500 for this project.
  TLS verified against live resources and reported honestly: Event Hubs
  and Cosmos both cap at TLS 1.2 as a configurable floor, Key Vault
  exposes no TLS setting at all (platform-managed >=1.2) -- the PDD's
  literal "TLS 1.3 tunnels" is not achievable as an enforced floor on
  any of the three; documented in
  docs/architecture/observability_queries.md alongside 5 saved KQL
  queries (throughput, error rate, dead-letter incidents, replica
  lifecycle, heartbeat gaps) authored against Container Apps' documented
  log schema. Confirmed the Chunk 8 LLM quota constraint is now worded
  as a settled, permanent constraint rather than an open TODO. Two
  Known-Issue items closed this chunk (velocity stub, untracked Gremlin
  grant); no new stubs left silently in place.
- 2026-07-11 — Claude Code — Chunk 10 ADDENDUM (three items raised after
  the chunk was first marked complete):
  (1) REGISTRY RECONSIDERED. User asked point-blank whether making the
  GHCR package public was something explicitly approved or something
  decided and reported after the fact. Honest answer: explicitly
  approved -- the AskUserQuestion option the user selected stated
  plainly "makes the image (compiled binary only) publicly pullable."
  Gap acknowledged: no tool call in that session actually changed the
  package's visibility, so it's not certain whether the user performed
  the flip or GHCR defaulted it that way for a personal-account package
  -- that ambiguity should have been surfaced at the time and wasn't.
  Decision: switch to Azure Container Registry regardless, since a
  publicly pullable image is inconsistent with this project's
  no-static-secret, no-public-exposure pattern everywhere else, budget
  headroom easily covers it, and MI-based pull removes the ambiguity
  entirely. Verified live pricing via the Azure Retail Prices API (not
  assumed): ACR Basic = $0.1666/day base unit (~$5.00/30-day month) +
  $0.10/GB-month over the free 10GiB (our image is 141MB, well within
  it) -- ~$5/month against ~$74 headroom. Verified in the azurerm
  provider SOURCE (website doc is ambiguous here) that
  `registry.identity = "System"` is valid for the container app's own
  system-assigned identity, same pattern already proven for the KEDA
  scale rule's `identity_id`. Terraform written
  (infra/envs/dev/container_registry.tf: azurerm_container_registry
  Basic, admin_enabled=false, AcrPull role assignment to the app's MI;
  infra/envs/dev/ingestion_app.tf updated with a `registry` block and
  image path pointing at the new ACR). Plan shown, saved as
  tfplan_chunk10_addendum: 2 to add, 1 to change, 0 to destroy --
  AWAITING GO-AHEAD, not yet applied. Once applied, the plan is: `az acr
  import` the current image straight from the (still public) GHCR
  package -- no Docker Desktop needed for this one-time copy -- then
  flip the GHCR package back to private (or delete it) once the ACR
  pull path is confirmed working, closing the public-exposure gap for
  real rather than just adding an alternative next to it.
  (2) CI TEST GATE. .github/workflows/build-ingestion-image.yml split
  into a `test` job (cargo test --release + cargo clippy --release
  --all-targets -- -D warnings) that the `build-and-push` job now
  `needs:` -- a red test run blocks the push. Verified the exact clippy
  invocation passes locally first (0 warnings) so the new gate doesn't
  immediately redline CI.
  (3) LOAD-TEST CONTAMINATION. Checked empirically (partition
  properties via the azure-eventhub SDK) rather than assumed: the
  ~6,400 events flagged as leftover from Chunks 4/7's validation runs
  have ALREADY aged out via the 1-day retention -- both partitions'
  beginning_sequence_number has advanced past them. That specific
  concern is moot. Found instead a smaller NEW residual: 600 events
  from this session's own KEDA-verification burst (sent ~14:02 UTC),
  which will itself fully age out by ~2026-07-12 14:02 UTC. Decision for
  Chunk 11: don't gate the load test on waiting for natural expiry.
  Instead (a) for throughput/latency numbers, capture each partition's
  starting sequence number immediately before the test run and measure
  only events at/after that marker -- the same technique
  examples/eventhub_validate.rs already uses, which is immune to any
  pre-existing backlog regardless of its source; (b) for any KEDA
  scale-timing SLO specifically (sensitive to TOTAL retained count, not
  just new arrivals, per the Chunk 10 finding that this scaler has no
  checkpointing consumer), first verify all partitions report
  `is_empty=true` -- or explicitly net out the pre-existing count --
  before measuring "time to scale from empty."
- 2026-07-11 — Claude Code — Chunk 10 ADDENDUM RESOLVED: ACR migration
  applied and verified live. `az acr import` copied the image straight
  from the (still-public) GHCR package into acrargusdevto614f -- no
  Docker Desktop needed for the one-time copy. Two real bugs hit and
  fixed during apply, both honestly documented rather than brute-forced
  past: (1) the first apply tried to update the container app's image
  AND create the AcrPull role assignment in the same operation with no
  dependency between them; the app update failed (image didn't exist in
  ACR yet on attempt 1, then a real RBAC-propagation race on attempt 2)
  and aborted before the role assignment ever landed in state. Adding a
  `depends_on` from the container app to the role assignment was
  attempted but rejected -- it creates a graph CYCLE, since the role
  assignment's principal_id already references the container app's
  identity in the other direction. Fix: created the AcrPull grant
  directly via `az rest` (the `az role assignment` CLI subcommand itself
  turned out to be broken on this machine's az-cli 2.87.0 install --
  `--scope` reproducibly fails with a bogus MissingSubscription error
  even against known-good resources; routed around it with a raw ARM
  PUT, same API Terraform uses), then `terraform import`'d it into state
  so the next apply saw it as already-satisfied. (2) Git Bash's MSYS
  path-conversion mangled the leading `/subscriptions/...` of the import
  ID into a Windows path -- fixed with MSYS_NO_PATHCONV=1, the same
  workaround this project already uses for `docker run` volume mounts.
  End state, verified live: argus-ingestion's active image is
  `acrargusdevto614f.azurecr.io/argus-ingestion:chunk10`,
  provisioningState Succeeded, runningStatus Running, logs confirm a
  clean start (Key Vault salt fetched, correct sink) -- MI-based ACR
  pull works end-to-end. GHCR anonymous pull, which returned HTTP 200
  earlier in this same session, now returns 404 -- the package appears
  to already be private (no tool call in this session changed its
  visibility; either the user did it directly or GitHub's behavior
  changed some other way -- not independently confirmed, and flipping
  package visibility is an account-settings change this agent won't
  push on unilaterally). The prior Known-Issue bullet about the public
  GHCR package is removed below since ACR is now the live pull path
  regardless of GHCR's current state.

Last updated: 2026-07-11 by Claude Code
