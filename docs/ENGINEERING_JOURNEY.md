# Engineering Journey — Project Argus

This is a curated record of the moments in this project where an assumption
was checked and turned out to be wrong, a bug was found and fixed, or a spec
conflict had to be resolved. It is not a changelog and not a highlight reel —
each entry is here because something was *believed* to be true and the real
behaviour differed. The full chronological history lives in
[`context.md`](../context.md).

A standing rule ran through every session: **verify library, API, and cloud
behaviour empirically or against current docs — never from memory.** The list
below is what that rule actually caught. Read it as evidence that the rule
earned its keep, not as a claim that any single item is remarkable.

---

## Security & authentication

### Cosmos DB Gremlin *does* accept Entra ID (Azure AD) tokens — contradicting both our own earlier note and Microsoft's own docs
- **Assumed:** Cosmos DB Gremlin has no data-plane AAD auth; the wire-protocol
  password must be the account key. This was written into the Chunk 5 loader,
  taken from a Microsoft "Secure your Gremlin account" page that states
  plainly "Cosmos DB does not natively support authentication via managed
  identity."
- **Actually:** Gremlin-specific RBAC is real —
  `Microsoft.DocumentDB/databaseAccounts/gremlinRoleDefinitions` /
  `gremlinRoleAssignments`, with built-in "Data Reader" / "Data Contributor"
  roles — and the resulting Entra token *is* accepted by the Gremlin
  wire-protocol connection as the password field.
- **Verified:** created a Data Contributor role assignment for the dev
  identity, then connected `gremlinpython` using nothing but a
  `DefaultAzureCredential().get_token("https://cosmos.azure.com/.default")`
  token and ran a full add → read-back → drop cycle on a throwaway vertex. All
  three succeeded.
- **Changed:** both `graph/loader.py` and the inference service dropped the
  account key entirely (`ARGUS_COSMOS_KEY` and the `az cosmosdb keys list`
  subprocess calls are gone). The honest caveat was kept in `context.md`: the
  Microsoft doc statement is either stale or refers to a different
  managed-identity-only path — but what's running was tested against the live
  account, not assumed from either the old claim or the new docs.

### The POC's MD5 PII hashing is broken; the production spec says SHA-256
- **Assumed (by the POC snippet):** `device_id` could be hashed with MD5 for
  pseudonymisation.
- **Actually:** MD5 is cryptographically broken, and the project's *own*
  production guide (PDD section 5) specifies "SHA-256 salted tokens" — the two
  source documents disagreed.
- **Verified:** cross-read the POC blueprint against the PDD; treated the
  security-controls spec as authoritative over the illustrative snippet.
- **Changed:** the Rust engine uses `sha2` SHA-256 salted with a secret. In
  Chunk 10 that salt moved out of an env-var placeholder into a real Key Vault
  secret, fetched at runtime via managed identity — no static secret anywhere.

### Event Hubs separates "Send" and "Listen" AMQP claims — one role isn't enough
- **Assumed:** "Azure Event Hubs Data Sender" on the identity would cover the
  Chunk 4 send-then-read-back validation.
- **Actually:** the AMQP claim model separates Send from Listen; reading events
  back needs "Data Receiver" too.
- **Verified:** the first apply (Sender only) sent 3,000 events successfully
  but failed to read them back with `UnauthorizedAccess ('Listen' claims
  required)`.
- **Changed:** added the Receiver role in a second reviewed plan+apply, rather
  than reaching for a broad "Owner" grant to make the error go away.

### GHCR-public image → ACR with managed-identity pull
- **Assumed (mid-Chunk-10):** publishing the container image to a public GHCR
  package was an acceptable way to let Container Apps pull it without a stored
  credential.
- **Actually:** a publicly pullable image is exactly the kind of exposure this
  project avoided everywhere else (no static secrets, no public surfaces). When
  asked directly whether that had been explicitly approved, the honest answer
  was that the tradeoff *was* surfaced and approved — but the ambiguity of who
  actually flipped the package to public had not been, and should have been.
- **Verified:** confirmed via the Azure retail pricing API that ACR Basic is
  ~$5/mo, and verified in the azurerm provider source (not the ambiguous
  website doc) that a Container App can pull from ACR using its own
  system-assigned identity (`registry.identity = "System"`, AcrPull role).
- **Changed:** migrated to a private ACR with managed-identity pull. The
  migration itself surfaced a Terraform dependency-cycle (the AcrPull role
  references the app identity, which references the registry) — resolved by
  granting the role out-of-band and importing it into state.

---

## Data modelling & the graph

### Cosmos Gremlin uses one container with one partition key — not one container per vertex label
- **Assumed (by the PDD):** section 1's table has a per-row "Partition Key"
  column, reading as though each vertex label gets its own partitioning.
- **Actually:** a Cosmos Gremlin graph is a *single* container with a *single*
  partition key. The per-label table only makes sense as indexing guidance.
- **Verified:** against Microsoft Learn's Gremlin data-modelling docs before
  writing the loader.
- **Changed:** used one container with a single low-cardinality shared key
  (`/partitionKey = "argus"`) so every fraud traversal — which is inherently
  cross-entity — stays same-partition, and reinterpreted the PDD table as
  composite-index guidance. Full reasoning in
  [`partition_key_strategy.md`](architecture/partition_key_strategy.md). This
  trades away horizontal write scaling the data volume never needed for query
  locality it very much does.

### gremlinpython / Cosmos Gremlin has real, non-obvious constraints
- **Assumed:** a modern Gremlin driver would support bytecode traversals and
  the current GraphSON version.
- **Actually:** Cosmos Gremlin supports *no* bytecode (string queries +
  bindings only), requires the GraphSON **v2** serializer (v3 unsupported), and
  rejects null property values outright.
- **Verified:** against Microsoft docs, then confirmed `gremlinpython` 3.8.1
  works against the live container despite Microsoft's compatibility table
  recommending the much older 3.4.13.
- **Changed:** the loader emits string queries with bindings, pins the v2
  serializer, and skips `None`/`NaN` properties per row.

### A spec formula that doesn't compile
- **Assumed:** the PDD's "Syndicate Cascade Index" Tableau formula could be
  used verbatim.
- **Actually:** it mixes an aggregate (`SUM`, `COUNTD`) with a row-level
  `[proxy_flag]`, which Tableau rejects ("cannot mix aggregate and
  non-aggregate arguments").
- **Verified:** by building the calculated field in the workbook.
- **Changed:** wrapped the row-level term as `MAX([proxy_flag])` — the minimal
  correction preserving intent (any proxy exposure in scope applies the
  multiplier). The other two calculated fields were used verbatim.

---

## Cloud infrastructure & cost

### Terraform azurerm v4 renamed attributes that would have failed `apply`
- **Assumed:** attribute names from memory (`enable_free_tier`,
  `enable_rbac_authorization`).
- **Actually:** azurerm v4 renamed them to `free_tier_enabled` and
  `rbac_authorization_enabled`.
- **Verified:** read the live provider schema from the azurerm GitHub source
  (the Terraform Registry site is JS-rendered and wouldn't fetch) *before*
  applying, because real budget was on the line.
- **Changed:** used the correct names; the apply succeeded first try instead of
  erroring against real resources.

### Standard-tier Event Hubs partitions are immutable, and Standard→Premium is not an in-place upgrade
- **Assumed (going into load testing):** partition count and tier were tunable
  knobs for a throughput benchmark.
- **Actually:** on Standard tier the partition count is fixed at creation
  ("not possible... only Premium/Dedicated"), and moving Standard→Premium
  requires deploying a *new* namespace, not resizing.
- **Verified:** against the Event Hubs FAQ and quota docs before proposing any
  change.
- **Changed:** the load test scaled only the reversible knob (throughput
  units), and the enterprise benchmark stood up a *parallel* Premium namespace
  (deleted at teardown) rather than mutating the dev one. Flipping the tier
  variable wholesale was explicitly rejected: it would also flip
  `cosmos_free_tier=false`, which forces *replacement* of the Cosmos account —
  destroying the graph, the Gremlin RBAC grants, and the once-per-subscription
  free tier.

### The KEDA autoscale rule was scaling the wrong tier of the system
- **Assumed (Chunk 10):** an Event Hubs–lag KEDA rule on the ingestion service
  was a sensible autoscaler.
- **Actually:** it scales the *producer* on backlog in the hub the producer
  writes *into*. Backlog is production-minus-consumption; adding producer
  replicas can't drain it and can only add to it. The real lagging tier is the
  consumer (the inference service).
- **Verified with live traffic:** during the load test, sending events *from a
  different machine* still caused the deployed ingestion app to scale 0→2
  replicas — visibly reacting to a backlog it could do nothing about.
- **Changed:** documented as a confirmed design bug with the correct fix
  (move the scale rule to a containerised inference consumer keyed on its own
  checkpointed lag). Not yet implemented — it needs new deployment
  infrastructure — so it remains the one honestly-open known issue rather than
  a rushed patch.

### KEDA's Event Hubs scaler needs a blob checkpoint store even under managed identity
- **Assumed:** with managed-identity auth, the scaler wouldn't need a storage
  account.
- **Actually:** the `azure-eventhub` scaler requires a blob checkpoint
  container in every checkpoint strategy except `AzureFunction`.
- **Verified:** against the KEDA scaler docs.
- **Changed:** provisioned a near-zero-cost Standard LRS storage account whose
  sole purpose is that checkpoint container.

### An Entra token expiring mid-run looks like a *transport* error, not a 401
- **Assumed:** a long-running load would recover from token expiry by catching
  "401"/"Unauthorized" in the error string.
- **Actually:** when the token expires, Cosmos closes the websocket, which
  surfaces as a connection/transport error — the "401" match never fired, and
  every insert after expiry failed silently into a collected-failures list.
- **Verified:** the first full-graph load put all 142,395 vertices in, then
  failed ~548k of the FUNDS_TRANSFER edges; a fresh-token single insert of the
  *same* edge worked in 0.09s, isolating the cause to token lifecycle.
- **Changed:** the loader now refreshes the token proactively on a time budget
  (at drain-safe chunk boundaries, so nothing is mid-flight on a client being
  closed) and broadens reactive detection to transport errors. The re-run
  landed 1,930,094 / 1,930,979 edges (99.95%); the residual 885 were a
  one-second token-boundary race, documented not hidden.

---

## ML & inference

### The inference "compute cost" was mostly a data-structure bug, not compute
- **Assumed (since Chunk 7):** the per-batch inference latency (~9–17s) was
  dominated by the full-graph GNN forward pass, and the fix would be either
  2-hop subgraph extraction or a bigger/GPU SKU.
- **Actually:** profiling during the enterprise benchmark showed the pure
  forward pass is ~1.0s. The real ~11s cost was `tensors()` rebuilding the
  edge index by **sorting a 2.68-million-entry Python set on every batch**.
  Separately, the Cosmos score-writes were strictly sequential (~13–17s).
- **Verified:** timed the forward pass, the tensor rebuild, and the writes
  independently; and measured that a 2-hop subgraph around even *2* seed nodes
  pulls 23,664 nodes / 2.0M edges (59% of this unusually dense graph, avg
  degree ~67) — so the "correct" subgraph optimisation would cost *more*, not
  less. That approach was rejected **with data**.
- **Changed:** cached the edge tensor and appended new edges incrementally
  (11.4s → 26–47ms), and made the writes concurrent over a connection pool
  (13–17s → 0.6–2.2s). Per-batch latency fell from 21.96s to ~2.2s at full
  enterprise scale. It still does **not** meet the <300ms SLO — the 0.68s CPU
  forward alone exceeds it, with no GPU available — and that is reported as a
  fail, not softened.

### Leakage guard on the GNN features
- **Assumed (tempting):** `risk_base` is a useful node feature.
- **Actually:** `risk_base` is derived partly from the `isFraud` label, so
  using it would leak the target into the features.
- **Verified:** traced `risk_base`'s derivation in `graph_schema.py`.
- **Changed:** excluded it; the four node features are purely behavioural
  (tx_count, unique_counterparties, value_variance, device_assoc_count). Rings
  are held out *whole* (union-find over shared members) in the train/test split
  so GraphSAGE aggregation can't leak structural signal from train neighbours
  into test predictions.

---

## LLM & the compliance agent

### A "passing" groundedness guardrail that was passing on emptiness
- **Assumed (Chunk 8 follow-up):** raising the LLM's reasoning effort to HIGH
  would improve SAR drafts, and a 6/6 groundedness pass meant success.
- **Actually:** HIGH effort consumed the entire completion budget on hidden
  reasoning for 3 of 6 accounts and returned **empty** drafts — which the
  guardrail passed *vacuously* (no text → no entities → no violations to
  find). A green metric for the worst possible reason.
- **Verified:** inspected the actual draft contents behind the passing metric.
- **Changed:** the guardrail now hard-fails any draft under 200 characters
  ("empty or truncated"), and reasoning effort settled at MEDIUM with a larger
  completion budget. This became the canonical example, referenced in later
  sessions, of *why* you look behind a good-looking number.

### The intended model was quota-blocked at the subscription level
- **Assumed:** the agent could run on Claude (or full GPT-5) via Azure AI
  Foundry.
- **Actually:** every Claude model — and every flagship OpenAI tier (full
  gpt-5, 5.1, 5.2, 5.4, 5.5, 5.6) — has a hard 0-TPM quota on this credit-grant
  subscription; only the mini/small tiers carry quota.
- **Verified:** the deployment failed with `InsufficientQuota`, and
  `az cognitiveservices usage list` confirmed 0 TPM across the flagship models.
- **Changed:** deployed `gpt-5-mini` (native `azurerm_cognitive_deployment`,
  bills as normal Azure consumption, same Entra auth). Also found empirically
  that the deployment 400s (`DeploymentModelNotSupported`) if the model
  `version` is omitted — pinned to `2025-08-07`. This is documented as a
  settled, permanent constraint of the subscription, not an open to-do.

---

## The Rust ingestion engine

### The Event Hubs SDK's real API differed from training-data expectations
- **Assumed:** the credential type would be `DefaultAzureCredential` and the
  two client builders would have matching signatures.
- **Actually (azure_messaging_eventhubs 0.15):** the credential is
  `DeveloperToolsCredential` (that other name doesn't exist in this SDK
  generation), and `ProducerClient::open` takes `&str` while `ConsumerClient::open`
  takes an owned `String` — a genuine asymmetry between the two builders.
- **Verified:** read the crate's own tests/examples in the azure-sdk-for-rust
  GitHub repo (docs.rs wouldn't resolve via fetch) before writing any code.
- **Changed:** wrote against the real signatures; the code compiled and
  authenticated against live Event Hubs without the round of guess-and-fix a
  memory-based attempt would have needed.

### One event per network round-trip is a throughput cliff
- **Assumed:** `producer.send_event()` per enriched event was fine — it was,
  through every validation run of a few thousand events.
- **Actually:** at real load (the 590k-row corpus) it collapsed to **~15
  events/sec** and 6+ GB of resident memory, because each event took its own
  AMQP round-trip and the per-event spawned tasks piled up unbounded.
- **Verified:** the first full-corpus run had to be killed; the crate source
  confirmed a real batch API (`create_batch` / `try_add_event_data` /
  `send_batch`) existed.
- **Changed:** `EventHubSink` now batches internally behind the unchanged
  `Sink` trait — callers are none the wiser — taking throughput to
  ~15,000–21,000 events/sec. A later pass added bounded-concurrency pipelined
  dispatch (8 batches in flight via a semaphore, backpressuring rather than the
  unbounded spawn that caused the original blow-up).

### A dead-letter write that could silently evaporate
- **Assumed:** `tokio::fs::File::write_all` returning meant the record was
  persisted.
- **Actually:** tokio's `File` buffers internally; returning does not mean the
  bytes reached the OS. A dead-letter record — whose entire purpose is to not
  lose a failed event — could vanish in the buffer.
- **Verified:** a reproducible ~1-in-5 test flake where the just-written record
  wasn't readable back.
- **Changed:** every dead-letter write now flushes through; 8/8 repeat runs
  green after the fix.

---

## What this list is and isn't

None of these are exotic. They are the ordinary failure modes of building on
real cloud services and third-party libraries: docs that lag the product,
SDK signatures that don't match memory, spec documents that contradict each
other or don't compile, and performance cliffs that only appear at real
scale. The point of the project's "verify, don't assume" rule was never to
find something clever — it was to catch exactly this ordinary class of thing
*before* it cost a failed `apply` against billed resources or a
green-for-the-wrong-reason metric. The record above is that rule doing its
job.
