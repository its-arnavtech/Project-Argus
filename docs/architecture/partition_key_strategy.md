# Cosmos DB Gremlin Partition Key Strategy

## The decision

The `argus-graph-container` Gremlin graph uses a single, low-cardinality,
shared partition key path (`/partitionKey`) across **every** vertex and edge
type — Account, Customer, Device, IPAddress, Merchant, and all five edge
labels (FUNDS_TRANSFER, ACCESSED_FROM, USED_DEVICE, SETTLED_AT, OWNS). It is
not one key per vertex label, and it is not a naturally high-cardinality key
like `acct_id`.

`docs/specs/PDD_Production_Guide.md` section 1's schema table has a
"Partition Key" column listed per vertex/edge row (e.g. `/acct_id` for
Account, `/device_hash` for Device). That table is being reinterpreted here
as **indexing policy guidance**, not literal per-container partition key
assignments — Cosmos DB's Gremlin API models an entire graph as one
container with one partition key, full stop. There is no mechanism for
"one container per vertex label" in a single logical graph; splitting
vertex labels across containers would mean the graph itself is split
across containers, which breaks single-traversal queries entirely (a
Gremlin traversal can't hop from a container-A vertex to a container-B
vertex in one query). So the PDD table's per-row partition key suggestions
are read here as "these are the properties worth indexing for that
label's typical query pattern" (see `index_policy`'s `composite_index`
blocks in `infra/modules/cosmos_db/main.tf`), not as literal partitioning
instructions.

## Why a low-cardinality shared key, not per-entity keys

A natural instinct is to partition by `acct_id` (or similar high-cardinality
per-entity ID) for even data distribution and horizontal write scaling.
That's the wrong tradeoff at this project's scale:

- **Data volume is small.** Chunk 1's real corpus is ~40K accounts and
  ~590K background edges plus a few hundred synthetic ring vertices/edges —
  well under one physical partition's ~50 GB / 10,000 RU/s capacity. There
  is no scale problem to solve with horizontal sharding.
- **Every fraud-detection traversal is inherently cross-entity.** Ring
  detection, multi-hop tracing, and shared-device/IP clustering all walk
  from one vertex to its neighbors and their neighbors. If the partition
  key were `acct_id`, a single 2-hop traversal (e.g. "find all accounts
  sharing a device with this account") would almost always cross partition
  boundaries — Cosmos DB Gremlin cross-partition traversals are
  significantly more expensive (in RU cost and latency) than same-partition
  ones, since the query engine has to fan out to multiple physical
  partitions and merge results.
- **A shared low-cardinality key keeps every traversal same-partition.**
  With one partition key value shared by (effectively) every document, the
  entire graph lives in one logical partition. Every traversal, no matter
  how many hops, stays local. This is the query-performance property that
  actually matters for this workload.

The explicit tradeoff: this gives up horizontal write scaling and the
ability to grow past one physical partition's ~20 GB logical partition
limit without hitting a ceiling. That's the right trade at ~40K accounts.
It stops being the right trade if this graph grows to genuinely
bank-scale account volumes — at that point, revisit with a real
partitioning strategy (e.g. hierarchical partition keys, or splitting by a
coarser dimension like region or account-opening cohort), not before.

## What this means for the loader (Chunk 5, still ahead)

Every vertex and edge document the loader writes needs a `partitionKey`
property. Since this key is deliberately low-cardinality/shared, the
simplest correct choice is a single constant value (e.g. `"argus"`) stamped
onto every document, or a small number of buckets if a future chunk decides
some coarse-grained separation is worth it. That decision belongs to
whoever implements the loader, informed by this note — it is not decided
here, since Terraform provisions the container's partition key *path*, not
the *values* that will populate it.
