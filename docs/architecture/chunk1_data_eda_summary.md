# Chunk 1 — Data Acquisition & Ring Injection: EDA Summary

Generated from `data/simulated/` (acquisition data_source: `real_kaggle`, seed: 42).

## Real vs. Synthetic Row Counts

| Table | real_kaggle | synthetic_ring | Total |
|---|---|---|---|
| accounts | 39974 | 315 | 40289 |
| customers | 39974 | 315 | 40289 |
| devices | 1786 | 10 | 1796 |
| ip_addresses | 59961 | 10 | 59971 |
| merchants | 50 | 0 | 50 |
| funds_transfer | 590540 | 320 | 590860 |
| accessed_from | 590540 | 42 | 590582 |
| used_device | 118666 | 42 | 118708 |
| settled_at | 590540 | 0 | 590540 |
| owns | 39974 | 315 | 40289 |

## Class Balance (Account vertices)

- Total accounts: **40,289**
- Ring-member accounts: **315** (0.78%)
- Non-member (background) accounts: **39,974** (99.22%)

## Ring Topology Stats

| Ring type | Count | Avg size (accounts) | Avg hop distance |
|---|---|---|---|
| circular | 20 | 6.05 | 2.15 |
| device_cluster | 10 | 4.20 | N/A (device-shared only) |
| smurfing | 15 | 11.60 | 1.82 |

> **"Avg hop distance" here = intra-ring path length** of an injected ring
> (a property of the ring's own topology). This is **not** the same metric as
> the Tableau extract's `hop_distance` column, which is BFS distance *from
> flagged accounts* across the whole graph. Same term, two definitions — see
> Issue #6.

- Total rings injected: **45**
- Total ring-member accounts: **315**
