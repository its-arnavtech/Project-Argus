# Chunk 1 — Data Acquisition & Ring Injection: EDA Summary

Generated from `data/simulated/` (acquisition data_source: `bundled_sample`, seed: 42).

## Real vs. Synthetic Row Counts

| Table | bundled_sample | synthetic_ring | Total |
|---|---|---|---|
| accounts | 14597 | 315 | 14912 |
| customers | 14597 | 315 | 14912 |
| devices | 6 | 10 | 16 |
| ip_addresses | 21895 | 10 | 21905 |
| merchants | 50 | 0 | 50 |
| funds_transfer | 15000 | 320 | 15320 |
| accessed_from | 15000 | 42 | 15042 |
| used_device | 3699 | 42 | 3741 |
| settled_at | 15000 | 0 | 15000 |

## Class Balance (Account vertices)

- Total accounts: **14,912**
- Ring-member accounts: **315** (2.11%)
- Non-member (background) accounts: **14,597** (97.89%)

## Ring Topology Stats

| Ring type | Count | Avg size (accounts) | Avg hop distance |
|---|---|---|---|
| circular | 20 | 6.05 | 2.15 |
| device_cluster | 10 | 4.20 | N/A (device-shared only) |
| smurfing | 15 | 11.60 | 1.82 |

- Total rings injected: **45**
- Total ring-member accounts: **315**
