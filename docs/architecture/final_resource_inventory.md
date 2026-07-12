# Final Azure Resource Inventory — rg-argus-dev

Captured 2026-07-12T11:55:05Z — the last live snapshot before the credit grant expires (2026-07-12 23:59).

## Resources (az resource list)

```
Name                                     ResourceGroup    Location    Type                                           Status
---------------------------------------  ---------------  ----------  ---------------------------------------------  ---------
kv-argus-dev-to614f                      rg-argus-dev     eastus2     Microsoft.KeyVault/vaults                      Succeeded
cosmos-argus-dev-to614f                  rg-argus-dev     eastus2     Microsoft.DocumentDB/databaseAccounts          Succeeded
evhns-argus-dev-to614f                   rg-argus-dev     eastus2     Microsoft.EventHub/namespaces                  Succeeded
argus-dev-law                            rg-argus-dev     eastus2     Microsoft.OperationalInsights/workspaces       Succeeded
argus-dev-cae                            rg-argus-dev     eastus2     Microsoft.App/managedEnvironments              Succeeded
argus-dev-foundry-to614f                 rg-argus-dev     eastus2     Microsoft.CognitiveServices/accounts           Succeeded
argus-dev-foundry-to614f/argus-dev-proj  rg-argus-dev     eastus2     Microsoft.CognitiveServices/accounts/projects  Succeeded
stargusdevto614f                         rg-argus-dev     eastus2     Microsoft.Storage/storageAccounts              Succeeded
argus-ingestion                          rg-argus-dev     eastus2     Microsoft.App/containerApps                    Succeeded
acrargusdevto614f                        rg-argus-dev     eastus2     Microsoft.ContainerRegistry/registries         Succeeded
```

## Full detail (name / type / location / sku)

[
  {
    "location": "eastus2",
    "name": "kv-argus-dev-to614f",
    "sku": null,
    "type": "Microsoft.KeyVault/vaults"
  },
  {
    "location": "eastus2",
    "name": "cosmos-argus-dev-to614f",
    "sku": null,
    "type": "Microsoft.DocumentDB/databaseAccounts"
  },
  {
    "location": "eastus2",
    "name": "evhns-argus-dev-to614f",
    "sku": "Standard",
    "type": "Microsoft.EventHub/namespaces"
  },
  {
    "location": "eastus2",
    "name": "argus-dev-law",
    "sku": null,
    "type": "Microsoft.OperationalInsights/workspaces"
  },
  {
    "location": "eastus2",
    "name": "argus-dev-cae",
    "sku": null,
    "type": "Microsoft.App/managedEnvironments"
  },
  {
    "location": "eastus2",
    "name": "argus-dev-foundry-to614f",
    "sku": "S0",
    "type": "Microsoft.CognitiveServices/accounts"
  },
  {
    "location": "eastus2",
    "name": "argus-dev-foundry-to614f/argus-dev-proj",
    "sku": null,
    "type": "Microsoft.CognitiveServices/accounts/projects"
  },
  {
    "location": "eastus2",
    "name": "stargusdevto614f",
    "sku": "Standard_LRS",
    "type": "Microsoft.Storage/storageAccounts"
  },
  {
    "location": "eastus2",
    "name": "argus-ingestion",
    "sku": null,
    "type": "Microsoft.App/containerApps"
  },
  {
    "location": "eastus2",
    "name": "acrargusdevto614f",
    "sku": "Basic",
    "type": "Microsoft.ContainerRegistry/registries"
  }
]

## Final Spend Summary

Two independent figures, clearly labeled — they do NOT agree because the
Cost Management API lags real usage by 8-24h and had not yet posted the
enterprise-benchmark hours at capture time.

### (A) Azure Cost Management API — rg-argus-dev, month-to-date
Captured 2026-07-12 ~11:55 UTC. **This UNDER-reports** (lagged):

| Service | MTD cost |
|---|---|
| Event Hubs | $2.53 |
| Foundry Models (gpt-5-mini) | $0.09 |
| Container Registry | $0.06 |
| Key Vault / Storage | ~$0.00 |
| Cosmos DB | $0.00 *(lag — the 7h at 10k RU/s had not posted)* |
| **API total** | **$2.68** |

### (B) Claude Code runtime-based estimates (the truer near-term figure)
- **Chunk 11 load test**: temporary 12 TU + 5 replicas for a bounded
  window, reverted same session. Small (minutes of elevated TU) — a few $.
- **Inter-chunk enterprise benchmark (2026-07-12)**: Premium EH 4 PU
  ($4.108/hr) + Cosmos +9,000 RU/s ($0.72/hr) for ~7.05h ≈ **$34.06**
  incremental. Overran the plan (~$25-30) because Premium sat provisioned
  during a broken token-expiry load and long idle gaps — documented
  honestly in context.md.
- **Chunk 12 (this session)**: queries + 315 Cosmos writes + 3 gpt-5-mini
  SAR generations ≈ **<$0.10**.

**Best estimate of total grant spend across the whole project: ~$40-45**
of the ~$100 grant. The API figure will converge toward this over the next
day (but the grant expires 2026-07-12 23:59, so the final posted number may
never fully catch up in the portal).

## Scale-Down Note (Step 0.5)
Container App `argus-ingestion` was already at minimum (min 0 / max 1
replicas — scales to zero when idle). Event Hubs `evhns-argus-dev-to614f`
was already at the Standard-tier floor of 1 TU. No change was needed or
made; both were left at minimum by the Chunk 11 revert. (Moot regardless:
the grant expires tonight and all resources deprovision with it.)
