# Ingestion Service Observability (Chunk 10)

## How logs flow (verified, not assumed)

The Container Apps **environment** `argus-dev-cae` was provisioned (Chunk 3)
with `logs_destination = "log-analytics"` bound to workspace `argus-dev-law`.
For Container Apps, that environment-level setting IS the log wiring:
every app in the environment ships `stdout`/`stderr` to the
`ContainerAppConsoleLogs_CL` table and lifecycle events to
`ContainerAppSystemLogs_CL` automatically. There is no separate
per-app `azurerm_monitor_diagnostic_setting` needed for console logs —
that resource pattern applies to control-plane diagnostics on other
service types, not Container Apps console output.

The ingestion binary's log lines are deliberately greppable:
`[INITIALIZATION]`, `[DRAIN COMPLETE]`, `[SERVICE]`, `[INGESTION ERROR]`,
`[EVENTHUB_SINK]`, `[DEAD-LETTER FAILURE]`.

No Azure Monitor dashboard/workbook is built out here — not budget-justified
(Log Analytics ingestion itself is the only cost, pennies at this volume;
the first 5 GB/month per workspace are free on the default pricing tier).
These saved queries are the scope.

## Saved queries (Log Analytics, workspace `argus-dev-law`)

### 1. Ingestion throughput (events drained per run)

```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "argus-ingestion"
| where Log_s has "[DRAIN COMPLETE]"
| parse Log_s with * "Ingested " events:long " events" *
| project TimeGenerated, events
| order by TimeGenerated desc
```

### 2. Error rate (ingestion + sink errors per 15 min)

```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "argus-ingestion"
| where Log_s has "[INGESTION ERROR]" or Log_s has "[EVENTHUB_SINK] send failed"
| summarize errors = count() by bin(TimeGenerated, 15m)
| order by TimeGenerated desc
```

### 3. Dead-letter incidents (should be zero; each line is a lost-send)

```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "argus-ingestion"
| where Log_s has "[DEAD-LETTER FAILURE]" or Log_s has "Failed to forward to sink"
| project TimeGenerated, Log_s
| order by TimeGenerated desc
```

### 4. Replica lifecycle (scale-from-zero activations, restarts)

```kusto
ContainerAppSystemLogs_CL
| where ContainerAppName_s == "argus-ingestion"
| where Reason_s in ("StartingContainer", "ContainerCreated", "ScaleUp", "ScaleDown")
    or Log_s has "Scaling"
| project TimeGenerated, Reason_s, Log_s
| order by TimeGenerated desc
```

### 5. Service heartbeat gaps (idle service should tick every 5 min)

```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "argus-ingestion"
| where Log_s has "[SERVICE] heartbeat"
| summarize last_heartbeat = max(TimeGenerated)
| extend minutes_since = datetime_diff("minute", now(), last_heartbeat)
```

> Caveat: `_CL` custom-table names and column suffixes (`_s`) reflect the
> classic Container Apps log schema. If the workspace ever migrates to the
> dedicated-table schema, the table names become `ContainerAppConsoleLogs`
> / `ContainerAppSystemLogs` without suffixes — same fields, adjust
> accordingly. Queries were authored against the documented schema; they
> get their first live validation once the app has produced logs.

## TLS posture (Chunk 10 verification — reported honestly)

PDD section 5 says "active transport paths secured by TLS 1.3 tunnels."
What the platform actually allows, checked against the live resources:

| Service | Configurable floor | Actual setting | TLS 1.3? |
|---|---|---|---|
| Event Hubs `evhns-argus-dev-to614f` | `minimumTlsVersion`, max assignable floor is **1.2** | `1.2` | Negotiated opportunistically by modern clients; cannot be enforced as a floor |
| Cosmos DB `cosmos-argus-dev-to614f` | `minimalTlsVersion`, allowed values top out at **Tls12** | `Tls12` | Same — no Tls13 floor option exists |
| Key Vault `kv-argus-dev-to614f` | **No TLS property exposed at all** | platform-managed | Azure enforces ≥1.2 service-side (1.0/1.1 retired platform-wide Aug 2025) |

Honest conclusion: **"TLS 1.2 minimum, enforced everywhere; TLS 1.3
negotiated where both ends support it"** is the true, achievable posture.
The PDD's literal "TLS 1.3 tunnels" is not independently configurable on
any of these three services today; claiming TLS-1.3-enforced compliance
would be false. All traffic is nonetheless encrypted in transit at ≥1.2.
