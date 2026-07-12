# Example SAR Draft (real generated artifact)

Generated 2026-07-12 by the Chunk 8 LangGraph compliance pipeline
(agents/orchestrator.py) against the live Cosmos graph, model gpt-5-mini
via Azure AI Foundry. All 3 drafts in this batch passed the deterministic
groundedness guardrail on the first attempt (0 regenerations). This is the
unedited output for one flagged account, preserved here because the live
Azure deployment is torn down as of 2026-07-12 (credit grant expired).

```
EXAMPLE -- EVIDENCE BUNDLE for ACC-R00065:
{
  "target_account": "ACC-R00065",
  "gnn_risk_score": 1.0,
  "owner": {
    "cust_id": "CUST-R-CIRC-011-1",
    "kyc": "VERIFIED"
  },
  "graph_linkages": [
    {
      "neighbor": "ACC-R00066",
      "relationship": "funds_transfer_out",
      "hop_distance": 1,
      "amount": 17481.9
    },
    {
      "neighbor": "ACC-R00064",
      "relationship": "funds_transfer_in",
      "hop_distance": 1,
      "amount": 18678.89
    }
  ],
  "neighbor_gnn_scores": {
    "ACC-R00066": 0.9168,
    "ACC-R00064": 1.0
  },
  "behavioral_metrics": {
    "inbound_count": 1,
    "outbound_count": 1,
    "inbound_total": 18678.89,
    "outbound_total": 17481.9,
    "inbound_outbound_ratio": 0.517,
    "pass_through_symmetry": 0.936,
    "velocity_tx_per_hour": 184.615,
    "burst_tx_in_1h": 2,
    "amount_std": 846.4
  },
  "investigation_findings": [
    "Gremlin traversal from ACC-R00065: 0 shared-device linkage(s), 0 shared-IP linkage(s), 1 outbound and 1 inbound FUNDS_TRANSFER edge(s) in the loaded graph.",
    "Behavioral profile for ACC-R00065: 1 inbound / 1 outbound transfers; pass-through symmetry 0.936; burst of 2 transaction(s) within one hour."
  ]
}

========================================================================
EXAMPLE -- SAR DRAFT for ACC-R00065 (grounded=True):

SUBJECT: ACC-R00065 (CUST-R-CIRC-011-1; KYC: VERIFIED; GNN risk score: 1.0)

SUSPICIOUS ACTIVITY SUMMARY: ACC-R00065 has one inbound funds transfer of 18678.89 from ACC-R00064 (neighbor GNN score 1.0) and one outbound funds transfer of 17481.9 to ACC-R00066 (neighbor GNN score 0.9168). Behavioral metrics show a near-symmetric pass-through profile (pass_through_symmetry 0.936; inbound_outbound_ratio 0.517) with a burst of 2 transaction(s) within one hour and high velocity (velocity_tx_per_hour 184.615).

NARRATIVE: Gremlin traversal from ACC-R00065 shows "0 shared-device linkage(s), 0 shared-IP linkage(s), 1 outbound and 1 inbound FUNDS_TRANSFER edge(s) in the loaded graph." The account recorded inbound_count 1 (inbound_total 18678.89) and outbound_count 1 (outbound_total 17481.9) with amount_std 846.4, pass_through_symmetry 0.936, inbound_outbound_ratio 0.517, velocity_tx_per_hour 184.615 and burst_tx_in_1h 2. Linked neighbors are ACC-R00064 (funds_transfer_in; amount 18678.89; neighbor GNN score 1.0) and ACC-R00066 (funds_transfer_out; amount 17481.9; neighbor GNN score 0.9168). Investigation findings are limited to the loaded graph results and behavioral profile as stated; no shared-device or shared-IP linkages were identified.

RECOMMENDATION: Recommend further investigation and enhanced review of linked accounts ACC-R00064 and ACC-R00066.
```
