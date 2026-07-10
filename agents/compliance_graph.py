"""
Chunk 8: LangGraph agentic compliance loop -- the real implementation of
POC_Blueprint.md section 4, replacing every hardcoded/simulated output:

  - NetworkTracerAgent  -> real Gremlin traversals against
    argus-graph-container (Entra-token auth, same pattern as graph/loader.py
    -- no account key).
  - BehavioralAnalystAgent -> real per-account metrics computed from the
    actual FUNDS_TRANSFER data (velocity, inbound/outbound ratio,
    pass-through symmetry, amount variance).
  - ComplianceSARGeneratorAgent -> real LLM call to the Foundry deployment
    (gpt-5-mini-argus; Claude Opus 4.8 was quota-blocked on this
    subscription -- see context.md), grounded ONLY in the evidence bundle.

Orchestrated as an actual LangGraph StateGraph (nodes + a conditional
edge), not the POC's manual sequential .execute() chaining.

GROUNDEDNESS GUARDRAIL (mandatory): after generation, every specific
entity/number the draft references (account/customer IDs, device hashes,
IPs, dollar amounts, counts) is validated against the evidence bundle that
was passed into the prompt. A draft referencing anything not present in
evidence is marked ungrounded; one regeneration attempt (with the
violations fed back) is allowed, after which the draft is flagged as
FAILED rather than silently accepted. A compliance report that invents a
detail is a real failure mode, not a cosmetic one.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, TypedDict

import pandas as pd
from azure.identity import DefaultAzureCredential
from gremlin_python.driver import client as gclient
from gremlin_python.driver import serializer
from langgraph.graph import END, StateGraph
from openai import OpenAI

REPO_ROOT = Path(__file__).resolve().parents[1]
SIM_DIR = REPO_ROOT / "data" / "simulated"

COSMOS_ACCOUNT = "cosmos-argus-dev-to614f"
DATABASE = "argus-graph"
GRAPH = "argus-graph-container"
PARTITION_KEY_VALUE = "argus"

FOUNDRY_OPENAI_V1 = "https://argus-dev-foundry-to614f.services.ai.azure.com/openai/v1/"
LLM_DEPLOYMENT = "gpt-5-mini-argus"

_credential = DefaultAzureCredential()


def make_gremlin_client() -> gclient.Client:
    token = _credential.get_token("https://cosmos.azure.com/.default").token
    return gclient.Client(
        url=f"wss://{COSMOS_ACCOUNT}.gremlin.cosmos.azure.com:443/",
        traversal_source="g",
        username=f"/dbs/{DATABASE}/colls/{GRAPH}",
        password=token,
        message_serializer=serializer.GraphSONSerializersV2d0(),
    )


def make_llm_client() -> OpenAI:
    token = _credential.get_token("https://cognitiveservices.azure.com/.default").token
    return OpenAI(base_url=FOUNDRY_OPENAI_V1, api_key=token)


class AgentState(TypedDict, total=False):
    target_node: str
    gnn_risk_score: float
    graph_context: list[dict[str, Any]]
    behavioral_metrics: dict[str, Any]
    agent_findings: list[str]
    evidence_bundle: dict[str, Any]
    sar_draft: str
    grounded: bool
    groundedness_violations: list[str]
    regen_attempts: int
    audit_approved: bool


# ---------------------------------------------------------------------------
# Agent 1: Network Tracer -- real multi-hop Gremlin traversals
# ---------------------------------------------------------------------------
def network_tracer_agent(state: AgentState) -> dict[str, Any]:
    aid = state["target_node"]
    c = make_gremlin_client()
    try:
        def q(query: str, bindings: dict | None = None):
            b = {"aid": aid, "pk": PARTITION_KEY_VALUE}
            if bindings:
                b.update(bindings)
            return c.submit(message=query, bindings=b).all().result()

        linkages: list[dict[str, Any]] = []

        shared_dev = q(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk).out('USED_DEVICE').as('d')"
            ".in('USED_DEVICE').has('acct_id', neq(aid)).as('n')"
            ".select('d','n').by(values('device_hash')).by(values('acct_id')).dedup()"
        )
        for row in shared_dev:
            linkages.append(
                {"neighbor": row["n"], "relationship": "shared_device",
                 "hop_distance": 2, "via": row["d"]}
            )

        shared_ip = q(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk).out('ACCESSED_FROM').as('i')"
            ".in('ACCESSED_FROM').has('acct_id', neq(aid)).as('n')"
            ".select('i','n').by(values('ip_string')).by(values('acct_id')).dedup()"
        )
        for row in shared_ip:
            linkages.append(
                {"neighbor": row["n"], "relationship": "shared_ip",
                 "hop_distance": 2, "via": row["i"]}
            )

        transfers_out = q(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk).outE('FUNDS_TRANSFER').as('e')"
            ".inV().as('n').select('e','n').by(values('amount')).by(values('acct_id'))"
        )
        for row in transfers_out:
            linkages.append(
                {"neighbor": row["n"], "relationship": "funds_transfer_out",
                 "hop_distance": 1, "amount": round(float(row["e"]), 2)}
            )

        transfers_in = q(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk).inE('FUNDS_TRANSFER').as('e')"
            ".outV().as('n').select('e','n').by(values('amount')).by(values('acct_id'))"
        )
        for row in transfers_in:
            linkages.append(
                {"neighbor": row["n"], "relationship": "funds_transfer_in",
                 "hop_distance": 1, "amount": round(float(row["e"]), 2)}
            )

        owner = q(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk).in('OWNS')"
            ".project('cust_id','kyc').by(values('cust_id')).by(values('KYC_status'))"
        )

        neighbor_ids = sorted({l["neighbor"] for l in linkages})
        neighbor_scores = {}
        if neighbor_ids:
            rows = q(
                "g.V().hasLabel('Account').has('partitionKey',pk).has('acct_id', within(nids))"
                ".has('gnn_risk_score')"
                ".project('a','s').by(values('acct_id')).by(values('gnn_risk_score'))",
                {"nids": neighbor_ids},
            )
            neighbor_scores = {r["a"]: round(float(r["s"]), 4) for r in rows}

        findings = [
            f"Gremlin traversal from {aid}: {len(shared_dev)} shared-device linkage(s), "
            f"{len(shared_ip)} shared-IP linkage(s), {len(transfers_out)} outbound and "
            f"{len(transfers_in)} inbound FUNDS_TRANSFER edge(s) in the loaded graph."
        ]
        return {
            "graph_context": linkages,
            "agent_findings": state.get("agent_findings", []) + findings,
            "evidence_bundle": {
                **state.get("evidence_bundle", {}),
                "owner": owner[0] if owner else None,
                "neighbor_gnn_scores": neighbor_scores,
            },
        }
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Agent 2: Behavioral Analyst -- real metrics from the transaction corpus
# ---------------------------------------------------------------------------
_FT_CACHE: pd.DataFrame | None = None


def _ft() -> pd.DataFrame:
    global _FT_CACHE
    if _FT_CACHE is None:
        _FT_CACHE = pd.read_parquet(
            SIM_DIR / "edges_funds_transfer.parquet",
            columns=["src_acct_id", "dst_acct_id", "amount", "timestamp"],
        )
    return _FT_CACHE


def behavioral_analyst_agent(state: AgentState) -> dict[str, Any]:
    aid = state["target_node"]
    ft = _ft()
    out_tx = ft[ft["src_acct_id"] == aid]
    in_tx = ft[ft["dst_acct_id"] == aid]

    out_total = float(out_tx["amount"].sum())
    in_total = float(in_tx["amount"].sum())
    all_ts = pd.concat([out_tx["timestamp"], in_tx["timestamp"]])
    n_tx = len(out_tx) + len(in_tx)

    window_hours = max((all_ts.max() - all_ts.min()) / 3600.0, 1e-9) if n_tx > 1 else 0.0
    velocity_per_hour = round(n_tx / window_hours, 3) if window_hours > 0 else float(n_tx)

    # burst velocity: most transactions inside any rolling 1h window
    burst = 0
    ts_sorted = sorted(all_ts.tolist())
    j = 0
    for i in range(len(ts_sorted)):
        while ts_sorted[i] - ts_sorted[j] > 3600:
            j += 1
        burst = max(burst, i - j + 1)

    denom = in_total + out_total
    metrics = {
        "inbound_count": int(len(in_tx)),
        "outbound_count": int(len(out_tx)),
        "inbound_total": round(in_total, 2),
        "outbound_total": round(out_total, 2),
        "inbound_outbound_ratio": round(in_total / denom, 3) if denom > 0 else 0.0,
        "pass_through_symmetry": round(
            min(in_total, out_total) / max(in_total, out_total), 3
        ) if min(in_total, out_total) > 0 else 0.0,
        "velocity_tx_per_hour": velocity_per_hour,
        "burst_tx_in_1h": int(burst),
        "amount_std": round(float(pd.concat([out_tx["amount"], in_tx["amount"]]).std() or 0.0), 2)
        if n_tx > 1 else 0.0,
    }
    findings = [
        f"Behavioral profile for {aid}: {metrics['inbound_count']} inbound / "
        f"{metrics['outbound_count']} outbound transfers; pass-through symmetry "
        f"{metrics['pass_through_symmetry']}; burst of {metrics['burst_tx_in_1h']} "
        f"transaction(s) within one hour."
    ]
    return {
        "behavioral_metrics": metrics,
        "agent_findings": state.get("agent_findings", []) + findings,
    }


# ---------------------------------------------------------------------------
# Agent 3: Compliance SAR Generator -- real LLM call, evidence-grounded
# ---------------------------------------------------------------------------
SAR_SYSTEM_PROMPT = """You are a BSA/AML compliance analyst drafting a Suspicious Activity Report (SAR) narrative.

STRICT GROUNDING RULES -- violations invalidate the report:
1. Use ONLY entities (account IDs, customer IDs, device hashes, IP addresses) and numbers (amounts, counts, scores, ratios) that appear verbatim in the EVIDENCE JSON.
2. Do NOT invent, estimate, extrapolate, or round any figure. Quote numbers exactly as they appear in the evidence.
3. Do NOT reference institutions, dates, jurisdictions, or people that are not in the evidence.
4. If the evidence is insufficient for a section, state that explicitly instead of filling the gap.

Structure the draft as:
SUBJECT: <target account and owning customer>
SUSPICIOUS ACTIVITY SUMMARY: <2-3 sentences>
NARRATIVE: <one paragraph tying graph linkages and behavioral metrics together>
RECOMMENDATION: <one sentence>"""


def sar_generator_agent(state: AgentState) -> dict[str, Any]:
    evidence = {
        "target_account": state["target_node"],
        "gnn_risk_score": state["gnn_risk_score"],
        "owner": state.get("evidence_bundle", {}).get("owner"),
        "graph_linkages": state["graph_context"],
        "neighbor_gnn_scores": state.get("evidence_bundle", {}).get("neighbor_gnn_scores", {}),
        "behavioral_metrics": state["behavioral_metrics"],
        "investigation_findings": state["agent_findings"],
    }

    user_prompt = f"EVIDENCE JSON:\n{json.dumps(evidence, indent=2)}\n\nDraft the SAR narrative now."
    if state.get("groundedness_violations"):
        user_prompt += (
            "\n\nYour previous draft was REJECTED for referencing details not present in the "
            "evidence: " + "; ".join(state["groundedness_violations"])
            + "\nRegenerate using only evidence values."
        )

    llm = make_llm_client()
    kwargs: dict[str, Any] = dict(
        model=LLM_DEPLOYMENT,
        messages=[
            {"role": "system", "content": SAR_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        max_completion_tokens=3000,
    )
    try:
        resp = llm.chat.completions.create(**kwargs, reasoning_effort="low")
    except Exception:
        resp = llm.chat.completions.create(**kwargs)

    return {
        "sar_draft": resp.choices[0].message.content or "",
        "evidence_bundle": evidence,
        "regen_attempts": state.get("regen_attempts", 0) + (1 if state.get("groundedness_violations") else 0),
    }


# ---------------------------------------------------------------------------
# Groundedness guardrail -- deterministic post-generation validation
# ---------------------------------------------------------------------------
_ENTITY_PATTERNS = [
    re.compile(r"\bACC-[A-Za-z0-9]+\b"),
    re.compile(r"\bCUST-[A-Za-z0-9-]+\b"),
    re.compile(r"\bDEV-[A-Za-z0-9]+\b"),
    re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
]
_NUMBER_PATTERN = re.compile(r"\$?\b\d[\d,]*(?:\.\d+)?\b")
_ENUMERATION = re.compile(r"^\s*\d+[.)]\s|\bSection\s+\d+\b|\bV\b", re.IGNORECASE)


def _numbers_in(obj: Any, acc: set[float]) -> None:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        acc.add(round(float(obj), 6))
    elif isinstance(obj, str):
        for m in _NUMBER_PATTERN.finditer(obj):
            try:
                acc.add(round(float(m.group().lstrip("$").replace(",", "")), 6))
            except ValueError:
                pass
    elif isinstance(obj, dict):
        for v in obj.values():
            _numbers_in(v, acc)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _numbers_in(v, acc)


def _number_allowed(value: float, allowed: set[float]) -> bool:
    if value in allowed or round(value, 6) in allowed:
        return True
    # Accept faithful roundings/percentage forms of an evidence value
    # (e.g. draft "0.99" for evidence 0.994, or "99.4%" for 0.994).
    for a in allowed:
        for candidate in (a, a * 100):
            for nd in range(0, 5):
                if abs(round(candidate, nd) - value) < 1e-9:
                    return True
    return False


def groundedness_guardrail(state: AgentState) -> dict[str, Any]:
    draft = state.get("sar_draft", "")
    evidence_text = json.dumps(state.get("evidence_bundle", {}))
    violations: list[str] = []

    for pat in _ENTITY_PATTERNS:
        for m in set(pat.findall(draft)):
            if m not in evidence_text:
                violations.append(f"entity not in evidence: {m}")

    allowed: set[float] = set()
    _numbers_in(state.get("evidence_bundle", {}), allowed)
    for line in draft.splitlines():
        probe = line
        enum = _ENUMERATION.match(line)
        if enum:
            probe = line[enum.end():]
        for m in _NUMBER_PATTERN.finditer(probe):
            raw = m.group().lstrip("$").replace(",", "")
            try:
                val = float(raw)
            except ValueError:
                continue
            if not _number_allowed(val, allowed):
                violations.append(f"number not in evidence: {m.group()}")

    grounded = not violations
    return {
        "grounded": grounded,
        "groundedness_violations": sorted(set(violations)),
        "audit_approved": grounded,
    }


def _route_after_guardrail(state: AgentState) -> str:
    if state.get("grounded"):
        return END
    if state.get("regen_attempts", 0) >= 1:
        return END  # flagged as failed; do not loop forever
    return "sar_generator"


def build_compliance_graph():
    """The real LangGraph StateGraph: tracer -> analyst -> generator ->
    guardrail, with a conditional retry edge from the guardrail back to the
    generator (max 1 regeneration)."""
    g = StateGraph(AgentState)
    g.add_node("network_tracer", network_tracer_agent)
    g.add_node("behavioral_analyst", behavioral_analyst_agent)
    g.add_node("sar_generator", sar_generator_agent)
    g.add_node("groundedness_guardrail", groundedness_guardrail)

    g.set_entry_point("network_tracer")
    g.add_edge("network_tracer", "behavioral_analyst")
    g.add_edge("behavioral_analyst", "sar_generator")
    g.add_edge("sar_generator", "groundedness_guardrail")
    g.add_conditional_edges("groundedness_guardrail", _route_after_guardrail)
    return g.compile()
