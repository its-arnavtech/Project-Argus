"""
Chunk 8 orchestrator: discovers flagged accounts by querying Cosmos directly
(g.V().has('gnn_risk_score', gt(threshold)) -- the Chunk 7 "cross-query, not
cross-queue" decision; the composite index on gnn_risk_score makes this
cheap), runs the LangGraph compliance pipeline for a SMALL subset of them,
and stores each SAR draft back onto the corresponding Account vertex.

Storage decision (logged in context.md): no Azure SQL Warehouse exists in
this build (the PDD diagram mentions one; Chunk 3 deliberately never
provisioned it and there's no budget justification to add one), so the SAR
draft lives as properties on the flagged vertex itself:
  sar_draft, sar_generated_at, sar_grounded, sar_model.

Usage:
    python agents/orchestrator.py                # top 6 flagged accounts
    python agents/orchestrator.py --limit 8
    python agents/orchestrator.py --threshold 0.5
"""
from __future__ import annotations

import argparse
import json
import sys
import time

# Windows consoles default to cp1252, which can't print the model's
# occasional typographic characters (e.g. U+2011 non-breaking hyphen).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from compliance_graph import (
    LLM_DEPLOYMENT,
    PARTITION_KEY_VALUE,
    build_compliance_graph,
    make_gremlin_client,
)


def discover_flagged(c, threshold: float, limit: int) -> list[dict]:
    rows = (
        c.submit(
            message=(
                "g.V().hasLabel('Account').has('partitionKey', pk)"
                ".has('gnn_risk_score', gt(th))"
                ".project('acct_id','score','is_ring')"
                ".by(values('acct_id')).by(values('gnn_risk_score')).by(values('is_ring_member'))"
            ),
            bindings={"pk": PARTITION_KEY_VALUE, "th": threshold},
        )
        .all()
        .result()
    )
    rows.sort(key=lambda r: r["score"], reverse=True)
    return rows[:limit]


def store_sar(c, acct_id: str, draft: str, grounded: bool) -> None:
    c.submit(
        message=(
            "g.V().has('Account','acct_id',aid).has('partitionKey',pk)"
            ".property('sar_draft', d)"
            ".property('sar_generated_at', ts)"
            ".property('sar_grounded', gr)"
            ".property('sar_model', m)"
        ),
        bindings={
            "aid": acct_id,
            "pk": PARTITION_KEY_VALUE,
            "d": draft,
            "ts": int(time.time()),
            "gr": grounded,
            "m": LLM_DEPLOYMENT,
        },
    ).all().result()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--limit", type=int, default=6)
    args = parser.parse_args()

    pipeline = build_compliance_graph()

    c = make_gremlin_client()
    try:
        flagged = discover_flagged(c, args.threshold, args.limit)
        print(f"[ORCH] {len(flagged)} flagged account(s) selected "
              f"(gnn_risk_score > {args.threshold}, top {args.limit}):")
        for f in flagged:
            print(f"[ORCH]   {f['acct_id']}  score={f['score']:.4f}  ring={f['is_ring']}")

        results = []
        for f in flagged:
            t0 = time.time()
            final = pipeline.invoke(
                {"target_node": f["acct_id"], "gnn_risk_score": round(float(f["score"]), 4),
                 "agent_findings": [], "regen_attempts": 0}
            )
            store_sar(c, f["acct_id"], final["sar_draft"], bool(final["grounded"]))
            results.append((f["acct_id"], final))
            status = "GROUNDED" if final["grounded"] else "UNGROUNDED/FAILED"
            print(f"[ORCH] {f['acct_id']}: {status} in {time.time()-t0:.1f}s "
                  f"(regen_attempts={final.get('regen_attempts', 0)}; "
                  f"violations={final.get('groundedness_violations', [])})")

        n_pass = sum(1 for _, r in results if r["grounded"])
        print(f"\n[ORCH] SAR drafts: {len(results)} generated, "
              f"{n_pass} grounded, {len(results) - n_pass} failed groundedness")

        if results:
            aid, example = results[0]
            print("\n" + "=" * 72)
            print(f"EXAMPLE -- EVIDENCE BUNDLE for {aid}:")
            print(json.dumps(example["evidence_bundle"], indent=2)[:4000])
            print("\n" + "=" * 72)
            print(f"EXAMPLE -- SAR DRAFT for {aid} "
                  f"(grounded={example['grounded']}):\n")
            print(example["sar_draft"])
    finally:
        c.close()


if __name__ == "__main__":
    main()
