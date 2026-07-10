"""
Chunk 7 validation prep: writes a filtered JSONL of raw transactions whose
BOTH endpoints are accounts already loaded into Cosmos DB by Chunk 5's
loader (same deterministic subset selection, same seed), so the Rust
ingestion binary can send events that land on real, connected graph nodes
rather than orphans.

Usage:
    python ml/inference/prepare_validation_events.py
    # then send through the real Rust pipeline:
    #   ARGUS_SINK=eventhub ARGUS_INPUT_JSONL=data/simulated/validation_events.jsonl \
    #   ARGUS_EVENT_LIMIT=400 ingestion/target/release/ingestion.exe
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "graph"))

from loader import select_subset, SEED  # noqa: E402

SIM_DIR = REPO_ROOT / "data" / "simulated"
OUT = SIM_DIR / "validation_events.jsonl"
N_EVENTS = 400


def main() -> None:
    subset = select_subset(np.random.default_rng(SEED))
    accounts_df, _ = subset["vertices"]["Account"]
    loaded = set(accounts_df["acct_id"])
    # bias toward ring members so scores land on the interesting nodes too
    ring_loaded = set(accounts_df.loc[accounts_df["is_ring_member"], "acct_id"])

    kept, ring_kept = [], 0
    with open(SIM_DIR / "funds_transfer_raw.jsonl", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec["source_account"] in loaded and rec["target_account"] in loaded:
                is_ring = rec["source_account"] in ring_loaded or rec["target_account"] in ring_loaded
                kept.append((is_ring, line))
                ring_kept += int(is_ring)

    ring_lines = [ln for r, ln in kept if r]
    legit_lines = [ln for r, ln in kept if not r]
    selected = ring_lines[: N_EVENTS // 2] + legit_lines[: N_EVENTS - min(len(ring_lines), N_EVENTS // 2)]

    with open(OUT, "w", encoding="utf-8") as f:
        f.writelines(selected)
    print(
        f"[PREP] {len(selected)} validation events -> {OUT} "
        f"({min(len(ring_lines), N_EVENTS // 2)} ring-touching of {ring_kept} available)"
    )


if __name__ == "__main__":
    main()
