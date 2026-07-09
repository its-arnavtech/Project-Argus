"""
Chunk 1: IEEE-CIS Fraud Detection data acquisition.

Pulls the real `ieee-fraud-detection` Kaggle competition dataset (~590K
labeled transactions, real card-not-present fraud) via the Kaggle API.

No public dataset labels multi-hop mule/syndicate rings -- that ground
truth doesn't exist in the wild, which is exactly why ring_injector.py
layers a synthetic, labeled ring topology on top of this real transaction
backbone in a later step of this chunk.

Credential detection note: the installed `kaggle` package (v2.2.3) does
NOT use the classic ~/.kaggle/kaggle.json (username+key) file that older
kaggle package versions and most tutorials describe. It uses either:
  - OAuth login (`kaggle auth login`), cached locally, or
  - a KAGGLE_API_TOKEN environment variable, or
  - a token saved to ~/.kaggle/access_token
This script checks for the *current* mechanism (plus the legacy
kaggle.json path, in case an older kaggle package is installed instead)
and prints the exact real setup steps if nothing is found. It never
raises on missing credentials -- it falls back to a small, schema-
compatible bundled sample so nothing downstream is blocked.
"""
from __future__ import annotations

import json
import os
import sys
import time
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
COMPETITION = "ieee-fraud-detection"

SEED = 42
N_BACKGROUND_TX = 15_000
N_UNIQUE_CARDS = 5_000
IDENTITY_JOIN_RATE = 0.25  # real dataset: only ~24% of transactions have identity rows


def kaggle_credentials_present() -> bool:
    if os.environ.get("KAGGLE_API_TOKEN"):
        return True
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    kaggle_dir = Path.home() / ".kaggle"
    if (kaggle_dir / "access_token").exists():
        return True
    if (kaggle_dir / "credentials.json").exists():
        return True
    if (kaggle_dir / "kaggle.json").exists():
        return True
    return False


def print_setup_instructions() -> None:
    print(
        """
[ACQUISITION] Kaggle credentials not found -- falling back to a bundled sample.

To pull the real ~590K-row ieee-fraud-detection dataset, set up credentials
with the CURRENTLY INSTALLED kaggle package (v2.2.3+, OAuth-based -- this is
NOT the classic kaggle.json workflow most tutorials describe):

  Option A (recommended) -- OAuth login, no token file to manage:
    kaggle auth login
    (observed to write ~/.kaggle/credentials.json -- also checked here)

  Option B -- API token:
    1. Go to https://www.kaggle.com/settings/api and click "Generate New Token"
    2. Either:
       - export KAGGLE_API_TOKEN=<token>          (env var), or
       - save the token to ~/.kaggle/access_token  (file)

  Note: if the `kaggle` command isn't on PATH after install, use
  `python -m kaggle ...` instead.

  You must also accept the competition rules at
  https://www.kaggle.com/c/ieee-fraud-detection/rules before download works.

  (If you're using an older kaggle package (<2.0) instead, the legacy
  ~/.kaggle/kaggle.json {"username":...,"key":...} file is still checked
  and will work.)

Rerun this script after setting up credentials to replace the bundled
sample with the real dataset.
""",
        file=sys.stderr,
    )


def download_real_dataset(raw_dir: Path) -> bool:
    try:
        import kaggle  # noqa: F401 -- import deferred: raises if unauthenticated

        kaggle.api.authenticate()
        # Download only the two files we actually consume -- competition_download_files()
        # would pull test_transaction.csv/test_identity.csv/sample_submission.csv too
        # (~640MB of unlabeled data we have no use for).
        for fname in ("train_transaction.csv", "train_identity.csv"):
            kaggle.api.competition_download_file(COMPETITION, fname, path=str(raw_dir), quiet=False)
            zip_path = raw_dir / f"{fname}.zip"
            if zip_path.exists():
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(raw_dir)
                zip_path.unlink()
        required = raw_dir / "train_transaction.csv"
        return required.exists()
    except Exception as exc:  # noqa: BLE001 -- any failure here must not block the pipeline
        print(f"[ACQUISITION] Real Kaggle download failed ({exc!r}); falling back to sample.", file=sys.stderr)
        return False


def generate_bundled_sample(raw_dir: Path, n: int = N_BACKGROUND_TX, seed: int = SEED) -> None:
    """Schema-compatible subset of IEEE-CIS's train_transaction/train_identity
    columns. Deliberately omits the anonymized V1-V339/C/D/M feature blocks --
    they don't affect graph topology (our concern in this chunk) and will
    arrive with the real download once credentials are configured, ahead of
    GNN feature engineering in Chunk 6.
    """
    rng = np.random.default_rng(seed)

    card1 = rng.integers(1000, 1000 + N_UNIQUE_CARDS, size=n)
    addr1 = rng.choice(np.arange(100, 160), size=n)
    product_cd = rng.choice(["W", "C", "R", "H", "S"], size=n, p=[0.74, 0.12, 0.07, 0.04, 0.03])
    is_fraud = rng.choice([0, 1], size=n, p=[0.965, 0.035])
    tx_dt = np.sort(rng.integers(0, 30 * 24 * 3600, size=n))  # 30-day window, seconds

    tx_df = pd.DataFrame(
        {
            "TransactionID": np.arange(2_987_000, 2_987_000 + n),
            "isFraud": is_fraud,
            "TransactionDT": tx_dt,
            "TransactionAmt": np.round(rng.lognormal(mean=4.2, sigma=1.0, size=n), 2),
            "ProductCD": product_cd,
            "card1": card1,
            "card4": rng.choice(
                ["visa", "mastercard", "american express", "discover"], size=n, p=[0.6, 0.3, 0.07, 0.03]
            ),
            "card6": rng.choice(["debit", "credit"], size=n, p=[0.65, 0.35]),
            "addr1": addr1,
            "addr2": rng.choice([87, 60, 96], size=n, p=[0.9, 0.06, 0.04]),
            "dist1": np.where(rng.random(n) < 0.6, np.nan, rng.integers(0, 500, size=n)),
            "P_emaildomain": rng.choice(
                ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", np.nan],
                size=n,
                p=[0.35, 0.2, 0.15, 0.1, 0.2],
            ),
        }
    )

    has_identity = rng.random(n) < IDENTITY_JOIN_RATE
    identity_ids = tx_df.loc[has_identity, "TransactionID"].to_numpy()
    identity_df = pd.DataFrame(
        {
            "TransactionID": identity_ids,
            "DeviceType": rng.choice(["desktop", "mobile"], size=len(identity_ids), p=[0.55, 0.45]),
            "DeviceInfo": rng.choice(
                ["Windows", "iOS Device", "MacOS", "SM-G950F Build/", "Trident/7.0", "SAMSUNG SM-G531H"],
                size=len(identity_ids),
            ),
        }
    )

    raw_dir.mkdir(parents=True, exist_ok=True)
    tx_df.to_csv(raw_dir / "train_transaction.csv", index=False)
    identity_df.to_csv(raw_dir / "train_identity.csv", index=False)


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    data_source = "bundled_sample"

    if kaggle_credentials_present():
        print("[ACQUISITION] Kaggle credentials detected -- attempting real download.")
        if download_real_dataset(RAW_DIR):
            data_source = "real_kaggle"
        else:
            print("[ACQUISITION] Falling back to bundled sample after failed real download.", file=sys.stderr)
    else:
        print_setup_instructions()

    if data_source == "bundled_sample":
        generate_bundled_sample(RAW_DIR)

    tx_path = RAW_DIR / "train_transaction.csv"
    row_count = sum(1 for _ in open(tx_path, encoding="utf-8")) - 1 if tx_path.exists() else 0

    metadata = {
        "data_source": data_source,
        "row_count": row_count,
        "seed": SEED,
        "acquired_at_unix": int(time.time()),
        "competition": COMPETITION,
    }
    with open(RAW_DIR / "acquisition_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"[ACQUISITION] data_source={data_source} row_count={row_count} -> {RAW_DIR}")


if __name__ == "__main__":
    main()
