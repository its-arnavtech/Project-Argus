"""Unit tests for the ring-account row builder in
data/scripts/ring_injector.py::_new_account_customer_rows.

This is the single place that mints a synthetic ring account together with
its Customer and OWNS edge (the Chunk-7 fix that closed the missing-ring-OWNS
gap). The invariants below are exactly what that fix has to guarantee, so
they're worth pinning: 1:1 account↔customer↔owns pairing, the ring-membership
flags, and provenance tagging.
"""
import numpy as np

import ring_injector as ri


def _rows(ids=("ACC-R00000", "ACC-R00001", "ACC-R00002")):
    rng = np.random.default_rng(0)
    return ri._new_account_customer_rows(list(ids), "CIRC-000", "circular", rng)


def test_counts_match_input():
    accounts, customers, owns = _rows()
    assert len(accounts) == len(customers) == len(owns) == 3


def test_every_account_flagged_ring_member():
    accounts, _, _ = _rows()
    assert accounts["is_ring_member"].all()
    assert (accounts["ring_type"] == "circular").all()
    assert (accounts["ring_id"] == "CIRC-000").all()


def test_owns_pairs_customer_to_account_one_to_one():
    accounts, customers, owns = _rows()
    # every owns edge connects a minted customer to a minted account
    assert set(owns["dst_acct_id"]) == set(accounts["acct_id"])
    assert set(owns["src_cust_id"]) == set(customers["cust_id"])
    # exact 1:1 pairing per row
    for _, r in owns.iterrows():
        acct = accounts.loc[accounts["acct_id"] == r["dst_acct_id"]].iloc[0]
        assert acct["cust_id"] == r["src_cust_id"]


def test_provenance_tagged_synthetic_ring():
    accounts, customers, owns = _rows()
    assert (accounts["provenance"] == "synthetic_ring").all()
    assert (customers["provenance"] == "synthetic_ring").all()
    assert (owns["provenance"] == "synthetic_ring").all()


def test_kyc_status_is_valid_enum():
    _, customers, _ = _rows()
    assert set(customers["KYC_status"]).issubset({"VERIFIED", "PENDING"})


def test_tax_hash_is_sha256_shaped():
    _, customers, _ = _rows()
    for h in customers["tax_hash"]:
        assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)


def test_risk_base_in_unit_interval():
    accounts, _, _ = _rows()
    assert accounts["risk_base"].between(0.0, 1.0).all()
