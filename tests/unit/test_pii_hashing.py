"""Unit tests for the PII hashing helpers in data/scripts/graph_schema.py.

These lock in the Chunk-2 security correction (SHA-256, not the POC's MD5)
at the schema-derivation layer: `_sha256` is what produces the `tax_hash`
pseudonyms that go into the graph.
"""
import hashlib

import graph_schema as gs


def test_sha256_matches_hashlib():
    assert gs._sha256("account-123") == hashlib.sha256(b"account-123").hexdigest()


def test_sha256_is_64_hex_chars():
    h = gs._sha256("anything")
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_sha256_is_deterministic():
    assert gs._sha256("same-input") == gs._sha256("same-input")


def test_sha256_differs_by_input():
    assert gs._sha256("a") != gs._sha256("b")


def test_sha256_is_not_md5():
    # The whole point of the Chunk-2 correction: this must not be MD5.
    text = "customer-42"
    assert gs._sha256(text) != gs._md5(text)
    assert len(gs._sha256(text)) == 64
    assert len(gs._md5(text)) == 32
