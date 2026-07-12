"""Unit tests for the deterministic per-account device/IP fallbacks in
data/scripts/export_ingestion_jsonl.py.

Circular/smurfing ring accounts have no USED_DEVICE/ACCESSED_FROM edges of
their own, so the ingestion export synthesises a stable device/IP per account
(Chunk-2 decision). "Stable" is the property that matters: the same account
must always map to the same device/IP, so these are unit-tested for
determinism and well-formedness.
"""
import export_ingestion_jsonl as ex


def test_fallback_device_is_deterministic():
    assert ex._fallback_device("ACC-000001") == ex._fallback_device("ACC-000001")


def test_fallback_device_differs_by_account():
    assert ex._fallback_device("ACC-000001") != ex._fallback_device("ACC-000002")


def test_fallback_device_has_expected_prefix():
    assert ex._fallback_device("ACC-000001").startswith("DEV-FALLBACK-")


def test_fallback_ip_is_deterministic():
    assert ex._fallback_ip("ACC-000001") == ex._fallback_ip("ACC-000001")


def test_fallback_ip_is_valid_dotted_quad():
    ip = ex._fallback_ip("ACC-000123")
    octets = ip.split(".")
    assert len(octets) == 4
    assert octets[0] == "10" and octets[3] == "1"
    assert all(0 <= int(o) <= 255 for o in octets)


def test_fallback_ip_differs_across_many_accounts():
    # Not a strict guarantee (hash space is 16 bits), but a handful of
    # distinct accounts should not all collide.
    ips = {ex._fallback_ip(f"ACC-{i:06d}") for i in range(50)}
    assert len(ips) > 1
