"""Unit tests for graph/loader.py::_token_needs_refresh (Issue #12).

The full-corpus loader must proactively rebuild its Gremlin client BEFORE the
Entra token actually expires. The refresh decision was changed from a fixed
wall-clock age to "within a margin of the token's real `expires_on`" — this
pins that predicate.
"""
import loader


def test_no_refresh_when_token_fresh():
    # expires in 1 hour, default 5-min margin -> no refresh
    assert loader._token_needs_refresh(expires_on=10_000, now=10_000 - 3600) is False


def test_refresh_inside_margin():
    # 4 minutes to expiry, 5-min margin -> refresh
    assert loader._token_needs_refresh(expires_on=10_000, now=10_000 - 240) is True


def test_refresh_exactly_at_margin_boundary():
    assert loader._token_needs_refresh(expires_on=10_000, now=10_000 - 300) is True


def test_refresh_after_expiry():
    assert loader._token_needs_refresh(expires_on=10_000, now=10_050) is True


def test_custom_margin_respected():
    # 10-min margin: at 9 min to expiry it should refresh
    assert loader._token_needs_refresh(10_000, 10_000 - 540, margin_sec=600) is True
    assert loader._token_needs_refresh(10_000, 10_000 - 660, margin_sec=600) is False
