"""Unit tests for graph/loader.py::clean_props — the function that sanitises
a parquet row before it becomes Gremlin property bindings.

Cosmos Gremlin rejects null properties and can't serialise numpy scalar
types, so clean_props must drop None/NaN and downcast numpy scalars to native
Python types. This is pure and network-free, so it's a good unit target.
"""
import numpy as np

import loader


def test_drops_none_values():
    assert "b" not in loader.clean_props({"a": 1, "b": None})


def test_drops_python_nan():
    out = loader.clean_props({"a": 1.0, "b": float("nan")})
    assert "b" not in out
    assert out["a"] == 1.0


def test_drops_numpy_nan():
    out = loader.clean_props({"a": np.float64("nan")})
    assert out == {}


def test_converts_numpy_bool_to_python_bool():
    out = loader.clean_props({"flag": np.bool_(True)})
    assert out["flag"] is True
    assert type(out["flag"]) is bool


def test_converts_numpy_integer_to_python_int():
    out = loader.clean_props({"n": np.int64(7)})
    assert out["n"] == 7
    assert type(out["n"]) is int


def test_converts_numpy_floating_to_python_float():
    out = loader.clean_props({"x": np.float32(1.5)})
    assert out["x"] == 1.5
    assert type(out["x"]) is float


def test_passes_strings_through_unchanged():
    out = loader.clean_props({"acct_id": "ACC-000001"})
    assert out == {"acct_id": "ACC-000001"}


def test_mixed_row():
    row = {"acct_id": "ACC-1", "score": np.float64(0.9), "n": np.int64(3),
           "ring": np.bool_(False), "missing": None, "bad": float("nan")}
    out = loader.clean_props(row)
    assert out == {"acct_id": "ACC-1", "score": 0.9, "n": 3, "ring": False}
    assert type(out["n"]) is int and type(out["score"]) is float
