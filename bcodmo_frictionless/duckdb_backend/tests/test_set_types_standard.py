"""Differential tests for standard (bare-name) set_types.

The reference is the dataflows ``set_type`` primitive (the production
``standard_flows.set_types`` wrapper is broken -- undefined ``_set_type``)."""

import pytest

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
    run_duckdb,
)

STRING_SCHEMA = [
    {"name": "a", "type": "string"},
    {"name": "b", "type": "string"},
]


def test_set_number_type():
    data = [{"a": "1.5", "b": "x"}, {"a": "2.0", "b": "y"}]
    steps = [{
        "run": "set_types",
        "parameters": {"regex": False, "types": {"a": {"type": "number"}}},
    }]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    _, dk_schema = run_duckdb(data, STRING_SCHEMA, steps)
    assert {f["name"]: f["type"] for f in dk_schema}["a"] == "number"
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_delete_via_none_option():
    data = [{"a": "1", "b": "x"}]
    steps = [{
        "run": "set_types",
        "parameters": {"regex": False, "types": {"b": None}},
    }]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    _, dk_schema = run_duckdb(data, STRING_SCHEMA, steps)
    assert [f["name"] for f in dk_schema] == ["a"]  # b deleted
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_missing_field_raises():
    data = [{"a": "1", "b": "x"}]
    steps = [{
        "run": "set_types",
        "parameters": {"regex": False, "types": {"ghost": {"type": "number"}}},
    }]
    with pytest.raises(Exception):
        run_duckdb(data, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_set_number_type()
    test_delete_via_none_option()
    test_missing_field_raises()
    print("set_types (standard): dataflows == duckdb  ✓")
