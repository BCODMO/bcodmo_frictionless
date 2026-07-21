"""Differential tests for add_computed_field (bare/standard run-name)."""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "a", "type": "string"},
    {"name": "b", "type": "string"},
]


def test_numeric_sum():
    data = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    steps = [
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"regex": False, "types": {"a": {"type": "number"}, "b": {"type": "number"}}}},
        {"run": "add_computed_field",
         "parameters": {"fields": [
             {"operation": "sum", "source": ["a", "b"], "target": "total"}]}},
    ]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert str(dk_rows[0]["total"]) == "3" and str(dk_rows[1]["total"]) == "7"
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_format_string():
    data = [{"a": "x", "b": "y"}]
    steps = [{
        "run": "add_computed_field",
        "parameters": {"fields": [
            {"operation": "format", "target": "label", "with": "{a}-{b}"}]},
    }]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert dk_rows[0]["label"] == "x-y"
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_constant_and_target_dict():
    data = [{"a": "1", "b": "2"}]
    steps = [{
        "run": "add_computed_field",
        "parameters": {"fields": [
            {"operation": "constant", "target": {"name": "k", "type": "string"}, "with": "Z"}]},
    }]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert dk_rows[0]["k"] == "Z"
    assert_csv_identical(data, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_numeric_sum()
    test_format_string()
    test_constant_and_target_dict()
    print("add_computed_field: dataflows == duckdb  ✓")
