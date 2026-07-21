"""
Differential test for update_fields (schema-only metadata merge).

update_fields merges props into named field descriptors; row values are unchanged.
The differential harness compares field names/types + rows + CSV bytes, so parity
holds trivially for descriptive (non-type) props. We ALSO assert directly that the
DuckDB lane actually applied the prop -- this is the regression guard for the
``update_fields.flow`` ``parameters.pop("fields")`` mutation hazard (the harness now
deep-copies params per lane, so the DuckDB lane still sees ``fields``).
"""

import pytest

from bcodmo_frictionless.duckdb_backend.processors import update_fields  # noqa: F401
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
    run_duckdb,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "x", "type": "string"},
]
DATA = [
    {"id": "1", "x": "10.5"},
    {"id": "2", "x": ""},
    {"id": "3", "x": "-3"},
]


def test_metadata_update_parity_and_applied():
    steps = [
        {"run": "bcodmo_pipeline_processors.update_fields", "parameters": {"fields": {
            "x": {"units": "meters", "description": "depth"},
        }}},
    ]
    # Parity: names/types/rows/bytes identical across lanes.
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)

    # Direct: the DuckDB lane actually merged the props into field "x"
    # (guards the pop("fields") mutation hazard).
    _rows, schema = run_duckdb(DATA, STRING_SCHEMA, steps)
    x = next(f for f in schema if f["name"] == "x")
    assert x.get("units") == "meters" and x.get("description") == "depth", x


def test_missing_field_raises():
    steps = [
        {"run": "bcodmo_pipeline_processors.update_fields",
         "parameters": {"fields": {"nope": {"units": "m"}}}},
    ]
    with pytest.raises(Exception):
        run_duckdb(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_metadata_update_parity_and_applied()
    test_missing_field_raises()
    print("update_fields: metadata merge parity + applied  ✓")
