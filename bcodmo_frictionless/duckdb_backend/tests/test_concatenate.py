"""
Differential tests for concatenate (multi-resource -> one target resource).

Both lanes must end with the SAME single target resource: same fields, same rows in
concatenation order, byte-identical CSV. Row logic is the live ``concatenator``
reused verbatim; the target schema is re-derived and pinned here.
"""

import pytest

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
    run_duckdb_multi,
)


def test_concat_same_field_names_with_source_column():
    schemas = {
        "res_a": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "res_b": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
    }
    resources = {
        "res_a": [{"id": "1", "val": "x"}, {"id": "2", "val": "y"}],
        "res_b": [{"id": "3", "val": "z"}],
    }
    steps = [
        {"run": "bcodmo_pipeline_processors.concatenate", "parameters": {
            "sources": ["res_a", "res_b"],
            "target": {"name": "merged"},
            "fields": {"id": [], "val": []},
            "include_source_names": [{"type": "resource", "column_name": "src"}],
        }},
    ]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert set(dk) == {"merged"}, dk
    merged_rows, _ = dk["merged"]
    assert len(merged_rows) == 3
    assert [r["src"] for r in merged_rows] == ["res_a", "res_a", "res_b"]
    assert_csv_identical_multi(resources, schemas, steps)


def test_concat_field_remapping():
    # Different source field names collapse into one target field.
    schemas = {
        "left": [{"name": "a_id", "type": "string"}, {"name": "a_v", "type": "string"}],
        "right": [{"name": "b_id", "type": "string"}, {"name": "b_v", "type": "string"}],
    }
    resources = {
        "left": [{"a_id": "1", "a_v": "hello"}],
        "right": [{"b_id": "2", "b_v": "world"}],
    }
    steps = [
        {"run": "bcodmo_pipeline_processors.concatenate", "parameters": {
            "sources": ["left", "right"],
            "target": {"name": "concat"},
            "fields": {"key": ["a_id", "b_id"], "value": ["a_v", "b_v"]},
        }},
    ]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert set(dk) == {"concat"}, dk
    assert_csv_identical_multi(resources, schemas, steps)


def test_concat_default_target_name():
    schemas = {
        "r1": [{"name": "c", "type": "string"}],
        "r2": [{"name": "c", "type": "string"}],
    }
    resources = {"r1": [{"c": "1"}], "r2": [{"c": "2"}]}
    steps = [
        {"run": "bcodmo_pipeline_processors.concatenate", "parameters": {
            "sources": ["r1", "r2"],
            "fields": {"c": []},
        }},
    ]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert set(dk) == {"concat"}, dk  # default target name
    assert_csv_identical_multi(resources, schemas, steps)


def test_concat_no_matching_source_raises():
    schemas = {"r1": [{"name": "c", "type": "string"}]}
    resources = {"r1": [{"c": "1"}]}
    steps = [
        {"run": "bcodmo_pipeline_processors.concatenate", "parameters": {
            "sources": ["ghost"], "fields": {"c": []},
        }},
    ]
    with pytest.raises(Exception):
        run_duckdb_multi(resources, schemas, steps)


if __name__ == "__main__":
    test_concat_same_field_names_with_source_column()
    test_concat_field_remapping()
    test_concat_default_target_name()
    print("concatenate: multi-resource merge  dataflows == duckdb  ✓")
