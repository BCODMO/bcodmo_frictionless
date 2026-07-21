"""
Differential tests for the structural resource ops remove_resources and
rename_resource, using the multi-resource harness.

Both are pure datapackage-shape ops: no rows change, only which resources exist
and what they are named. Both lanes must end with the same set of resources, each
byte-identical.
"""

import pytest

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
    run_duckdb_multi,
)

SCHEMAS = {
    "left": [{"name": "id", "type": "string"}, {"name": "x", "type": "string"}],
    "right": [{"name": "id", "type": "string"}, {"name": "y", "type": "string"}],
}
RESOURCES = {
    "left": [{"id": "1", "x": "10"}, {"id": "2", "x": "20"}],
    "right": [{"id": "1", "y": "a"}, {"id": "2", "y": "b"}],
}


def test_remove_one_resource():
    steps = [
        {"run": "bcodmo_pipeline_processors.remove_resources",
         "parameters": {"resources": "right"}},
    ]
    df, dk = assert_equivalent_multi(RESOURCES, SCHEMAS, steps)
    assert set(dk) == {"left"}, dk
    assert_csv_identical_multi(RESOURCES, SCHEMAS, steps)


def test_remove_nonexistent_raises():
    steps = [
        {"run": "bcodmo_pipeline_processors.remove_resources",
         "parameters": {"resources": "nope"}},
    ]
    with pytest.raises(Exception):
        run_duckdb_multi(RESOURCES, SCHEMAS, steps)


def test_rename_resource():
    steps = [
        {"run": "bcodmo_pipeline_processors.rename_resource",
         "parameters": {"old_resource": "left", "new_resource": "renamed"}},
    ]
    df, dk = assert_equivalent_multi(RESOURCES, SCHEMAS, steps)
    assert set(dk) == {"renamed", "right"}, dk
    assert_csv_identical_multi(RESOURCES, SCHEMAS, steps)


def test_rename_missing_raises():
    steps = [
        {"run": "bcodmo_pipeline_processors.rename_resource",
         "parameters": {"old_resource": "ghost", "new_resource": "x"}},
    ]
    with pytest.raises(Exception):
        run_duckdb_multi(RESOURCES, SCHEMAS, steps)


def test_remove_then_rename():
    steps = [
        {"run": "bcodmo_pipeline_processors.remove_resources",
         "parameters": {"resources": "right"}},
        {"run": "bcodmo_pipeline_processors.rename_resource",
         "parameters": {"old_resource": "left", "new_resource": "final"}},
    ]
    df, dk = assert_equivalent_multi(RESOURCES, SCHEMAS, steps)
    assert set(dk) == {"final"}, dk
    assert_csv_identical_multi(RESOURCES, SCHEMAS, steps)


if __name__ == "__main__":
    test_remove_one_resource()
    test_rename_resource()
    test_remove_then_rename()
    print("resource ops: remove/rename  dataflows == duckdb (multi-resource)  ✓")
