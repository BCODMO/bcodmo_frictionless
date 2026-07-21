"""Differential tests for duplicate (bare/standard run-name; multi-resource copy)."""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
)


def test_duplicate_after_source():
    schemas = {"orig": [{"name": "id", "type": "string"}, {"name": "v", "type": "string"}]}
    resources = {"orig": [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]}
    steps = [{"run": "duplicate", "parameters": {"source": "orig", "target-name": "copy"}}]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert list(dk) == ["orig", "copy"]  # copy inserted right after source
    crows, _ = dk["copy"]
    assert [r["id"] for r in crows] == ["1", "2"]  # order preserved
    assert_csv_identical_multi(resources, schemas, steps)


def test_default_target_name_and_to_end():
    schemas = {
        "a": [{"name": "x", "type": "string"}],
        "b": [{"name": "x", "type": "string"}],
    }
    resources = {"a": [{"x": "1"}], "b": [{"x": "2"}]}
    steps = [{"run": "duplicate",
              "parameters": {"source": "a", "duplicate_to_end": True}}]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    # default target name <source>_copy, appended at the end
    assert list(dk) == ["a", "b", "a_copy"]
    assert_csv_identical_multi(resources, schemas, steps)


if __name__ == "__main__":
    test_duplicate_after_source()
    test_default_target_name_and_to_end()
    print("duplicate: dataflows == duckdb  ✓")
