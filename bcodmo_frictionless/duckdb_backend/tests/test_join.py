"""
Differential tests for join (bare/standard run-name; STANDARD dataflows join).

Both lanes run the SAME spec: the dataflows lane through the ``dataflows.join``
primitive (per ``standard_flows.join``), the DuckDB lane by driving the LIVE
``join_aux`` verbatim over a package shim. Every mode / aggregator / source.delete
must produce the SAME resource set, same fields+types, same rows in order, and
byte-identical CSV.

Source resources are declared BEFORE their target (the dataflows join primitive
requires the source to precede the target in the datapackage).
"""

import pytest

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
    run_duckdb_multi,
)


def test_half_outer_first_and_count():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}, {"name": "label", "type": "string"}],
    }
    resources = {
        "source": [{"id": "1", "val": "10"}, {"id": "1", "val": "20"}, {"id": "2", "val": "5"}],
        "target": [{"id": "1", "label": "a"}, {"id": "2", "label": "b"}, {"id": "3", "label": "c"}],
    }
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}"},
            "target": {"name": "target", "key": "{id}"},
            "fields": {
                "first_val": {"name": "val", "aggregate": "first"},
                "cnt": {"name": "val", "aggregate": "count"},
            },
            "mode": "half-outer",
        },
    }]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert set(dk) == {"source", "target"}, dk
    trows, tschema = dk["target"]
    # join fields not matching a source-schema name are appended alphabetically
    assert [f["name"] for f in tschema] == ["id", "label", "cnt", "first_val"]
    assert {f["name"]: f["type"] for f in tschema}["cnt"] == "integer"
    assert len(trows) == 3  # all target rows kept
    assert trows[2]["first_val"] is None and trows[2]["cnt"] is None  # id=3 unmatched
    assert_csv_identical_multi(resources, schemas, steps)


def test_inner_drops_unmatched_target():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}, {"name": "label", "type": "string"}],
    }
    resources = {
        "source": [{"id": "1", "val": "10"}, {"id": "2", "val": "5"}],
        "target": [{"id": "1", "label": "a"}, {"id": "3", "label": "c"}],
    }
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}"},
            "target": {"name": "target", "key": "{id}"},
            "fields": {"val": {"name": "val", "aggregate": "any"}},
            "mode": "inner",
        },
    }]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    trows, _ = dk["target"]
    assert [r["id"] for r in trows] == ["1"]  # id=3 dropped (no source match)
    assert_csv_identical_multi(resources, schemas, steps)


def test_full_outer_appends_unused_source_keys():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}, {"name": "label", "type": "string"}],
    }
    resources = {
        # source key "4" has no target row -> full-outer appends a row for it
        "source": [{"id": "1", "val": "10"}, {"id": "4", "val": "99"}],
        "target": [{"id": "1", "label": "a"}, {"id": "2", "label": "b"}],
    }
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}"},
            "target": {"name": "target", "key": "{id}"},
            "fields": {"val": {"name": "val", "aggregate": "any"}},
            "mode": "full-outer",
        },
    }]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    trows, _ = dk["target"]
    # 2 target rows + 1 appended for unused source key "4"
    assert len(trows) == 3
    assert trows[-1]["id"] == "4" and trows[-1]["val"] == "99"
    assert_csv_identical_multi(resources, schemas, steps)


def test_source_delete_removes_source():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}, {"name": "label", "type": "string"}],
    }
    resources = {
        "source": [{"id": "1", "val": "10"}],
        "target": [{"id": "1", "label": "a"}],
    }
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}", "delete": True},
            "target": {"name": "target", "key": "{id}"},
            "fields": {"val": {"name": "val", "aggregate": "any"}},
            "mode": "half-outer",
        },
    }]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    assert set(dk) == {"target"}, dk  # source dropped
    assert_csv_identical_multi(resources, schemas, steps)


def test_numeric_avg_and_sum_typed():
    """The cast-in path that distinguishes join from concatenate: set_types casts
    the source column to number, so the aggregators do Decimal arithmetic in BOTH
    lanes. Byte-identical requires the typed cast-in + format-out round-trip."""
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}, {"name": "label", "type": "string"}],
    }
    resources = {
        "source": [{"id": "1", "val": "10"}, {"id": "1", "val": "20"}, {"id": "2", "val": "5"}],
        "target": [{"id": "1", "label": "a"}, {"id": "2", "label": "b"}],
    }
    steps = [
        {
            "run": "bcodmo_pipeline_processors.set_types",
            "parameters": {"regex": False, "resources": "source",
                           "types": {"val": {"type": "number"}}},
        },
        {
            "run": "join",
            "parameters": {
                "source": {"name": "source", "key": "{id}"},
                "target": {"name": "target", "key": "{id}"},
                "fields": {
                    "avg_val": {"name": "val", "aggregate": "avg"},
                    "sum_val": {"name": "val", "aggregate": "sum"},
                },
                "mode": "half-outer",
            },
        },
    ]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    trows, tschema = dk["target"]
    types = {f["name"]: f["type"] for f in tschema}
    assert types["avg_val"] == "number" and types["sum_val"] == "number"
    assert str(trows[0]["avg_val"]) == "15" and str(trows[0]["sum_val"]) == "30"
    assert_csv_identical_multi(resources, schemas, steps)


def test_star_field_expansion():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "a", "type": "string"},
                   {"name": "b", "type": "string"}],
        "target": [{"name": "id", "type": "string"}],
    }
    resources = {
        "source": [{"id": "1", "a": "x", "b": "y"}],
        "target": [{"id": "1"}],
    }
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}"},
            "target": {"name": "target", "key": "{id}"},
            "fields": {"*": {"aggregate": "any"}},
            "mode": "half-outer",
        },
    }]
    df, dk = assert_equivalent_multi(resources, schemas, steps)
    _, tschema = dk["target"]
    # '*' expands to every source field not already a target field
    assert set(f["name"] for f in tschema) == {"id", "a", "b"}
    assert_csv_identical_multi(resources, schemas, steps)


def test_array_aggregator_raises():
    schemas = {
        "source": [{"name": "id", "type": "string"}, {"name": "val", "type": "string"}],
        "target": [{"name": "id", "type": "string"}],
    }
    resources = {"source": [{"id": "1", "val": "x"}], "target": [{"id": "1"}]}
    steps = [{
        "run": "join",
        "parameters": {
            "source": {"name": "source", "key": "{id}"},
            "target": {"name": "target", "key": "{id}"},
            "fields": {"vals": {"name": "val", "aggregate": "set"}},
            "mode": "half-outer",
        },
    }]
    with pytest.raises(NotImplementedError):
        run_duckdb_multi(resources, schemas, steps)


if __name__ == "__main__":
    test_half_outer_first_and_count()
    test_inner_drops_unmatched_target()
    test_full_outer_appends_unused_source_keys()
    test_source_delete_removes_source()
    test_numeric_avg_and_sum_typed()
    test_star_field_expansion()
    test_array_aggregator_raises()
    print("join: all modes/aggregators  dataflows == duckdb  ✓")
