"""Differential tests for sort (bare/standard run-name)."""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
    run_duckdb,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "label", "type": "string"},
]


def test_lexical_sort():
    data = [{"id": "3", "label": "c"}, {"id": "1", "label": "a"}, {"id": "2", "label": "b"}]
    steps = [{"run": "sort", "parameters": {"sort-by": "{id}"}}]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert [r["id"] for r in dk_rows] == ["1", "2", "3"]
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_reverse():
    data = [{"id": "1", "label": "a"}, {"id": "2", "label": "b"}, {"id": "3", "label": "c"}]
    steps = [{"run": "sort", "parameters": {"sort-by": "{id}", "reverse": True}}]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert [r["id"] for r in dk_rows] == ["3", "2", "1"]
    assert_csv_identical(data, STRING_SCHEMA, steps)


def test_numeric_sort_after_set_types():
    # As strings, "10" < "9"; as numbers, 9 < 10. Sorting AFTER set_types->number
    # must use the numeric order -- exercises KeyCalc's numeric bit-encoding on the
    # cast-in typed values.
    data = [{"id": "10", "label": "a"}, {"id": "9", "label": "b"}, {"id": "100", "label": "c"}]
    steps = [
        {"run": "set_types",
         "parameters": {"regex": False, "types": {"id": {"type": "number"}}}},
        {"run": "sort", "parameters": {"sort-by": "{id}"}},
    ]
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    assert [str(r["id"]) for r in dk_rows] == ["9", "10", "100"]
    assert_csv_identical(data, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_lexical_sort()
    test_reverse()
    test_numeric_sort_after_set_types()
    print("sort: dataflows == duckdb  ✓")
