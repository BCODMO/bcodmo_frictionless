"""
Differential test for edit_cells.

edit_cells overwrites individual cells by (1-based row number, field). Schema is
unchanged; only the named cells change. Both lanes must agree on typed rows +
byte-identical CSV.

Note the live ``process_resource`` DRAINS its ``edited`` dict via ``.pop()``; the
differential harness now deep-copies step params per lane, and the DuckDB processor
deep-copies again internally, so each lane runs from a pristine ``edited``.
"""

from bcodmo_frictionless.duckdb_backend.processors import edit_cells  # noqa: F401
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "x", "type": "string"},
    {"name": "y", "type": "string"},
]
DATA = [
    {"id": "1", "x": "10", "y": "a"},
    {"id": "2", "x": "", "y": "b"},
    {"id": "3", "x": "30", "y": "c"},
]


def test_edit_cells_string_and_int_keys():
    # Row 1: overwrite x; row 3: overwrite y. Uses a str key and an int key to
    # exercise both live lookup branches.
    steps = [
        {"run": "bcodmo_pipeline_processors.edit_cells", "parameters": {"edited": {
            "1": [{"field": "x", "value": "EDITED"}],
            3: [{"field": "y", "value": "Z"}],
        }}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_edit_cells_overwrites_missing_cell():
    # Row 2's x is empty/missing -> overwriting it is like any other cell.
    steps = [
        {"run": "bcodmo_pipeline_processors.edit_cells", "parameters": {"edited": {
            "2": [{"field": "x", "value": "FILLED"}],
        }}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_edit_cells_string_and_int_keys()
    test_edit_cells_overwrites_missing_cell()
    print("edit_cells: cell overwrites  dataflows == duckdb  ✓")
