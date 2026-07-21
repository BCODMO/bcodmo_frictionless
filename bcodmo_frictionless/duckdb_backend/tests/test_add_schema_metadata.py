"""
Differential test for add_schema_metadata.

add_schema_metadata merges resource/SCHEMA-level metadata; it does not touch the
field list, field types, row values, or CSV bytes. So both lanes must produce
identical fields + identical rows + byte-identical CSV. (The schema-level metadata
itself is not represented in the Phase-0 field-list schema -- a documented gap for
datapackage.json emission; see the processor docstring.)
"""

from bcodmo_frictionless.duckdb_backend.processors import add_schema_metadata  # noqa: F401
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
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


def test_metadata_is_data_noop():
    steps = [
        {"run": "bcodmo_pipeline_processors.add_schema_metadata",
         "parameters": {"description": "A test dataset", "keywords": ["a", "b"]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_metadata_then_typed_step():
    # Interleaved with a real typing step: still identical fields/rows/bytes.
    steps = [
        {"run": "bcodmo_pipeline_processors.add_schema_metadata",
         "parameters": {"title": "T"}},
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"regex": False, "types": {"x": {"type": "number"}}}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_metadata_is_data_noop()
    test_metadata_then_typed_step()
    print("add_schema_metadata: data no-op, parity holds  ✓")
