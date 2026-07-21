"""
Differential test (layer C) for string_format -- the processor that builds a new
string column by applying a python ``str.format`` template to one or more input
columns.

Covers: a plain multi-field concat template, a numeric format spec applied to a
``set_types``-typed (Decimal) input, an empty/missing input value (which nullifies
the whole output per the live "any missing input -> None" rule), overwriting an
existing field, and ``boolean_statement`` gating (non-passing rows get None for a
new output field). ``assert_csv_identical`` proves both lanes emit byte-identical
CSV, which only holds if the DuckDB UDF path reproduces the live ``.format``
output exactly.
"""

from bcodmo_frictionless.duckdb_backend.processors import string_format  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "site", "type": "string"},
    {"name": "lat", "type": "string"},
    {"name": "lon", "type": "string"},
]
DATA = [
    {"id": "1", "site": "A", "lat": "10.5", "lon": "20.25"},
    {"id": "2", "site": "B", "lat": "-3.2", "lon": "7.0"},
    {"id": "3", "site": "", "lat": "", "lon": "0"},  # empty site + lat -> missing
]


def test_concat_two_string_fields():
    # A plain template over two string inputs. Row 3 has site="" (a missing
    # value) -> the whole output is set to None (not formatted).
    steps = [
        {"run": "bcodmo_pipeline_processors.string_format", "parameters": {"fields": [
            {"output_field": "label", "input_string": "{}_{}",
             "input_fields": ["site", "id"]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_numeric_format_spec_on_typed_input():
    # set_types makes lat a number (Decimal); the format spec renders it with a
    # fixed number of decimals. Row 3 lat="" -> None after cast -> output None.
    steps = [
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"regex": False, "types": {"lat": {"type": "number"}}}},
        {"run": "bcodmo_pipeline_processors.string_format", "parameters": {"fields": [
            {"output_field": "lat_str", "input_string": "lat={:.3f}",
             "input_fields": ["lat"]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_multiple_positional_fields():
    steps = [
        {"run": "bcodmo_pipeline_processors.string_format", "parameters": {"fields": [
            {"output_field": "coord", "input_string": "{},{} ({})",
             "input_fields": ["lat", "lon", "site"]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_overwrite_existing_field():
    # output_field == an existing field name -> replaced in place, order kept.
    steps = [
        {"run": "bcodmo_pipeline_processors.string_format", "parameters": {"fields": [
            {"output_field": "site", "input_string": "site-{}",
             "input_fields": ["id"]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_boolean_statement_gates_rows():
    # Only rows where id != 2 are formatted; the others get None (new field).
    steps = [
        {"run": "bcodmo_pipeline_processors.string_format", "parameters": {
            "boolean_statement": "{id} != '2'",
            "fields": [
                {"output_field": "gated", "input_string": "{}#{}",
                 "input_fields": ["site", "id"]},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ✓")
