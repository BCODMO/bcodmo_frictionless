"""
Differential test (layer C) for round_fields.

round_fields is a NUMERIC processor: every test prepends a ``set_types``->number
step so the field is a real ``Decimal`` before rounding (as real pipelines do and
as the live processor requires -- it raises on a non-number field). The DuckDB
lane has no ``to_sql`` for round_fields (Decimal rounding is not byte-exact in
SQL), so ``assert_csv_identical`` pins the UDF path's bytes to the dataflows lane.

Covers the real param shapes (digits, preserve_trailing_zeros, maximum_precision,
convert_to_integer) plus edge cases: negative numbers, an empty-string (missing)
value row, and ``boolean_statement`` gating.
"""

from bcodmo_frictionless.duckdb_backend.processors import round_fields  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "x", "type": "string"},
]
# Includes negatives, a value at the rounding boundary, an integer-valued number,
# and an empty-string (missing) row to exercise passthrough.
DATA = [
    {"id": "1", "x": "10.567"},
    {"id": "2", "x": "-3.284"},
    {"id": "3", "x": "0.005"},
    {"id": "4", "x": "42"},
    {"id": "5", "x": "-0.001"},
    {"id": "6", "x": ""},
    {"id": "7", "x": "100.50"},
]


def _set_types_number():
    return {
        "run": "bcodmo_pipeline_processors.set_types",
        "parameters": {"regex": False, "types": {"x": {"type": "number"}}},
    }


def test_round_digits_2():
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [{"name": "x", "digits": 2}]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_round_digits_0():
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [{"name": "x", "digits": 0}]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_round_preserve_trailing_zeros():
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [
             {"name": "x", "digits": 2, "preserve_trailing_zeros": True},
         ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_round_maximum_precision():
    # Rows whose current precision is < digits pass through untouched.
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [
             {"name": "x", "digits": 2, "maximum_precision": True},
         ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_round_convert_to_integer():
    # digits==0 + convert_to_integer: retypes x number->integer AND casts to int.
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [
             {"name": "x", "digits": 0, "convert_to_integer": True},
         ]}},
    ]
    df_rows, dk_rows = assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_round_boolean_statement_gating():
    # Only rows where x > 0 are rounded; others pass through unchanged.
    steps = [
        _set_types_number(),
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {
             "fields": [{"name": "x", "digits": 1}],
             "boolean_statement": "{x} > 0",
         }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ✓")
