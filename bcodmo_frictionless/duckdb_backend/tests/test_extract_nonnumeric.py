"""
Differential test (layer C) for extract_nonnumeric.

extract_nonnumeric ADDS a new string field ``f"{name}{suffix}"`` after each source
field: numeric (or missing) source values leave the source alone and blank the new
field; NON-numeric source values are MOVED into the new field and the source is
nulled. There is no ``to_sql`` (Python ``float()`` parsing + the move/blank rewrite
is not byte-exact in SQL), so the DuckDB lane runs the live ``process_resource`` as
a UDF. ``assert_csv_identical`` proves both lanes emit byte-identical CSV.

Covers: values that ARE numeric (kept), values with non-numeric junk (extracted),
an empty/missing value row, an explicit non-default suffix, multiple fields, and a
``boolean_statement`` gate.
"""

from bcodmo_frictionless.duckdb_backend.processors import extract_nonnumeric  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "x", "type": "string"},
]
DATA = [
    {"id": "1", "x": "10.567"},   # numeric -> kept, x_ = ""
    {"id": "2", "x": "-3.2"},     # numeric (negative) -> kept, x_ = ""
    {"id": "3", "x": "abc"},      # non-numeric -> moved to x_, x -> None
    {"id": "4", "x": "12 m/s"},   # non-numeric junk -> moved to x_, x -> None
    {"id": "5", "x": ""},         # empty/missing -> treated numeric, x_ = ""
]


def test_extract_default_suffix():
    steps = [
        {"run": "bcodmo_pipeline_processors.extract_nonnumeric",
         "parameters": {"fields": ["x"]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_extract_explicit_suffix():
    steps = [
        {"run": "bcodmo_pipeline_processors.extract_nonnumeric",
         "parameters": {"fields": ["x"], "suffix": "_flag"}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_extract_multiple_fields():
    schema = [
        {"name": "id", "type": "string"},
        {"name": "a", "type": "string"},
        {"name": "b", "type": "string"},
    ]
    data = [
        {"id": "1", "a": "1.0", "b": "ok"},
        {"id": "2", "a": "junk", "b": "2"},
        {"id": "3", "a": "", "b": ""},
    ]
    steps = [
        {"run": "bcodmo_pipeline_processors.extract_nonnumeric",
         "parameters": {"fields": ["a", "b"]}},
    ]
    assert_equivalent(data, schema, steps)
    assert_csv_identical(data, schema, steps)


def test_extract_boolean_statement_gate():
    # Only rows where id == '3' or '4' are examined; others get x_ = "" and keep x.
    steps = [
        {"run": "bcodmo_pipeline_processors.extract_nonnumeric",
         "parameters": {"fields": ["x"], "boolean_statement": "{id} == '3'"}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ✓")
