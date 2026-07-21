"""
Differential test (layer C) for split_column -- a UDF-only processor that splits
one string column into multiple new string columns (1:1 rows).

Covers: a normal delimiter split, a regex ``pattern`` (capture-group) split,
``delete_input=True`` (input column dropped) vs ``delete_input=False`` (input
kept), input-field name reused as an output field (delete_input must NOT drop it),
a ``boolean_statement`` gate, and an empty/missing input row (which becomes None
after cast-in and must pass through as None in BOTH lanes). Every case asserts both
typed-row equivalence and byte-identical CSV.
"""

from bcodmo_frictionless.duckdb_backend.processors import split_column  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "pair", "type": "string"},
]
DATA = [
    {"id": "1", "pair": "abc-123"},
    {"id": "2", "pair": "def-456"},
    {"id": "3", "pair": ""},        # empty -> None after cast-in (missing passthrough)
    {"id": "4", "pair": "ghi-789"},
]


def test_delimiter_split_keep_input():
    steps = [
        {"run": "bcodmo_pipeline_processors.split_column", "parameters": {
            "delete_input": False,
            "fields": [
                {"input_field": "pair", "output_fields": ["letters", "digits"],
                 "delimiter": "-"},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_delimiter_split_delete_input():
    steps = [
        {"run": "bcodmo_pipeline_processors.split_column", "parameters": {
            "delete_input": True,
            "fields": [
                {"input_field": "pair", "output_fields": ["letters", "digits"],
                 "delimiter": "-"},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_pattern_split():
    steps = [
        {"run": "bcodmo_pipeline_processors.split_column", "parameters": {
            "delete_input": False,
            "fields": [
                {"input_field": "pair", "output_fields": ["letters", "digits"],
                 "pattern": r"^([a-z]+)-([0-9]+)$"},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_delete_input_reused_as_output():
    # delete_input=True but the input field name is reused as an output field:
    # the live processor keeps it (replaces in place), the schema must too.
    steps = [
        {"run": "bcodmo_pipeline_processors.split_column", "parameters": {
            "delete_input": True,
            "fields": [
                {"input_field": "pair", "output_fields": ["pair", "digits"],
                 "delimiter": "-"},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_boolean_statement_gate():
    # Only rows whose id == '1' get split; the rest get None for every output.
    # (The boolean DSL requires string literals in single quotes.)
    steps = [
        {"run": "bcodmo_pipeline_processors.split_column", "parameters": {
            "delete_input": False,
            "boolean_statement": "{id} == '1'",
            "fields": [
                {"input_field": "pair", "output_fields": ["letters", "digits"],
                 "delimiter": "-"},
            ],
        }},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  OK")
