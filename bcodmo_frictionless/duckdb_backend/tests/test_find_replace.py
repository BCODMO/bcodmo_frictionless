"""
Differential test (layer C) for find_replace.

find_replace edits string values in place via ``re.sub`` and leaves the schema
unchanged (every field stays a string). Both lanes run the SAME live
``_find_replace`` code (UDF-only, no ``to_sql``), so the gate is that the DuckDB
lane's VARCHAR round-trip and CSV serialization stay byte-identical to dataflows.

Covers: literal string replace, regex replace (character classes), the
uppercase/lowercase group callback, an empty/missing-value row (skipped by
default), ``replace_missing_values`` (empty value IS replaced), multiple fields
sharing patterns, and a ``boolean_statement`` gate.
"""

from bcodmo_frictionless.duckdb_backend.processors import find_replace  # noqa: F401  (register)
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
    {"id": "1", "x": "foo123bar", "y": "Hello World"},
    {"id": "2", "x": "barfoo", "y": "alpha"},
    {"id": "3", "x": "", "y": "BETA"},
]


def _run(steps):
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_literal_replace():
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "x", "patterns": [
                {"find": "foo", "replace": "FOO", "replace_function": "string"},
            ]},
        ]}},
    ]
    _run(steps)


def test_regex_replace():
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "x", "patterns": [
                {"find": "[0-9]+", "replace": "#", "replace_function": "string"},
            ]},
        ]}},
    ]
    _run(steps)


def test_uppercase_function():
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "y", "patterns": [
                {"find": "([a-z]+)", "replace_function": "uppercase"},
            ]},
        ]}},
    ]
    _run(steps)


def test_lowercase_function():
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "y", "patterns": [
                {"find": "([A-Z]+)", "replace_function": "lowercase"},
            ]},
        ]}},
    ]
    _run(steps)


def test_empty_value_skipped_by_default():
    # Row 3's x is "" (a missing value) -> left untouched; other rows change.
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "x", "patterns": [
                {"find": "^$", "replace": "EMPTY", "replace_function": "string"},
                {"find": "bar", "replace": "BAR", "replace_function": "string"},
            ]},
        ]}},
    ]
    _run(steps)


def test_replace_missing_values():
    # With replace_missing_values, the "" value IS processed (^$ -> EMPTY).
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "x", "patterns": [
                {"find": "^$", "replace": "EMPTY", "replace_function": "string",
                 "replace_missing_values": True},
            ]},
        ]}},
    ]
    _run(steps)


def test_multiple_fields_same_patterns():
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {"fields": [
            {"name": "x", "patterns": [
                {"find": "[a-z]+", "replace": "_", "replace_function": "string"},
            ]},
            {"name": "y", "patterns": [
                {"find": "[a-z]+", "replace": "_", "replace_function": "string"},
            ]},
        ]}},
    ]
    _run(steps)


def test_boolean_statement_gate():
    # Only rows where id == '1' get their patterns applied.
    steps = [
        {"run": "bcodmo_pipeline_processors.find_replace", "parameters": {
            "boolean_statement": "{id} == '1'",
            "fields": [
                {"name": "x", "patterns": [
                    {"find": "foo", "replace": "FOO", "replace_function": "string"},
                ]},
            ]}},
    ]
    _run(steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ok")
