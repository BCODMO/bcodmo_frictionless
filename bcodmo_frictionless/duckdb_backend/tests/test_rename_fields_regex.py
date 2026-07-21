"""Differential tests for bcodmo_pipeline_processors.rename_fields_regex.

Both lanes must agree on typed rows + final schema (assert_equivalent) AND on
byte-identical CSV egress (assert_csv_identical). Covers a regex that matches
multiple fields, a capture-group substitution, and a pattern that matches none
(no-op rename). An empty-string cell exercises missing-value handling.
"""

from bcodmo_frictionless.duckdb_backend.processors import (  # noqa: F401  (register)
    rename_fields_regex,
)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "temp_min", "type": "string"},
    {"name": "temp_max", "type": "string"},
]
DATA = [
    {"id": "1", "temp_min": "10.5", "temp_max": "20.5"},
    {"id": "2", "temp_min": "", "temp_max": "-3"},
    {"id": "3", "temp_min": "8", "temp_max": "9"},
]


def test_matches_multiple_fields():
    steps = [
        {
            "run": "bcodmo_pipeline_processors.rename_fields_regex",
            "parameters": {
                "fields": ["temp_min", "temp_max"],
                "pattern": {"find": "temp", "replace": "temperature"},
            },
        },
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_capture_group_substitution():
    steps = [
        {
            "run": "bcodmo_pipeline_processors.rename_fields_regex",
            "parameters": {
                "fields": ["temp_min", "temp_max"],
                "pattern": {"find": r"temp_(.*)", "replace": r"\1_temp"},
            },
        },
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_matches_none_is_noop():
    steps = [
        {
            "run": "bcodmo_pipeline_processors.rename_fields_regex",
            "parameters": {
                "fields": ["id"],
                "pattern": {"find": "zzz", "replace": "qqq"},
            },
        },
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)
