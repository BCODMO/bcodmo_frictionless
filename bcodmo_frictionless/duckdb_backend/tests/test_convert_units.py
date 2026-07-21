"""Differential tests for the convert_units processor: the DuckDB lane (live
process_resource wrapped as a UDF) must agree with the dataflows lane, byte for
byte. A numeric processor, so every case first runs set_types -> number on the
input column (TEMPLATE.md rule 3)."""

import pytest

from bcodmo_frictionless.duckdb_backend.processors import convert_units  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
    run_dataflows,
    run_duckdb,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "depth", "type": "string"},
]

# includes a negative value and a fractional value
DATA = [
    {"id": "1", "depth": "10"},
    {"id": "2", "depth": "-3.5"},
    {"id": "3", "depth": "100.25"},
    {"id": "4", "depth": "0"},
]


def _set_types_step():
    return {
        "run": "bcodmo_pipeline_processors.set_types",
        "parameters": {"regex": False, "types": {"depth": {"type": "number"}}},
    }


def test_feet_to_meter_in_place():
    steps = [
        _set_types_step(),
        {
            "run": "bcodmo_pipeline_processors.convert_units",
            "parameters": {"fields": [{"name": "depth", "conversion": "feet_to_meter"}]},
        },
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_mile_to_km_in_place():
    steps = [
        _set_types_step(),
        {
            "run": "bcodmo_pipeline_processors.convert_units",
            "parameters": {"fields": [{"name": "depth", "conversion": "mile_to_km"}]},
        },
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_preserve_field_adds_new_column():
    # preserve_field=True writes the converted value to a NEW field, keeping the
    # original column untouched; the new field is appended right after it.
    steps = [
        _set_types_step(),
        {
            "run": "bcodmo_pipeline_processors.convert_units",
            "parameters": {
                "fields": [
                    {
                        "name": "depth",
                        "conversion": "fathom_to_meter",
                        "preserve_field": True,
                        "new_field_name": "depth_m",
                    }
                ]
            },
        },
    ]
    _, dk_rows = assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)
    # sanity: the new column exists and the original is preserved
    assert set(dk_rows[0].keys()) == {"id", "depth", "depth_m"}


def test_multiple_fields_and_inch_to_cm():
    schema = [
        {"name": "id", "type": "string"},
        {"name": "len_in", "type": "string"},
        {"name": "dist_mi", "type": "string"},
    ]
    data = [
        {"id": "1", "len_in": "12", "dist_mi": "2.5"},
        {"id": "2", "len_in": "-1.5", "dist_mi": "0"},
    ]
    steps = [
        {
            "run": "bcodmo_pipeline_processors.set_types",
            "parameters": {
                "regex": False,
                "types": {
                    "len_in": {"type": "number"},
                    "dist_mi": {"type": "number"},
                },
            },
        },
        {
            "run": "bcodmo_pipeline_processors.convert_units",
            "parameters": {
                "fields": [
                    {"name": "len_in", "conversion": "inch_to_cm"},
                    {"name": "dist_mi", "conversion": "mile_to_km"},
                ]
            },
        },
    ]
    assert_equivalent(data, schema, steps)
    assert_csv_identical(data, schema, steps)


def test_missing_value_raises_in_both_lanes():
    # LIVE-CODE QUIRK: the live process_resource's missing-value branch references
    # an unbound `new_field_name` (an empty cell cast to number becomes None), so
    # ANY missing value raises. Both lanes run that same live code, so both raise.
    data = [{"id": "1", "depth": "10"}, {"id": "2", "depth": ""}]
    steps = [
        _set_types_step(),
        {
            "run": "bcodmo_pipeline_processors.convert_units",
            "parameters": {"fields": [{"name": "depth", "conversion": "feet_to_meter"}]},
        },
    ]
    with pytest.raises(Exception):
        run_dataflows(data, steps)
    with pytest.raises(Exception):
        run_duckdb(data, STRING_SCHEMA, steps)
