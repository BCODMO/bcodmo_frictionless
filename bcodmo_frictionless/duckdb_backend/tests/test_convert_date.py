"""
Differential test (layer C) for convert_date -- the processor that exercises the
temporal format-out round-trip END TO END.

A convert_date output field carries only ``outputFormat`` (no cast ``format``).
The dataflows lane keeps a live ``datetime`` in memory; the DuckDB lane stores
VARCHAR and must round-trip it through that ``outputFormat`` (the casting.py
storage-format invariant). ``assert_csv_identical`` proves both lanes emit
byte-identical CSV, which only holds if that round-trip is exact.

Covers: python strptime (single input_field), multiple ``inputs`` (date + time),
output_type=date, output_type=string, and a chained convert_date whose input is a
field produced by an earlier convert_date (typed-input re-serialization path).
"""

from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

STRING_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "date_str", "type": "string"},
    {"name": "time_str", "type": "string"},
]
DATA = [
    {"id": "1", "date_str": "2020-01-15", "time_str": "03:04:05"},
    {"id": "2", "date_str": "1999-12-31", "time_str": "23:59:59"},
    {"id": "3", "date_str": "2021-06-01", "time_str": "00:00:00"},
]


def test_single_input_datetime():
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "dt", "output_format": "%Y-%m-%d %H:%M:%S",
             "input_field": "date_str", "input_format": "%Y-%m-%d"},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_output_type_date():
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "d", "output_type": "date", "output_format": "%m/%d/%Y",
             "input_field": "date_str", "input_format": "%Y-%m-%d"},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_output_type_string():
    # output_type=string -> the output field is a plain string (strftime'd),
    # no temporal round-trip, but still must match byte-for-byte.
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "iso", "output_type": "string", "output_format": "%Y%m%dT%H%M%S",
             "input_field": "date_str", "input_format": "%Y-%m-%d"},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_multiple_inputs():
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "dt", "output_format": "%Y-%m-%dT%H:%M:%S", "inputs": [
                {"field": "date_str", "format": "%Y-%m-%d"},
                {"field": "time_str", "format": "%H:%M:%S"},
            ]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


def test_chained_convert_date_reads_typed_field():
    # First convert_date makes a datetime field; the second reads THAT field
    # (a temporal input -> re-serialized via its outputFormat). This is the
    # storage-format round-trip stressed twice.
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "dt", "output_format": "%Y-%m-%d %H:%M:%S",
             "input_field": "date_str", "input_format": "%Y-%m-%d"},
        ]}},
        {"run": "bcodmo_pipeline_processors.convert_date", "parameters": {"fields": [
            {"output_field": "year_only", "output_type": "string", "output_format": "%Y",
             "inputs": [{"field": "dt", "format": "%Y-%m-%d %H:%M:%S"}]},
        ]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)
    assert_csv_identical(DATA, STRING_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ✓")
