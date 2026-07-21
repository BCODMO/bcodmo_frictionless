"""
Equivalence test for bcodmo_pipeline_processors.set_types (schema/deferred-cast
tier).

set_types has NO to_sql (casting is deferred in the DuckDB lane), so the gate is
not "to_sql == process_rows" but the DEFERRED-CAST MODEL end to end:

  * process_rows casts via the shared casting module (= frictionless
    schema_validator) -- the source of truth.
  * the Engine applies set_types SCHEMA-ONLY (values stay VARCHAR), and the cast
    materializes at the output boundary (`typed_rows`) using the final schema.
  * typed_rows(engine) == process_rows(input, schema)  -- i.e. deferring the cast
    to output produces exactly the values the dataflows lane would cast inline.
"""

import datetime

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend import REGISTRY
from bcodmo_frictionless.duckdb_backend.engine import Engine
from bcodmo_frictionless.duckdb_backend.equivalence import harness

PROC = REGISTRY["bcodmo_pipeline_processors.set_types"]

# All fields start as strings (as after an all-VARCHAR ingest).
SCHEMA = [
    {"name": "depth", "type": "string"},
    {"name": "count", "type": "string"},
    {"name": "label", "type": "string"},
    {"name": "when", "type": "string"},
]
ROWS = [
    {"depth": "10.5", "count": "3", "label": "a", "when": "2020-01-15"},
    {"depth": "0", "count": "0", "label": "b", "when": "2021-12-31"},
    {"depth": "-3.25", "count": "42", "label": "c", "when": "1999-06-01"},
]

PARAMS = {
    "regex": False,
    "types": {
        "depth": {"type": "number"},
        "count": {"type": "integer"},
        "when": {"type": "date", "format": "%Y-%m-%d"},
    },
}


def test_update_schema_applies_types_and_output_format():
    new = PROC.update_schema([dict(f) for f in SCHEMA], PARAMS)
    by = {f["name"]: f for f in new}
    assert by["depth"]["type"] == "number"
    assert by["count"]["type"] == "integer"
    assert by["label"]["type"] == "string"          # untouched
    assert by["when"]["type"] == "date"
    # temporal outputFormat derived from format
    assert by["when"]["outputFormat"] == "%Y-%m-%d"


def test_deferred_cast_matches_process_rows():
    # source of truth: cast inline (what the dataflows lane does)
    expected = list(PROC.process_rows([dict(r) for r in ROWS], PARAMS, SCHEMA))

    # DuckDB lane: schema-only apply, cast deferred to the output boundary
    eng = Engine()
    eng.ingest_rows("data", ROWS, SCHEMA)
    eng.run([{"run": PROC.name, "parameters": PARAMS}])

    # values are still VARCHAR in storage until we cast at output
    stored = eng.rows("data")
    assert stored[0]["depth"] == "10.5" and isinstance(stored[0]["depth"], str)

    actual = eng.typed_rows("data")

    assert len(actual) == len(expected) == 3
    for col in ["depth", "count", "label", "when"]:
        a = [r.get(col) for r in actual]
        e = [r.get(col) for r in expected]
        assert a == e, f"column {col}: duckdb={a} expected={e}"

    # and the casts really are typed, not strings
    assert actual[0]["depth"] == 10.5
    assert actual[0]["count"] == 3
    assert actual[0]["when"] == datetime.date(2020, 1, 15)


if __name__ == "__main__":
    test_update_schema_applies_types_and_output_format()
    test_deferred_cast_matches_process_rows()
    print("set_types: schema-only apply + deferred cast == frictionless  ✓")
