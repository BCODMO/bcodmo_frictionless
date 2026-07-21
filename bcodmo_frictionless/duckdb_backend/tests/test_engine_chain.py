"""
Chained-engine equivalence: a multi-step pipeline (filter -> boolean_add) run
through the DuckDB Engine matches the bcodmo row logic applied directly, with
order preserved via __rownum__.
"""

from bcodmo_frictionless.duckdb_backend.engine import Engine
from bcodmo_frictionless.duckdb_backend.equivalence import harness

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_filter_rows import (
    _boolean_filter_rows,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_add_computed_field import (
    process_resource,
    compute_schema_fields,
)

# string-safe (regex) computed field: valid in bcodmo before set_types casts.
CRUISE_TYPE = {
    "target": "Cruise_type", "type": "string",
    "functions": [
        {"boolean": "{Cruise_num} == re'^1.*'", "value": "BATS Core", "math_operation": False},
        {"boolean": "{Cruise_num} == re'^2.*'", "value": "BATS Bloom A", "math_operation": False},
        {"boolean": "{Cruise_num} == re'^3.*'", "value": "BATS Bloom B", "math_operation": False},
    ],
}
SCHEMA = [{"name": "keep", "type": "string"}, {"name": "Cruise_num", "type": "string"}]
INPUT = [
    {"keep": "yes", "Cruise_num": "10001"},
    {"keep": "no", "Cruise_num": "30002"},
    {"keep": "yes", "Cruise_num": "20003"},
    {"keep": "yes", "Cruise_num": "40004"},
    {"keep": "no", "Cruise_num": "10005"},
    {"keep": "yes", "Cruise_num": "30006"},
]

STEPS = [
    {"run": "bcodmo_pipeline_processors.boolean_filter_rows",
     "parameters": {"resources": ["data"], "boolean_statement": "{keep} == 'yes'", "missing_values": []}},
    {"run": "bcodmo_pipeline_processors.boolean_add_computed_field",
     "parameters": {"resources": ["data"], "fields": [CRUISE_TYPE], "missing_values": []}},
]


def _expected():
    filtered = list(_boolean_filter_rows(iter([dict(r) for r in INPUT]), [], "{keep} == 'yes'"))
    added = list(process_resource(filtered, [CRUISE_TYPE], []))
    schema = [f["name"] for f in compute_schema_fields(SCHEMA, [CRUISE_TYPE])]
    return [{k: r.get(k) for k in schema} for r in added]


def test_engine_chain_matches_bcodmo():
    import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)

    eng = Engine()
    eng.ingest_rows("data", INPUT, SCHEMA)
    eng.run(STEPS)
    actual = eng.rows("data")

    expected = _expected()
    assert len(actual) == len(expected) == 4, (len(actual), len(expected))
    keys = ["keep", "Cruise_num", "Cruise_type"]
    for k in keys:
        a = [harness.norm(r.get(k)) for r in actual]
        e = [harness.norm(r.get(k)) for r in expected]
        assert a == e, f"column {k}: duckdb={a} bcodmo={e}"


if __name__ == "__main__":
    test_engine_chain_matches_bcodmo()
    print("engine chain (filter -> boolean_add) == bcodmo, order preserved  ✓")
