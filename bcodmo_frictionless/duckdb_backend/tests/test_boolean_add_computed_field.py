"""
Phase 0 acceptance test for the single-source contract, on the reference
processor (boolean_add_computed_field).

Proves, over edge-case data:
  * to_sql (native fast path)  ==  process_rows (source of truth)   [gate B]
  * engine UDF-default path     ==  process_rows                    [plumbing]
  * DSL fuzz: interpreter == compiler over random inputs            [gate B]
"""

import duckdb
import pytest

from bcodmo_frictionless.duckdb_backend import REGISTRY
from bcodmo_frictionless.duckdb_backend import engine
from bcodmo_frictionless.duckdb_backend.equivalence import harness, fuzz_dsl

PROC = REGISTRY["bcodmo_pipeline_processors.boolean_add_computed_field"]

# ---- representative field configs (Vessel shape, regex, math value) ----------
VESSEL = {
    "target": "Vessel", "type": "string",
    "functions": [
        {"boolean": " {Cruise_num_temp}==1 or {Cruise_num_temp}==2", "value": "R/V Weatherbird I", "math_operation": False},
        {"boolean": "{Cruise_num_temp}>=3 and {Cruise_num_temp}<=7", "value": "R/V Cape Henlopen", "math_operation": False},
        {"boolean": "{Cruise_num_temp}>=8 and {Cruise_num_temp}<=13", "value": "R/V Weatherbird I", "math_operation": False},
        {"boolean": "{Cruise_num_temp}==242", "value": "R/V Oceanus", "math_operation": False},
        {"boolean": "{Cruise_num_temp}==356 and {Cruise_num_temp}==357", "value": "R/V Endeavor", "math_operation": False},
        {"boolean": "{Cruise_num_temp}>=423 and {Cruise_num_temp}<=424", "value": "R/V Neil Armstrong", "math_operation": False},
    ],
}
CRUISE_TYPE = {
    "target": "Cruise_type", "type": "string",
    "functions": [
        {"boolean": "{Cruise_num} == re'^1.*'", "value": "BATS Core", "math_operation": False},
        {"boolean": "{Cruise_num} == re'^2.*'", "value": "BATS Bloom A", "math_operation": False},
        {"boolean": "{Cruise_num} == re'^3.*'", "value": "BATS Bloom B", "math_operation": False},
    ],
}
LONGITUDE = {
    "target": "Longitude_deployed", "type": "number",
    "functions": [
        {"boolean": "{Longitude_west_in}>=0", "value": "{Longitude_west_in}*(-1)", "math_operation": True},
    ],
}
FIELD_CONFIGS = [VESSEL, CRUISE_TYPE, LONGITUDE]

SCHEMA = [
    {"name": "Cruise_num_temp", "type": "number"},
    {"name": "Cruise_num", "type": "string"},
    {"name": "Longitude_west_in", "type": "number"},
    {"name": "Longitude_west_out", "type": "number"},
]

_temps = [None, 1, 2, 3, 7, 8, 13, 242, 356, 357, 422, 423, 424, 999]
_nums = ["10001", "19999", "20001", "30001", "40001", None]
_longs = [-5.0, 0.0, 64.5, 180.0, None]


def _rows():
    return [
        {"Cruise_num_temp": t,
         "Cruise_num": _nums[i % len(_nums)],
         "Longitude_west_in": _longs[i % len(_longs)],
         "Longitude_west_out": _longs[(i + 2) % len(_longs)]}
        for i, t in enumerate(_temps)
    ]


def _base_relation(con, rows):
    con.execute(
        'CREATE OR REPLACE TABLE data (id INTEGER, "Cruise_num_temp" DOUBLE, '
        '"Cruise_num" VARCHAR, "Longitude_west_in" DOUBLE, "Longitude_west_out" DOUBLE)'
    )
    con.executemany("INSERT INTO data VALUES (?,?,?,?,?)", [
        (i, r["Cruise_num_temp"], r["Cruise_num"], r["Longitude_west_in"], r["Longitude_west_out"])
        for i, r in enumerate(rows)
    ])
    return con.sql('SELECT "Cruise_num_temp", "Cruise_num", "Longitude_west_in", "Longitude_west_out" FROM data ORDER BY id')


@pytest.mark.parametrize("field_cfg", FIELD_CONFIGS, ids=lambda f: f["target"])
def test_to_sql_matches_process_rows(field_cfg):
    con = duckdb.connect()
    rows = _rows()
    params = {"fields": [field_cfg], "missing_values": []}
    target = field_cfg["target"]

    is_math = any(
        fn.get("math_operation", False) for fn in field_cfg.get("functions", [])
    )

    # source of truth
    source = harness.column(list(PROC.process_rows([dict(r) for r in rows], params)), target)

    # engine UDF-default path (the source of truth on a relation) — always valid
    rel = _base_relation(con, rows)
    udf = harness.column(engine.run_udf(con, rel, PROC, params), target)
    assert harness.diff_columns(source, udf) == [], f"udf != process_rows for {target}"

    # native to_sql path (gate B): only for string/regex values. Math-value
    # functions intentionally defer to the UDF (Decimal repr), so to_sql is None.
    rel = _base_relation(con, rows)
    native_rel = PROC.to_sql(con, rel, params, SCHEMA)
    if is_math:
        assert native_rel is None, f"{target}: math value should defer to UDF (to_sql None)"
    else:
        native = harness.column(engine.to_rows(native_rel), target)
        assert harness.diff_columns(source, native) == [], f"to_sql != process_rows for {target}"


def test_dsl_fuzz_interpreter_equals_compiler():
    con = duckdb.connect()
    mismatches = fuzz_dsl.run(con, iterations=3000)
    assert mismatches == [], f"{len(mismatches)} DSL mismatches, e.g. {mismatches[:5]}"


if __name__ == "__main__":
    for fc in FIELD_CONFIGS:
        test_to_sql_matches_process_rows(fc)
        print(f"  {fc['target']:<20} to_sql == process_rows == udf  ✓")
    test_dsl_fuzz_interpreter_equals_compiler()
    print("  DSL fuzz (3000 cases)   interpreter == compiler  ✓")
    print("\nPhase 0 reference: ALL EQUIVALENCE CHECKS PASS")
