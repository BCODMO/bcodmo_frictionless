"""
Equivalence tests for the field-manipulation processors (gate B):

    delete_fields  ·  bcodmo_pipeline_processors.rename_fields
                   ·  bcodmo_pipeline_processors.reorder_fields

For each, over representative data:
  * to_sql (native EXCLUDE/RENAME/identity) == process_rows (source of truth)
  * engine UDF-default path                  == process_rows
  * update_schema yields the expected output field set/order

Relations are built through the Engine so the hidden ``__rownum__`` column is
present and must be carried by the native paths.
"""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend import REGISTRY
from bcodmo_frictionless.duckdb_backend.engine import Engine, to_rows
from bcodmo_frictionless.duckdb_backend.equivalence import harness

SCHEMA = [
    {"name": "a", "type": "string"},
    {"name": "b", "type": "string"},
    {"name": "c", "type": "string"},
]
ROWS = [
    {"a": "1", "b": "x", "c": "p"},
    {"a": "2", "b": "y", "c": "q"},
    {"a": "3", "b": "z", "c": "r"},
]


def _check(name, params):
    """Assert to_sql == process_rows == udf across every output column, and that
    update_schema matches the columns the row logic actually produces."""
    proc = REGISTRY[name]

    # source of truth
    source = list(proc.process_rows([dict(r) for r in ROWS], params))
    out_cols = [f["name"] for f in proc.update_schema([dict(f) for f in SCHEMA], params)]

    # update_schema must agree with what process_rows actually emits
    assert set(out_cols) == set(source[0].keys()), (
        f"{name}: update_schema {out_cols} != process_rows keys {list(source[0].keys())}"
    )

    eng = Engine()
    eng.ingest_rows("data", ROWS, SCHEMA)
    st = eng.resources["data"]

    native = to_rows(proc.to_sql(eng.con, st.relation, params, st.schema))
    udf = to_rows(eng.udf_map(st.relation, proc, params))

    for col in out_cols:
        s = harness.column(source, col)
        n = harness.column(native, col)
        u = harness.column(udf, col)
        assert harness.diff_columns(s, n) == [], f"{name}: to_sql != process_rows on {col}"
        assert harness.diff_columns(s, u) == [], f"{name}: udf != process_rows on {col}"

    # native path must carry __rownum__ for ordering
    assert "__rownum__" in [d[0] for d in proc.to_sql(eng.con, st.relation, params, st.schema).description], (
        f"{name}: to_sql dropped __rownum__"
    )


def test_delete_fields():
    _check("delete_fields", {"fields": ["b"], "regex": False})


def test_delete_fields_regex():
    _check("delete_fields", {"fields": ["[bc]"], "regex": True})


def test_rename_fields():
    _check(
        "bcodmo_pipeline_processors.rename_fields",
        {"fields": [{"old_field": "a", "new_field": "alpha"},
                    {"old_field": "c", "new_field": "gamma"}]},
    )


def test_reorder_fields():
    _check(
        "bcodmo_pipeline_processors.reorder_fields",
        {"fields": ["c", "a", "b"]},
    )


def test_reorder_changes_output_order():
    """reorder is a schema/projection op: the engine's ordered output reflects it."""
    proc = REGISTRY["bcodmo_pipeline_processors.reorder_fields"]
    params = {"fields": ["c", "a", "b"]}
    eng = Engine()
    eng.ingest_rows("data", ROWS, SCHEMA)
    st = eng.resources["data"]
    eng.resources["data"] = eng.state(
        "data", proc.to_sql(eng.con, st.relation, params, st.schema),
        proc.update_schema(st.schema, params),
    )
    got = eng.rows("data")
    assert list(got[0].keys()) == ["c", "a", "b"], list(got[0].keys())


if __name__ == "__main__":
    test_delete_fields()
    test_delete_fields_regex()
    test_rename_fields()
    test_reorder_fields()
    test_reorder_changes_output_order()
    print("field ops: delete/rename/reorder  to_sql == process_rows == udf  ✓")
