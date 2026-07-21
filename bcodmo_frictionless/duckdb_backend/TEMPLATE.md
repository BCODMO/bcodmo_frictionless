# Adding a leaf processor to the DuckDB backend

This is the recipe every single-resource processor follows. It exists so the two
engines (dataflows lane, DuckDB lane) can **never diverge**: the row logic is
authored ONCE (the live `bcodmo_pipeline_processors` `process_resource`) and the
DuckDB lane reuses it verbatim. A byte-identical differential test is the gate.

Read these reference implementations before you start:

| pattern                                                                  | reference                                                    |
| ------------------------------------------------------------------------ | ------------------------------------------------------------ |
| UDF-only value/temporal transform (delegates to live `process_resource`) | `processors/convert_date.py`                                 |
| Native `to_sql` fast path (byte-exact only) + delegate                   | `processors/delete_fields.py`, `processors/rename_fields.py` |
| Schema-only / deferred-cast tier (`apply` override, no `to_sql`)         | `processors/set_types.py`                                    |

## The contract (see `processor.py`)

A processor subclasses `Processor` and sets `name` = the spec `run:` value
(e.g. `"bcodmo_pipeline_processors.round_fields"`). It provides:

- `update_schema(self, schema, params) -> list` — the new frictionless field list
  (list of `{"name","type",...}` dicts). This MUST mirror the schema mutation the
  live `flow()`/`func(package)` performs on `resource["schema"]["fields"]`. This is
  the one place the DuckDB lane's schema logic lives.
- `process_rows(self, rows, params, schema=None) -> iterable[dict]` — THE row
  logic. **Delegate to the live `process_resource`** — do not reimplement. `schema`
  is the INPUT frictionless field list (list of dicts); most delegates need it only
  to build `missing_values`/`datapackage_fields`.
- `to_sql(self, con, rel, params, schema)` — OPTIONAL. Return a `DuckDBPyRelation`
  ONLY if it is **provably byte-exact** vs `process_rows`. **Default: don't write
  one** (inherit the base `return None` → the UDF path runs the live code). Value
  transforms that touch `Decimal`/`float`/temporal math are NOT byte-exact in SQL —
  leave `to_sql` unset. `to_sql` is opt-in only for structural column ops
  (EXCLUDE/RENAME/REPLACE) where DuckDB reproduces the bytes exactly.

## Rules

1. **One resource, 1:1 rows.** The default `Processor.apply` handles single-resource
   row processors where row count is preserved. If your processor filters or expands
   rows, it needs a `to_sql` (the UDF path asserts 1:1) — ask before doing that.
2. **Delegate row logic.** `process_rows` calls the live `process_resource`. Match
   its exact signature (they differ per processor — read the live file). Common args:
   - `missing_values` → use `[""]` (the frictionless resource default that the live
     `get_missing_values` returns; resource-level missingValues aren't tracked yet).
   - `boolean_statement` → `params.get("boolean_statement")` when the live one takes it.
3. **Cast-in is automatic.** The engine casts VARCHAR→typed using the INPUT schema
   before `process_rows`, so a numeric processor sees `Decimal` exactly as the
   dataflows lane does. For numeric processors, your differential test should run a
   `set_types`→number step first (as real pipelines do) so the field is numeric.
4. **Temporal outputs:** create fields with `outputFormat` (mirror the live schema);
   the `casting.py` storage-format invariant round-trips the VARCHAR. Don't set a
   cast `format` unless the live schema does.
5. **Do NOT edit `__init__.py` or `PLAN.md`** (avoids parallel-edit conflicts). Instead,
   import your processor module at the top of your TEST file so `@register` runs:
   `from bcodmo_frictionless.duckdb_backend.processors import round_fields  # noqa: F401`
   (Central registration into `__init__.py` is done afterward.)

## Test (the gate)

Create `tests/test_<name>.py`. Use the differential harness:

```python
from bcodmo_frictionless.duckdb_backend.processors import round_fields  # noqa: F401  (register)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent, assert_csv_identical,
)

STRING_SCHEMA = [{"name": "id", "type": "string"}, {"name": "x", "type": "string"}]
DATA = [{"id": "1", "x": "10.567"}, {"id": "2", "x": "-3.2"}, {"id": "3", "x": ""}]

def test_round():
    steps = [
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"regex": False, "types": {"x": {"type": "number"}}}},
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [{"name": "x", "digits": 2}]}},
    ]
    assert_equivalent(DATA, STRING_SCHEMA, steps)     # typed rows + schema match
    assert_csv_identical(DATA, STRING_SCHEMA, steps)  # byte-identical CSV
```

Cover the real param shapes (from the live processor / UI), plus edge cases:
missing/empty values, negative numbers, `boolean_statement` gating if supported,
and (for temporal) an explicit output format. Include an empty-string row to
exercise missing-value handling.

Run: `python -m pytest bcodmo_frictionless/duckdb_backend/tests/test_<name>.py -q`
and iterate until green. `assert_csv_identical` failing means the DuckDB lane's
bytes differ from dataflows — that is a real divergence; fix it, don't weaken the test.
