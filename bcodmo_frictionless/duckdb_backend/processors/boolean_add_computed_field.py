"""
Reference Processor: boolean_add_computed_field.

Demonstrates the single-source contract end to end:

  * process_rows -> delegates to the LIVE bcodmo ``process_resource`` (the one
    source of truth for the row logic; unchanged).
  * update_schema -> the schema logic (append/replace target fields in order).
  * to_sql        -> the shared DSL->SQL compiler (a verified mirror of
    process_rows, not a reimplementation).

SEMANTICS
---------
Behavior: for each configured `field`, evaluates its ordered `functions`; each
matching function sets `field.target` to its value (LAST match wins). Non-matching
rows get NULL for a new target. Adds/overwrites the target column; row count and
order unchanged (order_effect = "keep").

Params:
  * ``fields``: list of ``{target, type, functions:[{boolean, value, math_operation}]}``
  * ``missing_values``: values treated as NULL by the DSL (from the resource schema)

Edge cases / invariants: last-match-wins (compiler reverses CASE); None/missing
operands -> comparison False (SQL NULL under CASE); regex uses Python ``re.match``
anchoring; math values via the math DSL. Output order preserved.

Engine notes: ``process_rows`` is the source of truth. ``to_sql`` mirrors it for
string/regex-valued functions and is guarded by the equivalence + DSL fuzz tests.
Math-value functions (``math_operation: true``) are DEFERRED to the UDF source of
truth: bcodmo evaluates them with ``Decimal`` arithmetic whose per-value string
representation (``Decimal('180')*-1`` -> ``'180'``, not ``'180.0'``) is not
reproducible by DuckDB's DOUBLE/DECIMAL columnar types, so a step containing any
math value returns ``None`` from ``to_sql`` and runs exactly via ``process_rows``.
Known-accepted differences: none (byte-exact, verified by the differential CSV test).
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_add_computed_field import (
    process_resource,
    compute_schema_fields,
)

from ..processor import Processor, register, next_alias
from ..dsl_sql import compile_boolean_add_computed_field


@register
class BooleanAddComputedField(Processor):
    name = "bcodmo_pipeline_processors.boolean_add_computed_field"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # Single source: the exact schema transform the dataflows lane uses.
        return compute_schema_fields(schema, params["fields"])

    def process_rows(self, rows, params, schema=None):
        # THE source of truth: the live bcodmo per-row logic, unchanged.
        return process_resource(rows, params["fields"], params.get("missing_values", []))

    def to_sql(self, con, rel, params, schema):
        # OPT-IN fast path only where it is provably BYTE-exact. Math-value
        # functions use bcodmo's Decimal arithmetic, whose per-value string repr
        # (e.g. Decimal('180')*-1 -> '-180', not '-180.0') cannot be reproduced by
        # DuckDB's DOUBLE/DECIMAL columnar types. So if ANY function is a
        # math_operation, defer the whole step to the UDF source of truth (exact).
        # String/regex values (incl. the big Vessel classification) stay SQL.
        for f in params["fields"]:
            if any(fn.get("math_operation", False) for fn in f.get("functions", [])):
                return None

        # Star-based projection so __rownum__ and untouched columns pass through:
        # REPLACE existing target columns, append genuinely-new ones.
        existing = set(rel.columns)
        replaces, appends = [], []
        for f in params["fields"]:
            case = compile_boolean_add_computed_field(f)
            (replaces if f["target"] in existing else appends).append(
                f'{case} AS "{f["target"]}"'
            )
        star = f"* REPLACE ({', '.join(replaces)})" if replaces else "*"
        select = star + ("" if not appends else ", " + ", ".join(appends))
        a = next_alias("bacf")
        return rel.query(a, f"SELECT {select} FROM {a}")
