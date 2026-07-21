"""
Processor: boolean_filter_rows.

SEMANTICS
---------
Behavior: keeps only rows for which ``boolean_statement`` (the boolean DSL)
evaluates truthy; drops the rest. Schema unchanged. Row order of surviving rows
preserved (order_effect = "keep").

Params:
  * ``boolean_statement``: a boolean DSL expression
  * ``missing_values``: values treated as NULL by the DSL
  * ``resources``: which resources to filter

Edge cases: None/missing operands follow the DSL's None-safety (comparison
False); a missing/empty statement keeps all rows.

Engine notes: ``process_rows`` delegates to the live ``_boolean_filter_rows``
(source of truth). ``to_sql`` is ``WHERE compile(statement)`` from the shared DSL
compiler; guarded by the DSL fuzz + differential tests. Known-accepted
differences: none.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_filter_rows import (
    _boolean_filter_rows,
)

from ..processor import Processor, register, next_alias
from ..dsl_sql import compile_boolean_statement


@register
class BooleanFilterRows(Processor):
    name = "bcodmo_pipeline_processors.boolean_filter_rows"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return schema

    def process_rows(self, rows, params, schema=None):
        return _boolean_filter_rows(
            rows, params.get("missing_values", []), params.get("boolean_statement")
        )

    def to_sql(self, con, rel, params, schema):
        cond = compile_boolean_statement(params.get("boolean_statement"))
        if cond is None:
            return rel
        # WHERE keeps __rownum__ and all columns; only drops rows.
        a = next_alias("bfr")
        return rel.query(a, f"SELECT * FROM {a} WHERE {cond}")
