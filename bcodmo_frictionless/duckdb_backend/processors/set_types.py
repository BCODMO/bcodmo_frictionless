"""
Processor: bcodmo_pipeline_processors.set_types.

SEMANTICS
---------
Behavior: assigns frictionless types/formats to fields whose names match the keys
of ``types`` (each key literal or regex per ``regex``), then the values are cast
to those types. Adds no fields, drops no rows; row order preserved
(order_effect = "keep"). For temporal fields with an input ``format`` and no
explicit ``outputFormat``, ``outputFormat`` is set to ``format`` so written data
matches the input (mirrors the live processor).

Params:
  * ``types``: ``{name_or_regex: {type, format, ...}}``
  * ``regex``: whether the ``types`` keys are regexes (default True, matching the
    live ``flow()``; the UI sends ``false``)
  * ``resources``: which resources to apply to.

Edge cases: a ``types`` key matching no field is an error (mirrors the live
processor). A cell that cannot be cast raises (frictionless default), which is how
set_types can fail a pipeline on bad data.

TIER: schema / deferred-cast (PLAN.md §storage). The DuckDB lane stores VARCHAR
and casts at the UDF boundary and at dump, so set_types here is SCHEMA-ONLY: it
updates field types and leaves the stored values untouched (``apply`` override).
It therefore has NO ``to_sql`` mirror -- casting is not applied at this step in the
DuckDB lane. Correctness is proven END TO END (byte-identical dump via the shared
``casting`` module = frictionless ``schema_validator``), not by a per-step
``to_sql == process_rows`` gate.

``process_rows`` remains the source-of-truth cast semantics (delegates to the
shared ``casting.cast_rows`` = frictionless ``schema_validator``): it is what the
dataflows lane does, what the UDF boundary/dump reuse, and what the equivalence
test pins.
"""

import re

from ..processor import Processor, register
from ..casting import cast_rows


def _apply_types(fields, params):
    """The one place set_types' schema transform lives: return a new field list
    with matched fields' descriptors updated (+ temporal outputFormat derivation).
    Mirrors the live set_types descriptor logic."""
    types = params.get("types", {})
    regex = params.get("regex", True)
    out = [dict(f) for f in fields]
    field_names = [f["name"] for f in out]
    for name, options in types.items():
        pattern = re.compile(f"^{name}$" if regex else f"^{re.escape(name)}$")
        if not any(pattern.match(n) for n in field_names):
            raise Exception(
                f'Type pattern "{name}" did not match any fields. '
                f"Available fields: {sorted(field_names)}"
            )
        for field in out:
            if pattern.match(field["name"]):
                field.update(options)
                if (
                    field.get("type") in ["datetime", "date", "time"]
                    and field.get("format")
                    and "outputFormat" not in options
                ):
                    field["outputFormat"] = field["format"]
    return out


@register
class SetTypes(Processor):
    name = "bcodmo_pipeline_processors.set_types"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return _apply_types(schema, params)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: cast to the UPDATED field types via frictionless.
        if schema is None:
            raise ValueError("set_types.process_rows requires the input schema")
        new_fields = _apply_types(schema, params)
        return cast_rows(rows, new_fields)

    def apply(self, engine, params):
        # Schema/deferred-cast tier: update field types, leave VARCHAR values as
        # they are. The cast materializes at the UDF boundary / dump.
        for name in engine.matched(params):
            st = engine.resources[name]
            engine.resources[name] = engine.state(
                name, st.relation, self.update_schema(st.schema, params)
            )
