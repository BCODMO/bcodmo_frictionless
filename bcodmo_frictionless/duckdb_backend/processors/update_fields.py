"""
Processor: bcodmo_pipeline_processors.update_fields.

SEMANTICS
---------
Behavior: merges the given ``props`` into each named field's descriptor
(``field.update(props)`` in the live processor). Row VALUES pass through unchanged
(the live flow does ``yield from package``); only field metadata changes. No fields
added/removed, no rows dropped, order preserved (order_effect = "keep").

Params:
  * ``fields``: ``{field_name: {prop: value, ...}}`` -- props merged into that field.
  * ``resources``: which resources to apply to.

Edge cases: a ``field_name`` not present raises (mirrors the live processor).

TIER: schema-only / deferred-cast (PLAN.md §4.2). Like set_types this updates the
tracked field descriptors and leaves the stored VARCHAR values untouched (``apply``
override); casting materializes at the UDF boundary / dump against the FINAL schema.
No ``to_sql`` (nothing to compute on the rows).

KNOWN LIMITATION: the live processor does NOT re-cast existing row values when a
prop changes a field's ``type`` (rows pass through as-is), whereas the DuckDB lane
casts VARCHAR by the final type at the output boundary. So using update_fields to
CHANGE a field's type would diverge -- and the differential test would catch it.
Real usage updates descriptive metadata (title, units, description, ``bcodmo:``),
not types; that is fully equivalent.
"""

from ..processor import Processor, register


def _apply_updates(fields, params):
    """The one place update_fields' schema transform lives: merge each field's
    props into the matching field descriptor. Mirrors the live update_fields."""
    updates = params.get("fields", {})
    out = [dict(f) for f in fields]
    for field_name, props in updates.items():
        target = next((f for f in out if f["name"] == field_name), None)
        if target is None:
            raise Exception(f'Field "{field_name}" not found in the datapackage')
        target.update(props)
    return out


@register
class UpdateFields(Processor):
    name = "bcodmo_pipeline_processors.update_fields"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return _apply_updates(schema, params)

    def process_rows(self, rows, params, schema=None):
        # Rows are unchanged; this exists for contract completeness (the DuckDB
        # lane uses the schema-only apply override below, not this).
        return iter(rows)

    def apply(self, engine, params):
        # Schema-only tier: update field descriptors, leave VARCHAR values as-is.
        for name in engine.matched(params):
            st = engine.resources[name]
            engine.resources[name] = engine.state(
                name, st.relation, self.update_schema(st.schema, params)
            )
