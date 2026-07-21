"""
Processor: bcodmo_pipeline_processors.rename_fields.

SEMANTICS
---------
Behavior: renames fields by exact name. For each ``{old_field, new_field}`` the
column ``old_field`` becomes ``new_field`` (value carried over). Row count and
order preserved (order_effect = "keep").

Params:
  * ``fields``: list of ``{old_field, new_field}`` (exact names, applied in order)
  * ``resources``: which resources to apply to.

Edge cases / invariants: ``old_field`` missing from the schema is an error (raised
by the live processor); renaming to a ``new_field`` that already exists is an
error. Renames are treated as independent (the common case); ``SELECT * RENAME``
applies them simultaneously, which matches the live sequential pops for
independent old/new sets.

Engine notes: ``process_rows`` delegates to the live ``process_resource`` (source
of truth). ``update_schema`` applies the same rename to the tracked field list.
``to_sql`` is ``SELECT * RENAME (old AS new, ...)`` which carries ``__rownum__``
and untouched columns. Known-accepted differences: none.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.rename_fields import (
    process_resource,
)

from ..processor import Processor, register, next_alias


@register
class RenameFields(Processor):
    name = "bcodmo_pipeline_processors.rename_fields"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # Mirror the live descriptor logic: validate old exists, rename in place.
        fields = params.get("fields", [])
        names = {f["name"] for f in schema}
        out = [dict(f) for f in schema]
        for spec in fields:
            old, new = spec["old_field"], spec["new_field"]
            if old not in names:
                raise Exception(
                    f'Field "{old}" not found. Available fields: {sorted(names)}'
                )
            for f in out:
                if f["name"] == old:
                    f["name"] = new
            names.discard(old)
            names.add(new)
        return out

    def process_rows(self, rows, params, schema=None):
        return process_resource(rows, params.get("fields", []))

    def to_sql(self, con, rel, params, schema):
        fields = params.get("fields", [])
        if not fields:
            return rel
        renames = ", ".join(
            f'"{s["old_field"]}" AS "{s["new_field"]}"' for s in fields
        )
        a = next_alias("rf")
        return rel.query(a, f"SELECT * RENAME ({renames}) FROM {a}")
