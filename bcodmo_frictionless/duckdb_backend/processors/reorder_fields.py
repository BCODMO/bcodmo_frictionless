"""
Processor: bcodmo_pipeline_processors.reorder_fields.

SEMANTICS
---------
Behavior: reorders the schema field list to the given ``fields`` order. Row VALUES
are untouched -- the live processor only rewrites the resource descriptor's field
order and passes rows through verbatim (``yield from package``). Output column
order is then governed by the schema. Row count and order preserved
(order_effect = "keep").

Params:
  * ``fields``: the full list of field names in the desired order. Must be a
    permutation of the existing fields (every field named exactly once).
  * ``resources``: which resources to apply to.

Edge cases: a name not present, or a list whose length differs from the field
count, is an error (mirrors the live processor).

Engine notes: this is a pure schema/projection op. ``update_schema`` returns the
fields in the new order; ``process_rows`` is identity (values unchanged, as in the
library); ``to_sql`` returns the relation unchanged -- the engine projects columns
in schema order at ``rows()``/``to_csv()`` time, so ``__rownum__`` and values ride
through untouched. Known-accepted differences: none.
"""

from ..processor import Processor, register


@register
class ReorderFields(Processor):
    name = "bcodmo_pipeline_processors.reorder_fields"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        by_name = {f["name"]: f for f in schema}
        new_list = []
        for name in fields:
            if name not in by_name:
                raise Exception(
                    f"Field {name} not found in the list of fields: {list(by_name)}"
                )
            new_list.append(by_name[name])
        if len(new_list) != len(schema):
            raise Exception(
                f"Only {len(new_list)} were passed in to the reorder_fields step, "
                f"{len(schema)} required"
            )
        return new_list

    def process_rows(self, rows, params, schema=None):
        # The live processor leaves row values untouched; only field ORDER (in the
        # schema) changes. Output column order is applied at projection time.
        return iter(rows)

    def to_sql(self, con, rel, params, schema):
        # No row/column-content change; schema reorder handles output ordering.
        return rel
