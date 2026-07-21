"""
Processor: duplicate (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Copies one resource to a new resource with the same schema and the same rows in
the same order. The copy is inserted immediately AFTER the source, or appended at
the end when ``duplicate_to_end`` is set.

Params (matching ``standard_flows.duplicate``):
  * ``source``: resource to copy (default: the first resource).
  * ``target-name``: copy's name (default: ``<source>_copy``).
  * ``target-path``: copy's path (not tracked by the field-list schema; ignored).
  * ``duplicate_to_end``: append the copy at the end instead of after the source.

TIER: structural / multi-resource. Overrides ``apply``. The live
``dataflows.duplicate`` streams the copy through a KVFile keyed by an 8-hex row
index, which PRESERVES source order; ``engine.rows(source)`` is already in
``__rownum__`` (source) order, so re-ingesting it yields the identical order with a
fresh ``__rownum__``. The source resource is left untouched. Byte-identical by
construction (a copy changes no values). ``target-path`` and redis progress are not
carried (Phase 0).
"""

import copy

from ..processor import Processor, register


@register
class Duplicate(Processor):
    name = "duplicate"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        names = list(engine.resources)
        if not names:
            raise Exception("duplicate: no resources to copy")
        source = params.get("source") or names[0]
        if source not in engine.resources:
            raise Exception(
                f"duplicate source resource ({source}) not found; resources={names}"
            )
        target_name = params.get("target-name") or (source + "_copy")
        if target_name in engine.resources:
            raise Exception(
                f"duplicate target ({target_name}) already exists; resources={names}"
            )
        duplicate_to_end = params.get("duplicate_to_end", False)

        st = engine.resources[source]
        rows = engine.rows(source)  # VARCHAR, source order
        schema = [copy.deepcopy(f) for f in st.schema]
        engine.ingest_rows(target_name, rows, schema)  # fresh __rownum__ = source order
        copy_state = engine.resources.pop(target_name)  # remove from end for repositioning

        new_order = {}
        for n in names:
            new_order[n] = engine.resources[n]
            if n == source and not duplicate_to_end:
                new_order[target_name] = copy_state
        if duplicate_to_end:
            new_order[target_name] = copy_state
        engine.resources = new_order
