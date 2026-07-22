"""
Processor: update_resource (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Merges ``metadata`` props into the descriptor of every matched resource (matched by
the ``resources`` param -- a name, list, or None=all). Common uses: attach
resource-level metadata (``title``, ``bcodmo:`` keys), attach field metadata via a
``schema``, or RENAME a resource (``metadata['name']``). Rows are unchanged
(order_effect = "keep"); the merge surfaces in the dumped ``datapackage.json``.

Mirrors ``standard_flows.update_resource`` = ``resources = params.get('resources')``,
``metadata = params.pop('metadata', {})``, ``Flow(update_resource(resources,
**metadata))``, whose live ``dataflows.update_resource`` does ``resource.update(
props)`` for matched resources. We carry the merged descriptor on the engine's
per-resource state (which ``dump_to_s3`` re-stamps into datapackage.json), sync the
tracked schema fields if a ``schema`` was provided, and rename the resource if
``name`` was provided -- so the output matches the dataflows lane byte-for-byte.

TIER: structural / resource-metadata. ``apply``-only (no row change), so
``update_schema``/``process_rows`` are inert (contract completeness).
"""

import copy

from ..processor import Processor, register


@register
class UpdateResource(Processor):
    name = "update_resource"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        metadata = copy.deepcopy(params.get("metadata", {}) or {})
        matched = set(engine.matched(params))  # reads params["resources"]
        new_name = metadata.get("name")

        result = {}
        for name, st in engine.resources.items():
            if name not in matched:
                result[name] = st
                continue
            # Merge metadata into the carried descriptor (for datapackage.json).
            desc = copy.deepcopy(st.descriptor) if st.descriptor else {"name": name}
            desc.update(metadata)
            # If a schema was provided, sync the tracked fields (attaches field
            # metadata; a real type change is not a supported use of update_resource).
            schema = st.schema
            sc = metadata.get("schema")
            if isinstance(sc, dict) and "fields" in sc:
                schema = [dict(f) for f in sc["fields"]]
            # Rename if requested (dict key + descriptor name). Multiple matched
            # resources renamed to one name collapse, mirroring the descriptor
            # collision the live primitive would produce.
            out_name = new_name or name
            desc["name"] = out_name
            result[out_name] = engine.state(out_name, st.relation, schema, desc)
        engine.resources = result
