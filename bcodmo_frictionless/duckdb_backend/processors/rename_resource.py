"""
Processor: bcodmo_pipeline_processors.rename_resource.

SEMANTICS
---------
Behavior: renames a single resource from ``old_resource`` to ``new_resource``
(and, in the live processor, rewrites its ``path`` and a redis progress key). Rows
and schema are unchanged.

Params:
  * ``old_resource``: existing resource name (required).
  * ``new_resource``: new resource name (required).
  * ``cache_id``: (live only) redis progress bookkeeping -- not applicable here.

Edge cases: missing ``old_resource``/``new_resource`` raises "Both old_resource and
new_resource are required"; an ``old_resource`` not present raises "Resource ...
not found" (both mirror the live processor).

TIER: structural / multi-resource. Overrides ``apply`` to rename the key in
``engine.resources`` (position preserved). No per-row work.

NOT CARRIED (Phase 0): the resource ``path`` rewrite and redis progress flag are
laminar_server / datapackage concerns, not data-equivalence concerns; the DuckDB
lane does not track a per-resource ``path`` or talk to redis. Reintroduce when this
backend emits datapackage.json / drives progress.
"""

from ..processor import Processor, register


@register
class RenameResource(Processor):
    name = "bcodmo_pipeline_processors.rename_resource"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        old = params.get("old_resource")
        new = params.get("new_resource")
        if not old or not new:
            raise Exception(
                "Both old_resource and new_resource are required parameters in "
                "rename_resource"
            )
        if old not in engine.resources:
            raise Exception(
                f'Resource "{old}" not found in datapackage. '
                f"Available resources: {list(engine.resources)}"
            )
        # Rebuild the dict so the renamed resource keeps its position.
        rebuilt = {}
        for name, st in engine.resources.items():
            if name == old:
                rebuilt[new] = engine.state(new, st.relation, st.schema)
            else:
                rebuilt[name] = st
        engine.resources = rebuilt
