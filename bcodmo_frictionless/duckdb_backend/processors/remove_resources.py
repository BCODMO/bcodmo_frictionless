"""
Processor: bcodmo_pipeline_processors.remove_resources.

SEMANTICS
---------
Behavior: drops every resource whose name matches the ``resources`` pattern from
the datapackage; the rows of dropped resources are discarded. Other resources are
untouched.

Params:
  * ``resources``: which resource(s) to remove (a name or list of names).

Edge cases: a pattern matching NO resource raises (mirrors the live processor,
which raises "Resource pattern ... did not match any resources").

TIER: structural / multi-resource. Overrides ``apply`` to mutate ``engine.resources``
directly (there is no per-row work). No ``update_schema``/``process_rows``/``to_sql``
row path.

NOTE: resource matching here is by exact name / name-list (``engine.matched``), not
the full dataflows ``ResourceMatcher`` glob/regex. Real pipelines remove named
resources; extend to regex if a spec needs it.
"""

from ..processor import Processor, register


@register
class RemoveResources(Processor):
    name = "bcodmo_pipeline_processors.remove_resources"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # Not used (apply is overridden); identity for contract completeness.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        matched = engine.matched(params)
        if params.get("resources") is not None and not matched:
            raise Exception(
                f"Resource pattern {params.get('resources')!r} did not match any "
                f"resources in datapackage. Available resources: {list(engine.resources)}"
            )
        for name in matched:
            del engine.resources[name]
