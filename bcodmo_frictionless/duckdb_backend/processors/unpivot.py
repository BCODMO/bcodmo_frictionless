"""
Processor: unpivot (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Reshapes wide -> long. Each matched resource's ``unpivot`` fields (matched by regex
on ``name``) are collapsed: every unpivoted column becomes one output row carrying
the ``fields_to_keep`` columns, the ``extraKeyFields`` (their values derived from
the pivoted column name via regex substitution on ``keys``), and the
``extraValueField`` holding the pivoted cell's value. Schema and row count both
change (order_effect = "reset").

Params (matching ``standard_flows.unpivot``):
  * ``unpivot``: ``[{name: <regex>, keys: {extra_key_name: <value-or-\\N-ref>}}]``
  * ``extraKeyFields``: ``[{name, type}, ...]`` appended to the schema.
  * ``extraValueField``: ``{name, type}`` holding the unpivoted value.
  * ``resources``: which resources to apply to.

TIER: structural / reshape+recount. Overrides ``apply``. Both the NEW SCHEMA and
the NEW ROWS are the LIVE ``dataflows.processors.unpivot`` reused VERBATIM -- we
drive the real package-processing generator over a package/resource shim (same
trick as join). ``next(gen)`` runs its process_datapackage (which rewrites each
matched resource's ``schema['fields']`` to keep + extraKeys + extraValue), then
each yield is that resource's reshaped rows. unpivot only MOVES values (no
arithmetic), so VARCHAR rows are fed and re-ingested directly -- no cast needed
(like concatenate). Only matched resources are re-ingested (fresh ``__rownum__``);
others keep their state. The ``regex`` flag defaults True (as standard_flows).
"""

import copy

from dataflows.processors.unpivot import unpivot as _live_unpivot

from ..processor import Processor, register


class _Res:
    def __init__(self, name):
        self.name = name


class _ResourceShim:
    def __init__(self, name, rows):
        self.res = _Res(name)
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _Pkg:
    def __init__(self, descriptor):
        self.descriptor = descriptor


class _PackageShim:
    def __init__(self, descriptor, resources):
        self.pkg = _Pkg(descriptor)
        self._resources = resources

    def __iter__(self):
        return iter(self._resources)


@register
class Unpivot(Processor):
    name = "unpivot"
    order_effect = "reset"

    def update_schema(self, schema, params):
        # Not used for the apply; present for contract completeness.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        # NEVER-OOM: feed only the matched resources (the ones unpivot reshapes),
        # each as a LAZY VARCHAR stream, and stream each reshaped output straight
        # into a fresh table via reingest_stream -- nothing materializes a resource
        # or its (larger) reshaped output. Non-matched resources are untouched and
        # keep their slot. unpivot only MOVES values (no cast). Reading a resource
        # on an independent cursor (rows_iter) makes writing its replacement on the
        # main connection collision-free.
        matched = engine.matched(params)

        descriptor = {"resources": []}
        shims = []
        for n in matched:
            schema = engine.resources[n].schema
            descriptor["resources"].append(
                {"name": n, "schema": {"fields": copy.deepcopy(schema)}}
            )
            shims.append(_ResourceShim(n, engine.rows_iter(n)))

        package = _PackageShim(descriptor, shims)
        func = _live_unpivot(
            copy.deepcopy(params.get("unpivot")),
            copy.deepcopy(params.get("extraKeyFields")),
            copy.deepcopy(params.get("extraValueField")),
            resources=params.get("resources"),
        )
        gen = func(package)
        next(gen)  # process_datapackage rewrites matched resources' schemas in place
        new_descs = descriptor["resources"]

        # Iterate ``gen`` itself (index new_descs by position) so it runs to
        # StopIteration -- every fed resource is matched, so each output streams
        # into its own fresh table (reshaped rows + new schema, fresh __rownum__).
        for i, it in enumerate(gen):
            desc = new_descs[i]
            engine.reingest_stream(desc["name"], it, desc["schema"]["fields"])
