"""
Processor: bcodmo_pipeline_processors.load -- the SOURCE that seeds the engine.

Unlike every other processor (which is an N->N transform), ``load`` is a 0->N
source: it reads files/URLs/S3 objects and ADDS resources to the package. It is
also the operation that produces exactly the state the rest of the DuckDB backend
assumes as its starting point -- all-VARCHAR rows + an all-``string``-typed schema
-- because bcodmo ``load`` hard-forces ``infer_strategy="strings"`` /
``cast_strategy="strings"``.

SEMANTICS
---------
Read the ``from`` source(s) and ADD one resource per file/sheet to the engine
(names via ``name``/``use_filename``, multi-file/sheet expansion, S3 glob), each
as all-VARCHAR rows + an all-``string`` schema, with ``remove_empty_rows`` applied.
Existing resources are preserved; a name collision is an error. order_effect =
"reset" (a source defines fresh per-resource row order). ``apply``-only: there is
no input resource to transform, so ``update_schema``/``process_rows`` are inert
pass-throughs (present only for contract completeness).

SINGLE SOURCE OF TRUTH
----------------------
We do NOT reimplement tabulator, the custom parsers (bcodmo-fixedwidth,
bcodmo-regex-csv), the custom loaders (bcodmo-aws), Excel sheet handling, S3 glob
expansion, multi-file naming, ``use_filename``, or ``remove_empty_rows``. We run
the LIVE ``bcodmo_pipeline_processors.load.flow`` verbatim (the same structural-
driver pattern as join/unpivot/concatenate) and ingest whatever resources it
yields. The dataflows lane runs the same ``load.flow`` -- so the two lanes cannot
diverge on parsing/inference by construction; the only thing the DuckDB lane adds
is the ingest->dump round-trip of those exact VARCHAR bytes.

NEVER-OOM
---------
We consume the flow LAZILY via ``Flow(...).datastream()`` and hand each resource's
row iterator straight to ``Engine.ingest_iter``, which appends in bounded batches
and spills to disk. A resource is streamed one row-batch at a time; the whole file
is never held in Python memory. (dataflows streams resources in order, so each is
fully consumed before the next is pulled -- which is exactly how ingest wants it.)
"""

import copy
import importlib

from dataflows import Flow

from ..processor import Processor, register

_bcodmo_load = importlib.import_module(
    "bcodmo_frictionless.bcodmo_pipeline_processors.load"
)


@register
class Load(Processor):
    name = "bcodmo_pipeline_processors.load"
    order_effect = "reset"  # a source defines fresh row order per resource

    def update_schema(self, schema, params):
        # Not used: load is a source (no input resource). The real schema comes
        # from the live load flow's inferred descriptor. Present for contract
        # completeness (governance gate).
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        # Not used: there are no input rows to transform. Present for contract
        # completeness (governance gate).
        return iter(rows)

    def apply(self, engine, params):
        # load.flow POPS `from`/`name`/... and mutates its params dict; the engine
        # hands us the live step params, so copy defensively before the live flow
        # can chew on them.
        flow = Flow(_bcodmo_load.flow(copy.deepcopy(params)))
        ds = flow.datastream()
        for rw in ds.res_iter:
            name = rw.res.name
            if name in engine.resources:
                raise Exception(
                    f"load: resource {name!r} already exists in the package"
                )
            descriptor = copy.deepcopy(rw.res.descriptor)
            schema = descriptor.get("schema", {}).get("fields", [])
            # rw is a lazy row iterator -> streamed straight into the batched,
            # disk-spilling ingest. Nothing materializes the full resource. The
            # full descriptor is carried so a terminal dump can reproduce a
            # byte-identical datapackage.json.
            engine.ingest_iter(name, rw, schema, descriptor=descriptor)
