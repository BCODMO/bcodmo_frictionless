"""
Processor: bcodmo_pipeline_processors.dump_to_s3 -- the terminal SINK.

Writes each resource to S3 as a byte-identical CSV plus a byte-identical
``datapackage.json`` (bytes, hashes/etags, row counts, dialect, temporal formats,
``bcodmo:`` metadata). Like ``load``, this is not an N->N transform: it's an N->0
sink that emits S3 artifacts.

SEMANTICS
---------
For every resource in the engine: serialize its ORDERED, CAST rows with the
production ``CustomCSVFormat`` and upload to ``{prefix}/{name}.csv`` (single PUT
under 5MB, else multipart); then write ``{prefix}/datapackage.json`` describing
them (+ optional ``pipeline-spec.yaml`` / unique-lat-lon files). The engine's
resources are left unchanged (dump is terminal / pass-through), so order_effect =
"keep". ``apply``-only: there are no output rows to transform, so
``update_schema``/``process_rows`` are inert (contract completeness).

SINGLE SOURCE OF TRUTH
----------------------
We drive the LIVE ``S3Dumper`` verbatim (same structural-driver pattern as
join/concatenate/load): build an input ``DataStream`` from the engine's resources
-- each carrying the descriptor ``load`` produced (re-stamped with the live tracked
schema) and its typed+ordered rows -- set it as the dumper's ``source``, and call
``dumper.process()``. Every artifact (CSV via ``CustomCSVFormat``, the whole
``datapackage.json`` via ``DumperBase``, the multipart uploader, redis progress) is
the real production code, so the DuckDB lane's output cannot diverge from the
dataflows lane's -- there is nothing to keep in sync. Byte-identity is proven
end-to-end by the moto differential test.

NEVER-OOM
---------
``S3Dumper.rows_processor`` streams row-by-row, flushing multipart chunks once the
buffer exceeds the part size, so a huge resource never has to be held in memory.
We feed it ``engine.typed_rows_iter`` -- a LAZY, chunk-fetched, cast-on-the-fly
stream -- so nothing materializes the full resource on the DuckDB side either. The
whole load->dump path is memory-bounded (validated in tests/test_never_oom.py).
"""

import copy

from datapackage import Package
from dataflows.base.datastream import DataStream
from dataflows.base.resource_wrapper import ResourceWrapper

from bcodmo_frictionless.bcodmo_pipeline_processors.dump_to_s3 import S3Dumper

from ..processor import Processor, register


class _Source:
    """A minimal upstream for a ``DataStreamProcessor``: its ``_process`` just
    returns the prebuilt DataStream we want the dumper to consume."""

    def __init__(self, ds):
        self._ds = ds

    def _process(self):
        return self._ds


def _resource_descriptor(st):
    """The pre-dump frictionless descriptor for a resource: the load-time
    descriptor with its schema fields re-stamped from the live tracked schema
    (so set_types/rename/etc. are reflected), or a synthesized minimal one for a
    resource that never carried a descriptor (join/concatenate target, in-memory
    ingest)."""
    fields = [dict(f) for f in st.schema]
    if st.descriptor:
        d = copy.deepcopy(st.descriptor)
        d.setdefault("schema", {})["fields"] = fields
        return d
    return {
        "name": st.name,
        "path": f"{st.name}.csv",
        "profile": "tabular-data-resource",
        "format": "csv",
        "schema": {"fields": fields, "missingValues": [""]},
    }


@register
class DumpToS3(Processor):
    name = "bcodmo_pipeline_processors.dump_to_s3"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # Not used: dump is terminal. Present for contract completeness.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        names = list(engine.resources)

        # Build the input datapackage from the engine's resources, mirroring the
        # descriptors the dataflows lane would hand the dumper.
        descriptors = [_resource_descriptor(engine.resources[n]) for n in names]
        # Fold in any package-level metadata set by update_package (title/name/
        # custom keys); ``resources`` always comes from the per-resource state.
        dp = Package(descriptor={
            **copy.deepcopy(engine.package_descriptor),
            "resources": copy.deepcopy(descriptors),
        })
        # LAZY typed-row streams: S3Dumper.rows_processor consumes row-by-row and
        # flushes multipart chunks, and typed_rows_iter fetches from DuckDB in
        # bounded chunks -- so the whole dump stays memory-bounded (never-OOM).
        # Resources are drained sequentially by the dumper, so only one DuckDB
        # cursor is active at a time.
        res_iter = [
            ResourceWrapper(dp.resources[i], engine.typed_rows_iter(names[i]))
            for i in range(len(names))
        ]
        ds = DataStream(dp, res_iter, [])

        # Drive the LIVE S3Dumper exactly as bcodmo's dump_to_s3.flow does:
        # S3Dumper(bucket, prefix, **rest). Same options in => same artifacts out.
        params = copy.deepcopy(params)
        bucket = params.pop("bucket_name")
        prefix = params.pop("prefix")
        dumper = S3Dumper(bucket, prefix, **params)
        dumper.source = _Source(ds)
        dumper.process()
