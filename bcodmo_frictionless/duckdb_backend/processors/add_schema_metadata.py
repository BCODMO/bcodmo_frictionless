"""
Processor: bcodmo_pipeline_processors.add_schema_metadata.

SEMANTICS
---------
Behavior: merges the step parameters into the resource's SCHEMA-LEVEL descriptor
(``resource["schema"].update(metadata)`` in the live processor). This adds
schema/resource metadata (e.g. descriptive keys, ``bcodmo:`` blocks) for the output
datapackage; it does NOT change the field list, field types, row values, or CSV
bytes. Rows pass through unchanged (order_effect = "keep").

Params: the whole step ``parameters`` dict is the metadata merged in (the live
``flow`` passes ``parameters`` as ``metadata``); ``resources`` selects targets.

TIER: schema-only. The Phase-0 DuckDB schema is a FIELD LIST and has no place to
store resource/schema-level metadata, so this is a NO-OP on the tracked schema and
on the data. That is safe for CSV output (schema-level metadata never affects the
CSV bytes). Registering it means a pipeline using add_schema_metadata runs rather
than erroring on an unknown ``run:``.

KNOWN GAP: resource/schema-level metadata is not yet carried by the DuckDB lane;
it must be reintroduced when this backend emits ``datapackage.json``. A ``metadata``
key that alters casting (e.g. ``missingValues``) is also not tracked (the lane uses
the default ``[""]``); flag such usage when datapackage/missingValues support lands.
"""

from ..processor import Processor, register


@register
class AddSchemaMetadata(Processor):
    name = "bcodmo_pipeline_processors.add_schema_metadata"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # Resource/schema-level metadata is not represented in the field-list
        # schema and does not affect field names/types, row values, or CSV bytes.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        # No-op on data; keep the field list as-is (see module docstring).
        for name in engine.matched(params):
            st = engine.resources[name]
            engine.resources[name] = engine.state(
                name, st.relation, self.update_schema(st.schema, params)
            )
