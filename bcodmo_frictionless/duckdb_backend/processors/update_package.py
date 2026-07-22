"""
Processor: update_package (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Merges its parameters (minus any ``resources`` key) into the PACKAGE-level
frictionless descriptor -- ``title``, ``name``, and any custom top-level keys.
Resources, rows, and schemas are untouched (order_effect = "keep"); the effect is
visible only in the dumped ``datapackage.json`` top level.

Mirrors ``standard_flows.update_package`` = ``Flow(update_package(**parameters))``,
whose live ``dataflows.update_package`` deep-copies the metadata, drops
``resources``, and does ``package.descriptor.update(metadata)``. We keep the merged
metadata on ``engine.package_descriptor``; ``dump_to_s3`` folds it into the package
descriptor it hands the live ``S3Dumper``, so the top-level keys come out
byte-identical to the dataflows lane.

TIER: structural / package-metadata. ``apply``-only (no rows/schema change), so
``update_schema``/``process_rows`` are inert (contract completeness).
"""

import copy

from ..processor import Processor, register


@register
class UpdatePackage(Processor):
    name = "update_package"
    order_effect = "keep"

    def update_schema(self, schema, params):
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        metadata = copy.deepcopy(params)
        metadata.pop("resources", None)  # live update_package drops this key
        engine.package_descriptor.update(metadata)
