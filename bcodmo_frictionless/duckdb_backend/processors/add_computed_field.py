"""
Processor: add_computed_field (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Appends one or more computed columns. Each ``fields`` entry has an ``operation``
(sum/avg/max/min/multiply over ``source`` columns, or ``constant``/``join``/
``format`` using ``with``), and a ``target`` (name or ``{name, type}``). Existing
columns and rows pass through unchanged; row count and order preserved
(order_effect = "keep"). The new field's type is inferred by the live
``get_type`` (number for avg / any-number source, string for format/join, else
the first source type).

Params:
  * ``fields``: ``[{operation, source?, target, with?}]``
  * ``resources``: which resources to apply to.

TIER: leaf / 1:1 row map. Uses the DEFAULT ``apply`` (``update_schema`` +
``process_rows`` via the UDF path). ROW logic is the LIVE
``add_computed_field.process_resource`` reused verbatim; the new schema fields are
the LIVE ``get_new_fields``. The aggregators do type-dependent arithmetic (sum/avg/
...), so the UDF path casts VARCHAR->typed before ``process_rows`` and formats back
after -- through the shared ``casting`` module -- exactly as every numeric leaf
processor does.

NOTE: ``get_new_fields``/``process_resource`` mutate the ``fields`` param
(``target`` str -> dict); we deep-copy per call so a step's params are not
corrupted across the two lanes.
"""

import copy

from dataflows.processors.add_computed_field import (
    get_new_fields as _live_get_new_fields,
    process_resource as _live_process_resource,
)

from ..processor import Processor, register


def _normalized_fields(params):
    """The live func accepts ``fields`` as the first positional arg (or, with no
    positional, the kwargs as a single field). standard_flows always passes the
    list form, so we mirror that: ``params['fields']`` is the list."""
    return copy.deepcopy(params.get("fields", []))


@register
class AddComputedField(Processor):
    name = "add_computed_field"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = _normalized_fields(params)
        # get_new_fields reads resource['schema']['fields'] to infer types.
        resource = {"schema": {"fields": [dict(f) for f in schema]}}
        new_fields = _live_get_new_fields(resource, fields)
        return [dict(f) for f in schema] + new_fields

    def process_rows(self, rows, params, schema=None):
        fields = _normalized_fields(params)
        # The live func normalizes a str target to {name: ...} before calling
        # process_resource; process_resource itself reads target['name'].
        for f in fields:
            if isinstance(f.get("target"), str):
                f["target"] = {"name": f["target"]}
        return _live_process_resource(fields, iter(rows))
