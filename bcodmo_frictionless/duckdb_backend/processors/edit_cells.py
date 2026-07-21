"""
Processor: bcodmo_pipeline_processors.edit_cells.

SEMANTICS
---------
Behavior: overwrite individual cells by (row-number, field). ``edited`` maps a
1-based row number (int OR the same number as a str -- both are accepted) to a
list of ``{"field": <name>, "value": <new value>}`` edits. For each matched row
the listed fields are replaced with the new string values; every other cell and
every other row passes through unchanged. Adds/removes no fields, drops/adds no
rows, preserves row order (order_effect = "keep").

Params:
  * ``edited``: ``{row_number: [{"field": ..., "value": ...}, ...], ...}`` where
    ``row_number`` counts data rows from 1 and may be an int or a str.
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor:
  * A ``field`` that is not present in a targeted row raises
    ``field given to edit_cells processor not found in row: '<field>'`` (the live
    flow ALSO validates field existence up front against the schema; this is
    mirrored in ``update_schema``).
  * Any row number in ``edited`` that never matches a data row raises
    ``Passed in row numbers that were not used (...)`` -- because the live code
    ``pop``s each key as it is used and asserts the dict is empty at the end.
  * A missing/empty source cell is simply overwritten like any other cell; the
    new value is stored verbatim as a string.

TIER: UDF-only (no ``to_sql``). Row numbering is the live processor's own 1-based
counter over the row stream; reproducing that (plus the pop/leftover-key
validation) in SQL buys nothing and risks divergence, so the DuckDB lane always
runs the live ``process_resource`` as a UDF -- the SAME code the dataflows lane
runs. Correctness is pinned END TO END by the byte-identical differential test.

Live quirk -- ``edited`` is CONSUMED: the live ``process_resource`` mutates the
dict via ``.pop()`` (and asserts it is empty at the end). We therefore delegate on
a deep copy so the caller's ``params["edited"]`` is never drained (the dataflows
lane runs the live flow, which DOES drain its own copy -- see the test, which
hands each lane an independent ``edited``).
"""

import copy

from bcodmo_frictionless.bcodmo_pipeline_processors.edit_cells import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


@register
class EditCells(Processor):
    name = "bcodmo_pipeline_processors.edit_cells"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # edit_cells never changes the schema (it only overwrites cell values).
        # It DOES validate up front that every referenced field exists, exactly
        # as the live flow's func(package) does -- so both lanes fail identically
        # on a bad spec.
        edited = params.get("edited", {})
        field_names = {f["name"] for f in schema}
        for _row_num, cells in edited.items():
            for cell in cells:
                field = cell.get("field")
                if field and field not in field_names:
                    raise Exception(
                        f'Field "{field}" not found. '
                        f"Available fields: {sorted(field_names)}"
                    )
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live edit_cells row logic. It consumes ``edited``
        # via .pop(), so hand it a deep copy to leave params intact.
        edited = copy.deepcopy(params.get("edited", {}))
        missing_values = [""]
        return _df_process_resource(rows, edited, missing_values)
