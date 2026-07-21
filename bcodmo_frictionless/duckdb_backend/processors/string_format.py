"""
Processor: bcodmo_pipeline_processors.string_format.

SEMANTICS
---------
Behavior: for each entry in ``fields``, build ``output_field`` by applying a
python ``str.format`` template (``input_string``) to the values of one or more
``input_fields`` in order, i.e. ``input_string.format(*[row[f] for f in
input_fields])``. Adds a new field or overwrites an existing one (of the same
name); drops no rows; row order preserved (order_effect = "keep"). Every output
field is type ``string``.

Params:
  * ``fields``: list of ``{output_field, input_string, input_fields}`` configs.
    ``output_field``, ``input_string`` and a non-empty ``input_fields`` are all
    required (the live processor raises otherwise).
  * ``boolean_statement``: only rows passing it are formatted. A row that does
    NOT pass keeps an existing ``output_field`` value unchanged, or gets ``None``
    if the field is new (the live passthrough behavior).
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor exactly: if ANY of a field's
``input_fields`` holds a value in ``missingValues`` (or ``None``), the ENTIRE
``output_field`` for that row is set to the passthrough/None value (per the data
managers' convention) rather than formatted. A ``ValueError`` from ``.format``
(e.g. a numeric format spec applied to an incompatible value) is re-raised with
the offending row number. An ``input_field`` absent from the row raises; an
``input_field`` absent from the schema is rejected in ``update_schema`` (mirroring
the live ``string_format`` flow's validation).

TIER: UDF-only (no ``to_sql``). The row logic is python's ``str.format`` mini
mini-language over already-cast values (a missing/None input in any position
nullifies the whole output). Reproducing ``str.format`` -- format specs, the
"any missing input nullifies the row" rule, and the exact Decimal/temporal
``str`` forms the cast-in values carry -- byte-for-byte in DuckDB SQL is not
practical, so the DuckDB lane always runs the live ``process_resource`` as a UDF
(the SAME code the dataflows lane runs). Correctness is pinned END TO END by the
byte-identical differential test.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.string_format import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


def _build_schema(fields, package_fields):
    """The one place string_format's schema transform lives (mirrors the live
    ``string_format`` flow's ``func(package)``): each ``output_field`` becomes a
    ``{"name", "type": "string"}`` field, replacing an existing field of the same
    name in place (preserving order) or appended at the end."""
    new_field_names = [f["output_field"] for f in fields]
    new_fields_dict = {
        f["output_field"]: {"name": f["output_field"], "type": "string"}
        for f in fields
    }

    processed_fields = []
    remaining = list(new_field_names)
    for f in package_fields:
        if f["name"] in remaining:
            processed_fields.append(new_fields_dict[f["name"]])
            remaining.remove(f["name"])
        else:
            processed_fields.append(dict(f))
    for fname in remaining:
        processed_fields.append(new_fields_dict[fname])
    return processed_fields


def _validate_inputs(fields, package_fields):
    """Mirror the live flow's input-existence validation (so both lanes fail the
    same way on a bad spec)."""
    names = {f["name"] for f in package_fields}
    for field_config in fields:
        for input_field in field_config.get("input_fields", []):
            if input_field not in names:
                raise Exception(
                    f'Field "{input_field}" not found. '
                    f"Available fields: {sorted(names)}"
                )


@register
class StringFormat(Processor):
    name = "bcodmo_pipeline_processors.string_format"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        _validate_inputs(fields, schema)
        return _build_schema(fields, schema)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live string_format row logic. In the DuckDB lane
        # rows arrive already cast to the INPUT types (udf_map cast-in), exactly
        # as the dataflows lane feeds them.
        fields = params.get("fields", [])
        boolean_statement = params.get("boolean_statement")
        # DuckDB lane default missingValues (mirrors the frictionless resource
        # default used by get_missing_values); resource-level missingValues are
        # not yet tracked in the VARCHAR schema.
        missing_values = [""]
        return _df_process_resource(
            rows,
            fields,
            missing_values,
            boolean_statement=boolean_statement,
        )
