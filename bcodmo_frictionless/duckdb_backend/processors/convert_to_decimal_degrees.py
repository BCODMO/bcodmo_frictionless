"""
Processor: bcodmo_pipeline_processors.convert_to_decimal_degrees.

SEMANTICS
---------
Behavior: for each entry in ``fields``, read one STRING coordinate column
(``input_field``), parse it with the field's ``pattern`` (a regex carrying named
groups ``degrees`` + either ``decimal_minutes`` or ``minutes``/``seconds``, plus
an optional ``directional``) according to ``format``
(``degrees-decimal_minutes`` or ``degrees-minutes-seconds``), and write the
resulting decimal degrees to ``output_field`` as a ``number`` (Decimal). A fixed
``directional`` (``"N"``/``"S"``/``"E"``/``"W"``) may override the matched group;
``handle_out_of_bounds`` wraps seconds/minutes/degrees that spill their range.
Adds or replaces fields (an output field of the same name is replaced in place,
preserving order); drops no rows; row order preserved (order_effect = "keep").

Params:
  * ``fields``: list of field configs (``input_field``, ``output_field``,
    ``pattern``, ``format``, optional ``directional``, ``handle_out_of_bounds``,
    ``preserve_metadata``).
  * ``boolean_statement``: only rows passing it are converted; other rows get the
    passthrough/None behavior of the live processor (existing output kept, else
    None).
  * ``resources``: which resources to apply to.

INPUT TYPE: the coordinate ``input_field`` is a STRING (e.g. ``"14° 24.0' N"``) --
the live row logic does ``row_value = str(row_value)`` and regex-parses it -- so
there is NO ``set_types`` -> number step before this processor. The OUTPUT field
is a ``number`` (its schema type, mirroring the live ``func``).

Edge cases mirror the live processor: a missing input value (in ``missingValues``,
default ``[""]``) is copied through unchanged; a non-matching pattern, a missing
required regex group, seconds/minutes/degrees out of bounds without
``handle_out_of_bounds``, or a non-existent ``input_field`` all raise (input
existence is validated in ``update_schema``, mirroring the live flow).

TIER: UDF-only (no ``to_sql``). The conversion is ``Decimal`` coordinate
arithmetic (minutes/seconds division, directional sign, 360-wrapping) that is not
reproducible byte-exactly in DuckDB SQL, so the DuckDB lane always runs the live
``process_resource`` as a UDF -- the SAME code the dataflows lane runs.
Correctness is pinned END TO END by the byte-identical differential test.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.convert_to_decimal_degrees import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


def _build_schema(fields, package_fields):
    """The one place convert_to_decimal_degrees's schema transform lives (mirrors
    the live ``func(package)``): each output field becomes a ``number`` (optionally
    carrying the input field's ``bcodmo:`` metadata), then replaces an existing
    field of the same name in place (preserving order) or is appended at the end."""
    package_fields = [dict(f) for f in package_fields]
    package_fields_lookup = {f["name"]: f for f in package_fields}

    new_field_names = [f["output_field"] for f in fields]
    new_fields_dict = {}
    for field_config in fields:
        output_field = field_config["output_field"]
        input_field = field_config.get("input_field")

        new_field = {
            "name": output_field,
            "type": "number",
        }

        if (
            field_config.get("preserve_metadata", False)
            and input_field
            and input_field in package_fields_lookup
        ):
            orig_field = package_fields_lookup[input_field]
            if "bcodmo:" in orig_field:
                new_field["bcodmo:"] = orig_field["bcodmo:"]

        new_fields_dict[output_field] = new_field

    processed_fields = []
    remaining = list(new_field_names)
    for f in package_fields:
        if f["name"] in remaining:
            processed_fields.append(new_fields_dict[f["name"]])
            remaining.remove(f["name"])
        else:
            processed_fields.append(f)
    for fname in remaining:
        processed_fields.append(new_fields_dict[fname])
    return processed_fields


def _validate_inputs(fields, package_fields):
    """Mirror the live flow's input-existence validation (so both lanes fail the
    same way on a bad spec)."""
    names = {f["name"] for f in package_fields}
    for field_config in fields:
        input_field = field_config.get("input_field")
        if input_field and input_field not in names:
            raise Exception(
                f'Field "{input_field}" not found. '
                f"Available fields: {sorted(names)}"
            )


@register
class ConvertToDecimalDegrees(Processor):
    name = "bcodmo_pipeline_processors.convert_to_decimal_degrees"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        _validate_inputs(fields, schema)
        return _build_schema(fields, schema)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live convert_to_decimal_degrees row logic. The
        # coordinate input arrives as a VARCHAR string (cast-in leaves the input
        # field a string), exactly as the dataflows lane feeds it.
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
