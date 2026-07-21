"""
Processor: bcodmo_pipeline_processors.convert_date.

SEMANTICS
---------
Behavior: for each entry in ``fields``, parse one or more input columns into a
temporal value and write it to ``output_field`` as a ``datetime``/``date``/``time``
(or a formatted ``string``). Supports python strptime parsing (single
``input_field``/``input_format`` or multiple ``inputs``), excel/matlab serial
numbers, and decimalDay/decimalYear; plus input/output timezones, a fixed
``year``, and a ``boolean_statement`` gate. Adds or replaces fields; drops no rows;
row order preserved (order_effect = "keep").

Params:
  * ``fields``: list of field configs (``output_field``, ``output_format``,
    ``output_type``, ``input_field``/``inputs``, ``input_type``, timezones, ...).
  * ``boolean_statement``: only rows passing it are converted (others get the
    passthrough/None behavior of the live processor).
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: an output_type other than
datetime/date/time/string raises; ``%Z`` in the output format without input tz
raises; a missing input value (in ``missingValues``) sets the whole output to that
missing value. An input field that does not exist raises (validated in
``update_schema``, mirroring the live ``convert_date`` flow).

TIER: UDF-only (no ``to_sql``). The logic (timezone math via pytz/dateutil, Decimal
decimalYear arithmetic, excel/matlab serial conversion, strptime) is not
reproducible byte-exactly in DuckDB SQL, so the DuckDB lane always runs the live
``process_resource`` as a UDF -- the SAME code the dataflows lane runs. Correctness
is pinned END TO END by the byte-identical differential test.

Temporal round-trip: the output field carries only ``outputFormat`` (like the live
schema), and the shared ``casting`` module's storage-format invariant
(``_storage_format``) round-trips the DuckDB VARCHAR storage through that
``outputFormat`` -- so format-out and cast-in stay inverses even though there is no
cast ``format``. See casting.py and PLAN.md §4.2.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.convert_date import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


class _FieldShim:
    """Minimal stand-in for a frictionless ``Field`` (``.name`` + ``.descriptor``)
    as the live ``process_resource`` expects in ``datapackage_fields``. Only used
    by the ``inputs`` branch when an input value is already a temporal object, to
    read that input field's ``outputFormat``."""

    __slots__ = ("name", "descriptor")

    def __init__(self, d):
        self.name = d["name"]
        self.descriptor = d


def _build_schema(fields, package_fields):
    """The one place convert_date's schema transform lives (mirrors the live
    ``convert_date`` flow): build each output field, then replace an existing field
    of the same name in place (preserving order) or append it at the end."""
    package_fields = [dict(f) for f in package_fields]
    package_fields_lookup = {f["name"]: f for f in package_fields}

    new_field_names = [f["output_field"] for f in fields]
    new_fields_dict = {}
    for field_config in fields:
        output_field = field_config["output_field"]
        if field_config.get("output_type") != "string":
            new_field = {
                "name": output_field,
                "type": field_config.get("output_type", "datetime"),
                "outputFormat": field_config["output_format"],
            }
        else:
            new_field = {"name": output_field, "type": "string"}

        if field_config.get("preserve_metadata", False):
            original_field_name = None
            if "input_field" in field_config:
                original_field_name = field_config["input_field"]
            elif "inputs" in field_config and len(field_config["inputs"]) > 0:
                original_field_name = field_config["inputs"][0]["field"]
            if original_field_name and original_field_name in package_fields_lookup:
                orig_field = package_fields_lookup[original_field_name]
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
        if "inputs" in field_config:
            for input_d in field_config["inputs"]:
                if input_d["field"] not in names:
                    raise Exception(
                        f'Field "{input_d["field"]}" not found. '
                        f"Available fields: {sorted(names)}"
                    )
        elif "input_field" in field_config:
            if field_config["input_field"] not in names:
                raise Exception(
                    f'Field "{field_config["input_field"]}" not found. '
                    f"Available fields: {sorted(names)}"
                )


@register
class ConvertDate(Processor):
    name = "bcodmo_pipeline_processors.convert_date"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        _validate_inputs(fields, schema)
        return _build_schema(fields, schema)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live convert_date row logic. In the DuckDB lane the
        # rows arrive already cast to the INPUT types (udf_map cast-in), exactly
        # as the dataflows lane feeds them; datapackage_fields lets the live code
        # re-serialize a temporal input via its outputFormat.
        fields = params.get("fields", [])
        boolean_statement = params.get("boolean_statement")
        # DuckDB lane default missingValues (mirrors the frictionless resource
        # default used by get_missing_values); resource-level missingValues are
        # not yet tracked in the VARCHAR schema.
        missing_values = [""]
        datapackage_fields = [_FieldShim(f) for f in (schema or [])]
        return _df_process_resource(
            rows,
            fields,
            missing_values,
            datapackage_fields,
            boolean_statement=boolean_statement,
        )
