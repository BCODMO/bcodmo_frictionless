"""
Processor: bcodmo_pipeline_processors.split_column.

SEMANTICS
---------
Behavior: for each entry in ``fields``, take one ``input_field`` and split its
(string) value into several ``output_fields`` -- either by a regex ``pattern``
(each capture group -> one output field) or by a regex ``delimiter`` (``re.split``
-> one piece per output field). The number of groups/pieces MUST equal the number
of ``output_fields`` (else the live processor raises). Each output field is created
as ``type: "string"``. Rows are neither added nor dropped and order is preserved
(1:1 rows, order_effect = "keep").

Params:
  * ``fields``: list of ``{input_field, output_fields, pattern|delimiter,
    preserve_metadata?}``.
  * ``delete_input``: when True, drop each ``input_field`` from the schema/row
    UNLESS that input field name is itself reused as one of the output fields.
  * ``boolean_statement``: only rows passing it are split; failing rows get None
    for every output field (the live passthrough behavior).
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: a missing/empty input value (in
``missingValues``, i.e. None after cast-in) sets every output field to that value
(None); a ``pattern``/``delimiter`` yielding a different number of matches than
``output_fields`` raises; neither ``pattern`` nor ``delimiter`` raises. An input
field that does not exist raises (validated in ``update_schema``, mirroring the
live ``split_column`` flow).

TIER: UDF-only (no ``to_sql``). The row logic is arbitrary Python regex
(``re.search`` group extraction / ``re.split``) plus a boolean-statement gate and
missing-value passthrough -- not reproducible byte-exactly in DuckDB SQL. So the
DuckDB lane always runs the live ``process_resource`` as a UDF -- the SAME code the
dataflows lane runs. Correctness is pinned END TO END by the byte-identical
differential test. All output fields are plain strings, so there is no temporal /
numeric round-trip to worry about.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.split_column import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


def _validate_inputs(fields, package_fields):
    """Mirror the live flow's input-existence validation (so both lanes fail the
    same way on a bad spec)."""
    names = {f["name"] for f in package_fields}
    for field in fields:
        input_field = field.get("input_field")
        if input_field and input_field not in names:
            raise Exception(
                f'Field "{input_field}" not found. '
                f"Available fields: {sorted(names)}"
            )


def _build_schema(fields, delete_input, package_fields):
    """The one place split_column's schema transform lives (mirrors the live
    ``split_column`` ``func(package)``): add each ``output_field`` as a string field
    (replacing an existing field of the same name in place to preserve order, else
    appending at the end), and -- when ``delete_input`` -- drop each ``input_field``
    that is not reused as an output field."""
    package_fields = [dict(f) for f in package_fields]

    output_fields = []
    input_fields = []
    for field in fields:
        output_fields += field.get("output_fields", [])
        input_fields.append(field.get("input_field"))

    package_fields_lookup = {f["name"]: f for f in package_fields}

    new_field_names = [f for f in output_fields]
    new_fields_dict = {}
    for field_config in fields:
        input_field = field_config.get("input_field")
        field_output_fields = field_config.get("output_fields", [])
        preserve_metadata = field_config.get("preserve_metadata", False)

        for output_field in field_output_fields:
            new_field = {
                "name": output_field,
                "type": "string",
            }

            if (
                preserve_metadata
                and input_field
                and input_field in package_fields_lookup
            ):
                orig_field = package_fields_lookup[input_field]
                if "bcodmo:" in orig_field:
                    new_field["bcodmo:"] = orig_field["bcodmo:"]

            new_fields_dict[output_field] = new_field

    processed_fields = []
    for f in package_fields:
        if (
            delete_input
            and f["name"] in input_fields
            and f["name"] not in output_fields
        ):
            continue
        if f["name"] in new_field_names:
            processed_fields.append(new_fields_dict[f["name"]])
            new_field_names.remove(f["name"])
        else:
            processed_fields.append(f)
    for fname in new_field_names:
        processed_fields.append(new_fields_dict[fname])
    return processed_fields


@register
class SplitColumn(Processor):
    name = "bcodmo_pipeline_processors.split_column"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        delete_input = params.get("delete_input", False)
        _validate_inputs(fields, schema)
        return _build_schema(fields, delete_input, schema)

    def process_rows(self, rows, params, schema=None):
        # THE source of truth: the live split_column row logic, unchanged. In the
        # DuckDB lane rows arrive already cast to the INPUT types (udf_map cast-in),
        # exactly as the dataflows lane feeds them (so an empty "" input is None in
        # both lanes -> the missing-value passthrough matches).
        fields = params.get("fields", [])
        delete_input = params.get("delete_input", False)
        boolean_statement = params.get("boolean_statement")
        # DuckDB lane default missingValues (mirrors the frictionless resource
        # default used by the live get_missing_values); resource-level missingValues
        # are not yet tracked in the VARCHAR schema.
        missing_values = [""]
        return _df_process_resource(
            rows,
            fields,
            missing_values,
            delete_input=delete_input,
            boolean_statement=boolean_statement,
        )
