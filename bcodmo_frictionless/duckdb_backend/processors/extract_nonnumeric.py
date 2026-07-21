"""
Processor: bcodmo_pipeline_processors.extract_nonnumeric.

SEMANTICS
---------
Behavior: for each field name in ``fields``, ADD a new sibling field named
``f"{field}{suffix}"`` (``suffix`` defaults to ``"_"``) of type ``string``, placed
immediately AFTER its source field in the schema. Per row, if the source value is
numeric (``float(value)`` succeeds, or the value is missing/None) the new field is
set to ``""`` and the source value is left untouched; if the source value is
NON-numeric (``float`` raises ``ValueError``) the non-numeric text is MOVED into the
new field and the source field is set to ``None``. This "extracts" stray
non-numeric junk out of an otherwise-numeric column. Rows are neither dropped nor
reordered (1:1, order_effect = "keep"); a ``boolean_statement`` gate can restrict
which rows are processed (rows that don't pass just get ``""`` in the new field).

Params:
  * ``fields``: list of source field names to scan.
  * ``suffix``: appended to each source name to form the new field name (default
    ``"_"``).
  * ``boolean_statement``: only rows passing it are examined; non-passing rows get
    ``""`` in the new field and keep their source value.
  * ``preserve_metadata``: when true, copy the source field's ``bcodmo:`` metadata
    onto the new field.
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: a ``field`` not present in the schema raises;
a new field name that already exists in the schema raises; a missing/empty source
value (in ``missingValues``) is treated as numeric (new field ``""``, source kept).

TIER: UDF-only (no ``to_sql``). The non-numeric detection is Python ``float()``
parsing semantics (and the move/blank rewrite), which is not reproducible
byte-exactly in DuckDB SQL, so the DuckDB lane always runs the live
``process_resource`` as a UDF -- the SAME code the dataflows lane runs. Correctness
is pinned END TO END by the byte-identical differential test.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.extract_nonnumeric import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


def _build_schema(schema, fields, suffix, preserve_metadata):
    """The one place extract_nonnumeric's schema transform lives (mirrors the live
    ``extract_nonnumeric`` flow): validate each requested field exists, then walk
    the field list and insert a new string field ``f"{name}{suffix}"`` right after
    each matched source field."""
    package_fields = [dict(f) for f in schema]
    package_field_names = [f["name"] for f in package_fields]
    package_field_names_set = set(package_field_names)

    for field in fields:
        if field not in package_field_names_set:
            raise Exception(
                f'Field "{field}" not found. '
                f"Available fields: {sorted(package_field_names_set)}"
            )

    package_fields_lookup = {f["name"]: f for f in package_fields}

    new_fields = []
    for field in package_fields:
        new_fields.append(field)
        if field.get("name", None) in fields:
            new_field_name = f'{field.get("name")}{suffix}'
            if new_field_name in package_field_names:
                raise Exception(
                    f'The new field "{new_field_name}" already exists in the datapackage.'
                )

            new_field = {
                "name": new_field_name,
                "type": "string",
            }

            if preserve_metadata:
                original_field_name = field.get("name")
                if original_field_name and original_field_name in package_fields_lookup:
                    orig_field = package_fields_lookup[original_field_name]
                    if "bcodmo:" in orig_field:
                        new_field["bcodmo:"] = orig_field["bcodmo:"]

            new_fields.append(new_field)

    return new_fields


@register
class ExtractNonnumeric(Processor):
    name = "bcodmo_pipeline_processors.extract_nonnumeric"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        suffix = params.get("suffix", "_")
        preserve_metadata = params.get("preserve_metadata", False)
        return _build_schema(schema, fields, suffix, preserve_metadata)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live extract_nonnumeric row logic. Rows arrive
        # already cast to the INPUT types (udf_map cast-in), exactly as the
        # dataflows lane feeds them. DuckDB lane default missingValues (mirrors the
        # frictionless resource default used by get_missing_values); resource-level
        # missingValues are not yet tracked in the VARCHAR schema.
        return _df_process_resource(
            rows,
            params.get("fields", []),
            [""],
            suffix=params.get("suffix", "_"),
            boolean_statement=params.get("boolean_statement"),
        )
