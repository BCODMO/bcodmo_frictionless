"""
Processor: bcodmo_pipeline_processors.round_fields.

SEMANTICS
---------
Behavior: for each entry in ``fields``, round the (already numeric) value of the
named column to ``digits`` decimal places. Rounding uses Python ``Decimal`` +
``round`` (banker's rounding), exactly as the live processor. Does not add or
remove columns; drops no rows; row order preserved (order_effect = "keep").

Params:
  * ``fields``: list of ``{name, digits, ...}`` configs. Per-field flags mirror
    the live processor:
      - ``digits``: number of decimal places to round to (required).
      - ``preserve_trailing_zeros`` (default False): keep the rounded Decimal's
        trailing zeros; otherwise ``remove_trailing_zeros`` normalizes it.
      - ``maximum_precision`` (default False): only round rows whose current
        precision is >= ``digits`` (rows with less precision pass through
        untouched).
      - ``convert_to_integer`` (default False): only valid with ``digits == 0``;
        casts the rounded value to ``int`` AND retypes the field number->integer
        in the schema.
  * ``boolean_statement``: only rows passing it are rounded (others pass through
    unchanged), via the shared bcodmo boolean DSL.
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: a missing value (in ``missingValues``, here
``[""]``) or ``None`` passes through unchanged; rounding a field that is not typed
``number`` (or ``integer`` for the convert_to_integer path) raises. ``digits`` is
read without a default, so a field lacking it raises -- matching the live flow.

Schema mutation: round_fields normally leaves field names and types UNCHANGED. The
ONE exception (mirrored in ``update_schema``) is the ``convert_to_integer`` +
``digits == 0`` case, where the live ``round_fields`` flow retypes that field from
``number`` to ``integer`` on the datapackage (and raises if it was not a number).

TIER: UDF-only (no ``to_sql``). Decimal rounding, ``remove_trailing_zeros``
normalization and the per-value Decimal string representation are not reproducible
byte-exactly by DuckDB's DOUBLE/DECIMAL columnar types, so the DuckDB lane always
runs the live ``process_resource`` as a UDF -- the SAME code the dataflows lane
runs. Correctness is pinned END TO END by the byte-identical differential test.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.round_fields import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


class _ResShim:
    """Minimal stand-in for a dataflows resource: the live ``process_resource``
    reads ``rows.res.descriptor["schema"]`` to look up each field's declared type.
    In the DuckDB lane the rows arrive as a plain list, so we expose the INPUT
    frictionless schema here."""

    __slots__ = ("descriptor",)

    def __init__(self, schema):
        self.descriptor = {"schema": {"fields": schema}}


class _RowsShim:
    """Iterable wrapper carrying a ``.res`` so the live ``process_resource`` (which
    is written against a dataflows ResourceWrapper) can run unchanged over the
    DuckDB lane's materialized rows."""

    __slots__ = ("_rows", "res")

    def __init__(self, rows, schema):
        self._rows = rows
        self.res = _ResShim(schema)

    def __iter__(self):
        return iter(self._rows)


@register
class RoundFields(Processor):
    name = "bcodmo_pipeline_processors.round_fields"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # The one place round_fields' schema transform lives; mirrors the live
        # ``round_fields`` flow exactly. Names/types are unchanged EXCEPT that a
        # ``convert_to_integer`` + ``digits == 0`` field is retyped number->integer
        # (and a non-number field there raises, same message as the live flow).
        fields = params.get("fields", [])
        new_fields = [dict(f) for f in schema]
        names = {f["name"] for f in new_fields}
        for field in fields:
            if field["name"] not in names:
                raise Exception(
                    f'Field "{field["name"]}" not found. '
                    f"Available fields: {sorted(names)}"
                )
        for field in fields:
            if field.get("convert_to_integer", False) and field["digits"] == 0:
                for pf in new_fields:
                    if pf["name"] == field["name"]:
                        if pf["type"] != "number":
                            raise Exception(
                                f'Tried to round a field ("{field["name"]}") that has '
                                f'not been cast to a number: {field.get("type")}. Make '
                                f"sure all fields used with the round_fields processor "
                                f"are numbers."
                            )
                        pf["type"] = "integer"
        return new_fields

    def process_rows(self, rows, params, schema=None):
        # THE source of truth: the live round_fields per-row logic, unchanged.
        # Rows arrive already cast to the INPUT types (udf_map cast-in), exactly as
        # the dataflows lane feeds them; the shim exposes the input schema the live
        # code reads for each field's declared type. DuckDB-lane default
        # missingValues is [""] (resource-level missingValues aren't tracked yet).
        fields = params.get("fields", [])
        boolean_statement = params.get("boolean_statement")
        missing_values = [""]
        shim = _RowsShim(rows, schema or [])
        return _df_process_resource(
            shim, fields, missing_values, boolean_statement=boolean_statement
        )
