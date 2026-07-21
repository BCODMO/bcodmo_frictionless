"""
Processor: bcodmo_pipeline_processors.convert_units.

SEMANTICS
---------
Behavior: for each entry in ``fields``, multiply a ``number`` column by a fixed
unit-conversion factor and write the (Decimal) result back. The ``conversion``
key selects the factor: ``feet_to_meter`` (x0.3048), ``fathom_to_meter``
(x1.8288), ``inch_to_cm`` (x2.54), ``mile_to_km`` (x1.60934); any other value
raises. When ``preserve_field`` is true the converted value is written to a NEW
field ``new_field_name`` (the original column is kept unchanged); otherwise the
column is converted in place. Adds or replaces fields; drops no rows; row order
preserved (order_effect = "keep").

Params:
  * ``fields``: list of field configs (``name``, ``conversion``, optional
    ``preserve_field`` + ``new_field_name``, optional ``preserve_metadata``).
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: a field whose schema type is not ``number``
raises (validated in ``update_schema`` and again in the row loop); an unknown
``conversion`` raises; ``preserve_field`` true whose ``new_field_name`` collides
with another existing field raises; and a ``new_field_name`` equal to the source
column converts in place (no new field). NOTE: the live ``process_resource`` has a
bug in its missing-value branch -- a missing/None value (an empty cell cast to
``number`` becomes None) references an unbound ``new_field_name`` and so RAISES
(``UnboundLocalError``) for every row that carries a missing value. Because the
DuckDB lane runs that SAME live code, it raises too; the byte-identical
differential test pins this end to end (see ``tests/test_convert_units.py``).

TIER: UDF-only (no ``to_sql``). The conversion is ``Decimal`` multiplication
(trailing-zero-preserving, e.g. ``Decimal('10') * Decimal('0.3048') ==
Decimal('3.0480')``) that is not reproducible byte-exactly in DuckDB SQL, so the
DuckDB lane always runs the live ``process_resource`` as a UDF -- the SAME code
the dataflows lane runs. Correctness is pinned END TO END by the byte-identical
differential test.
"""

import copy

from bcodmo_frictionless.bcodmo_pipeline_processors.convert_units import (
    process_resource as _df_process_resource,
)

from ..processor import Processor, register


class _ResShim:
    """Minimal stand-in for a frictionless resource wrapper carrying just the
    ``descriptor["schema"]`` the live ``process_resource`` reads to learn each
    field's declared type."""

    __slots__ = ("descriptor",)

    def __init__(self, schema):
        self.descriptor = {"schema": {"fields": schema or []}}


class _RowsShim:
    """Wrap the DuckDB lane's plain list of typed row dicts so the live
    ``convert_units`` ``process_resource`` (which reads ``rows.res.descriptor``)
    sees the same interface the dataflows lane feeds it: an iterable of rows plus
    a ``.res`` carrying the INPUT schema."""

    __slots__ = ("_rows", "res")

    def __init__(self, rows, schema):
        self._rows = rows
        self.res = _ResShim(schema)

    def __iter__(self):
        return iter(self._rows)


def _build_schema(fields, package_fields):
    """The one place convert_units's schema transform lives (mirrors the live
    ``func(package)`` exactly): each converted field either stays in place, or --
    when ``preserve_field`` is set with a distinct ``new_field_name`` -- has a
    renamed copy of its descriptor appended right after it (dropping ``bcodmo:``
    metadata unless ``preserve_metadata``)."""
    field_dict = {f["name"]: f for f in fields}

    package_field_names = {f["name"] for f in package_fields}
    for field in fields:
        if field["name"] not in package_field_names:
            raise Exception(
                f'Field "{field["name"]}" not found. '
                f"Available fields: {sorted(package_field_names)}"
            )

    new_package_fields = []
    previous_field_names = set([f["name"] for f in package_fields])
    for package_field in package_fields:
        new_package_fields.append(package_field)
        if package_field["name"] in field_dict:
            field = field_dict[package_field["name"]]
            if package_field["type"] != "number":
                raise Exception(
                    f'Tried to convert the units of a field ("{field["name"]}") that '
                    f'has not been cast to a number: {package_field["type"]}. Make '
                    f"sure all fields used with the convert_units processor are numbers."
                )
            if field.get("preserve_field", False):
                new_field_name = field["new_field_name"]
                if (
                    new_field_name in previous_field_names
                    and new_field_name != package_field["name"]
                ):
                    raise Exception(
                        f"In the convert_units processor, attempted to add a new field "
                        f'name that already exists somewhere else: "{new_field_name}".'
                    )
                elif new_field_name != package_field["name"]:
                    new_package_field = copy.deepcopy(package_field)
                    new_package_field["name"] = new_field_name

                    if field.get("preserve_metadata", False):
                        pass
                    else:
                        if "bcodmo:" in new_package_field:
                            del new_package_field["bcodmo:"]

                    new_package_fields.append(new_package_field)
            else:
                if field.get("preserve_metadata", False):
                    pass

    return new_package_fields


@register
class ConvertUnits(Processor):
    name = "bcodmo_pipeline_processors.convert_units"
    order_effect = "keep"

    def update_schema(self, schema, params):
        fields = params.get("fields", [])
        return _build_schema(fields, schema)

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live convert_units row logic. In the DuckDB lane the
        # rows arrive already cast to the INPUT types (udf_map cast-in), exactly as
        # the dataflows lane feeds them, so the target column is a Decimal. The
        # live process_resource reads its field types from rows.res.descriptor --
        # the _RowsShim supplies that from the INPUT schema.
        fields = params.get("fields", [])
        # DuckDB lane default missingValues (mirrors the frictionless resource
        # default used by get_missing_values); resource-level missingValues are
        # not yet tracked in the VARCHAR schema.
        missing_values = [""]
        return _df_process_resource(
            _RowsShim(rows, schema),
            fields,
            missing_values,
        )
