"""
Processor: bcodmo_pipeline_processors.find_replace.

SEMANTICS
---------
Behavior: for each entry in ``fields``, apply an ordered list of ``patterns`` to
the (string) value of that field. Each pattern is a ``re.sub`` of ``find`` (always
a regex) with either a literal ``replace`` string (``replace_function == "string"``,
the default) or an upper/lowercasing callback (``replace_function`` in
``{"uppercase", "lowercase"}``, which uppercases/lowercases each captured group).
A value is left untouched when it is None or in ``missingValues`` UNLESS the pattern
sets ``replace_missing_values`` (then None becomes ``""`` first and the sub runs).
Adds/removes no fields; the schema is UNCHANGED (both lanes keep every field a
string). Drops no rows; row order preserved (order_effect = "keep"). A
``boolean_statement`` gates which rows are processed (non-matching rows pass
through untouched).

Params:
  * ``fields``: list of ``{"name": <field>, "patterns": [{"find", "replace",
    "replace_function", "replace_missing_values"}, ...]}``.
  * ``boolean_statement``: only rows passing it have their patterns applied.
  * ``resources``: which resources to apply to.

Edge cases mirror the live processor: a missing ``name`` or ``find`` raises; a
``replace_function`` not in {string, uppercase, lowercase} raises; ``string``
without a ``replace`` raises; when multiple ``fields`` are given they must all carry
identical ``patterns`` (validated at build time in the live ``find_replace``
factory, mirrored here in ``update_schema``); a field named in ``fields`` that is
not in the resource schema raises (live ``func(package)`` validation, mirrored in
``update_schema``).

TIER: UDF-only (no ``to_sql``). ``find``/``replace`` are Python ``re.sub`` calls
whose regex flavor and replacement semantics (backreferences, the group-wise
upper/lowercase callback) are not provably byte-exact against DuckDB's
``regexp_replace``. So the DuckDB lane always runs the live ``_find_replace`` as a
UDF -- the SAME code the dataflows lane runs -- and correctness is pinned END TO END
by the byte-identical differential test.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.find_replace import (
    _find_replace,
)

from ..processor import Processor, register


def _validate(fields, package_fields):
    """Mirror the live ``find_replace`` validation so both lanes fail identically
    on a bad spec: (1) when more than one field is targeted they must share the
    exact same ``patterns`` (live factory-time check), and (2) every targeted
    field name must exist in the resource schema (live ``func(package)`` check)."""
    if len(fields) > 1:
        reference_patterns = fields[0].get("patterns", [])
        reference_name = fields[0].get("name", None)
        for field in fields[1:]:
            if field.get("patterns", []) != reference_patterns:
                raise Exception(
                    f"All fields in find_replace must have the same patterns. "
                    f'Patterns for field "{field.get("name", None)}" do not match '
                    f'patterns for field "{reference_name}".'
                )
    names = {f["name"] for f in package_fields}
    for field in fields:
        name = field.get("name", None)
        if name is not None and name not in names:
            raise Exception(
                f'Field "{name}" not found. Available fields: {sorted(names)}'
            )


@register
class FindReplace(Processor):
    name = "bcodmo_pipeline_processors.find_replace"
    order_effect = "keep"

    def update_schema(self, schema, params):
        # find_replace edits string values in place; it does not add, remove, or
        # retype any field, so the schema passes through unchanged. We still run
        # the live spec validation here so a bad spec fails the same in both lanes.
        fields = params.get("fields", [])
        _validate(fields, schema)
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        # Source of truth: the live find_replace row logic (`_find_replace`). The
        # DuckDB lane feeds rows already cast to the INPUT types (all strings here),
        # exactly as the dataflows lane does.
        fields = params.get("fields", [])
        boolean_statement = params.get("boolean_statement")
        # DuckDB lane default missingValues (mirrors the frictionless resource
        # default returned by get_missing_values); resource-level missingValues are
        # not yet tracked in the VARCHAR schema.
        missing_values = [""]
        return _find_replace(
            rows, fields, missing_values, boolean_statement=boolean_statement
        )
