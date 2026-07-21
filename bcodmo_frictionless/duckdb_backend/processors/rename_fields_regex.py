"""
Processor: bcodmo_pipeline_processors.rename_fields_regex.

SEMANTICS
---------
Behavior: renames field NAMES via a regex substitution. For each name in
``fields`` the new name is ``re.sub(pattern["find"], pattern["replace"], name)``;
the column keeps its value under the new name. Row count and order preserved
(order_effect = "keep").

Params:
  * ``fields``: list of existing field NAMES (bare strings, not
    ``{old_field,new_field}`` dicts) to rename.
  * ``pattern``: ``{"find": <regex>, "replace": <repl>}`` applied via ``re.sub``
    to each named field (``replace`` may use backreferences). REQUIRED.
  * ``resources``: which resources to apply to.

Edge cases / invariants: a name in ``fields`` that is absent from the schema is
an error (mirrors the live ``func(package)``, which validates against the
ORIGINAL field-name set before any rename). A ``pattern`` whose ``find`` does not
match a given name yields that same name back -> a no-op rename (both lanes leave
the column untouched). Renaming to a name that already exists raises at row time
inside the live ``process_resource``.

Engine notes: ``process_rows`` delegates to the live ``process_resource`` (source
of truth; signature ``(rows, fields, pattern)``). ``update_schema`` applies the
same regex rename to the tracked field list, IN PLACE (order preserved). ``to_sql``
is ``SELECT * RENAME (old AS new, ...)`` which carries ``__rownum__`` and untouched
columns; no-op (old == new) renames are dropped from the clause. The renamed-name
computation is SHARED via ``_compute_renames`` so schema and SQL cannot diverge.
Known-accepted differences: none.
"""

import re

from bcodmo_frictionless.bcodmo_pipeline_processors.rename_fields_regex import (
    process_resource,
)

from ..processor import Processor, register, next_alias


def _compute_renames(schema, params):
    """The ONE place the regex renamed-name computation lives (shared by
    ``update_schema`` and ``to_sql`` so they can never diverge). Mirrors the live
    ``func(package)``: require ``pattern``, validate each named field exists in the
    ORIGINAL schema, then compute its new name via ``re.sub(find, replace, name)``.

    Returns a list of ``(old_name, new_name)`` pairs in ``fields`` order."""
    pattern = params.get("pattern")
    if not pattern:
        raise Exception('The "pattern" parameter is required')
    fields = params.get("fields", [])
    names = {f["name"] for f in schema}
    renames = []
    for field in fields:
        if field not in names:
            raise Exception(
                f'Field "{field}" not found. Available fields: {sorted(names)}'
            )
        new_name = re.sub(
            str(pattern["find"]), str(pattern["replace"]), str(field)
        )
        renames.append((field, new_name))
    return renames


@register
class RenameFieldsRegex(Processor):
    name = "bcodmo_pipeline_processors.rename_fields_regex"
    order_effect = "keep"

    def update_schema(self, schema, params):
        renames = dict(_compute_renames(schema, params))
        out = [dict(f) for f in schema]
        for f in out:
            if f["name"] in renames:
                f["name"] = renames[f["name"]]
        return out

    def process_rows(self, rows, params, schema=None):
        return process_resource(
            rows, params.get("fields", []), params.get("pattern")
        )

    def to_sql(self, con, rel, params, schema):
        renames = [(o, n) for o, n in _compute_renames(schema, params) if o != n]
        if not renames:
            return rel
        clause = ", ".join(f'"{o}" AS "{n}"' for o, n in renames)
        a = next_alias("rfr")
        return rel.query(a, f"SELECT * RENAME ({clause}) FROM {a}")
