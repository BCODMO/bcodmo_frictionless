"""
Processor: delete_fields (standard dataflows processor).

SEMANTICS
---------
Behavior: removes every field whose name matches one of the ``fields`` patterns;
all other columns and every row pass through unchanged. Row count and order
preserved (order_effect = "keep").

Params:
  * ``fields``: list of patterns. If ``regex`` is false each is matched literally
    (``re.escape``), else as a regex; both are anchored ``^...$``.
  * ``regex``: whether ``fields`` are regexes (default True, matching the
    dataflows library default; the UI sends ``false``).
  * ``resources``: which resources to apply to.

Edge cases: a pattern matching no field is a no-op warning in the library (not an
error); we mirror that (no raise). ``__rownum__`` is never a schema field, so it
is never deleted.

Engine notes: ``process_rows`` derives the surviving field names with the SAME
matcher used by ``update_schema`` and delegates the row drop to the live dataflows
``process_resource`` (source of truth). ``to_sql`` is ``SELECT * EXCLUDE
(matched cols)`` which drops exactly those columns while carrying ``__rownum__``
and everything else. Known-accepted differences: none.
"""

import re

from dataflows.processors.delete_fields import process_resource as _df_process_resource

from ..processor import Processor, register, next_alias


def _patterns(fields, regex):
    return [re.compile("^{}$".format(f if regex else re.escape(f))) for f in fields]


def _is_deleted(name, pats):
    return any(p.match(name) for p in pats)


@register
class DeleteFields(Processor):
    name = "delete_fields"
    order_effect = "keep"

    def update_schema(self, schema, params):
        pats = _patterns(params.get("fields", []), params.get("regex", True))
        return [f for f in schema if not _is_deleted(f["name"], pats)]

    def process_rows(self, rows, params, schema=None):
        pats = _patterns(params.get("fields", []), params.get("regex", True))
        rows = iter(rows)
        try:
            first = next(rows)
        except StopIteration:
            return iter(())
        surviving = [k for k in first.keys() if not _is_deleted(k, pats)]

        def chain():
            yield first
            yield from rows

        # Source of truth: the live dataflows row logic (keep surviving names).
        return _df_process_resource(chain(), surviving)

    def to_sql(self, con, rel, params, schema):
        pats = _patterns(params.get("fields", []), params.get("regex", True))
        deleted = [c for c in rel.columns if c != "__rownum__" and _is_deleted(c, pats)]
        if not deleted:
            return rel
        excl = ", ".join(f'"{c}"' for c in deleted)
        a = next_alias("df")
        return rel.query(a, f"SELECT * EXCLUDE ({excl}) FROM {a}")
