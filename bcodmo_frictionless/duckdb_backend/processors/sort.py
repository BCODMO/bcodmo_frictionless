"""
Processor: sort (STANDARD dataflows processor -- bare name).

SEMANTICS
---------
Reorders each matched resource's rows by the ``sort-by`` key (a format string like
``"{field}"`` or a list of field names), ascending or ``reverse``. Fields, values,
and row count are unchanged; only order changes (order_effect = "reset" -- the
resource gets a fresh ``__rownum__`` in sorted order).

Params:
  * ``sort-by``: key spec (format string or list) -- the live ``KeyCalc``.
  * ``reverse``: descending when truthy (default False).
  * ``resources``: which resources to apply to.

TIER: structural / row-reorder. Overrides ``apply``. The ORDER is produced by the
LIVE ``dataflows.processors.sort_rows._sorter`` (+ ``KeyCalc``) reused VERBATIM, so
the numeric bit-encoding of int/float/Decimal keys, the row-number tie-break, and
the reverse handling cannot diverge. ``KeyCalc`` inspects TYPED values (its numeric
branch triggers on int/float/Decimal), so rows are cast VARCHAR->typed (shared
``casting``) to compute the key -- exactly the values the dataflows lane sees. We
then re-ingest the ORIGINAL VARCHAR rows in the computed order (no value
round-trip: sort never changes values). ``_sorter`` uses a KVFile, so this is
out-of-core for free.
"""

from collections import deque

from dataflows.processors.sort_rows import KeyCalc as _KeyCalc, _sorter as _live_sorter

from ..processor import Processor, register
from ..casting import cast_rows


@register
class Sort(Processor):
    name = "sort"
    order_effect = "reset"

    def update_schema(self, schema, params):
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        # NEVER-OOM: the live ``_sorter`` is an external merge sort (KVFile-backed,
        # ``batch_size`` rows per run), so the sort itself is out-of-core. We must
        # not re-introduce a Python-side full materialization, so we STREAM: each
        # input row flows in as its typed form (for KeyCalc's type-aware key) with
        # the ORIGINAL VARCHAR payload carried alongside under ``__varchar__``; the
        # sorter spills/merges to disk and yields rows in order; we peel the VARCHAR
        # back out and stream it into a fresh table (reingest_stream). Sort never
        # changes values, so re-emitting the untouched VARCHAR round-trips exactly.
        key_calc = _KeyCalc(params["sort-by"])
        reverse = bool(params.get("reverse"))
        batch_size = params.get("batch_size", 1000)
        for name in engine.matched(params):
            schema = engine.resources[name].schema

            # Pair each typed row with its source VARCHAR row without a second
            # pass: ``feed`` stashes each VARCHAR row as it is pulled, and ``tagged``
            # pops it once the 1:1 streaming cast (schema_validator) yields the
            # matching typed row -- so ``carrier`` holds ~1 row, not the resource.
            carrier = deque()

            def feed(src=engine.rows_iter(name)):
                for vc in src:
                    carrier.append(vc)
                    yield vc

            def tagged(typed_iter=cast_rows(feed(), schema)):
                for typed in typed_iter:
                    vc = carrier.popleft()
                    row = dict(typed)
                    row["__varchar__"] = vc  # inert to KeyCalc (reads key fields)
                    yield row

            def ordered_varchar():
                for r in _live_sorter(tagged(), key_calc, reverse, batch_size):
                    yield r["__varchar__"]

            engine.reingest_stream(name, ordered_varchar(), schema)
