"""
Processor: join (STANDARD dataflows processor -- bare name, no bcodmo prefix).

SEMANTICS
---------
Joins a ``source`` resource into a ``target`` resource by key. Each ``fields``
entry aggregates one source field into a new target column (aggregate default
"any"). Row logic:

  * mode ``inner``       -> keep only target rows whose key matched a source key.
  * mode ``half-outer``  -> keep every target row; unmatched rows get the target's
                            own value for each field key (or None).
  * mode ``full-outer``  -> half-outer, plus one appended row per source key that
                            no target row matched (join fields + source key cols).
  * ``source.delete``    -> drop the source resource after indexing (default False,
                            matching ``standard_flows.join``; the dataflows
                            primitive's own default is True).
  * deprecated ``full``  -> if not None, overrides ``mode`` (True->half-outer).

This is the bare/standard run-name the laminar UI emits (see the
duckdb-backend-processor-routing memory). The dataflows reference is the STANDARD
``dataflows.join`` primitive (via ``standard_flows.join``), NOT the dead/broken
``bcodmo_pipeline_processors.join``.

TIER: structural / multi-resource. Overrides ``apply``. ROW LOGIC AND TARGET
SCHEMA are the LIVE ``dataflows.processors.join.join_aux`` reused VERBATIM -- we
drive the real package-processing generator with a resource/package shim (same
trick concatenate uses for ``concatenator``). So the aggregators (sum/avg/median/
max/min/first/last/any/count), the KVFile-ordered full-outer tail, the field
expansion/ordering, and the appended target-schema field types cannot diverge
from the dataflows lane -- it is the SAME code on the SAME (cast-in) values.

Unlike concatenate, join's aggregators do TYPE-DEPENDENT arithmetic, so the
VARCHAR storage is cast to typed values BEFORE ``join_aux`` sees them (via the
shared ``casting`` module, exactly as ``Engine.udf_map`` does) and the joined
rows are formatted back to VARCHAR-safe storage after -- both through the one
casting source, so casting can't diverge either.

NOT CARRIED (Phase 0):
  * Array-producing aggregators (``set``/``array``/``counters``) -- their list/
    tuple values do not round-trip byte-exactly through VARCHAR storage (Python
    ``repr`` != JSON, tuple != list). We RAISE rather than silently diverge.
  * ``deduplication`` (join_with_self, ``target.key`` = None) works if invoked,
    but ``standard_flows.join`` always passes a target key so the UI never hits it.
  * redis progress and the ``dpp:streaming`` resource prop (irrelevant to values).
"""

import copy

from dataflows.processors.join import join_aux as _live_join_aux

from ..processor import Processor, register
from ..casting import cast_rows, format_out_iter


_ARRAY_AGGS = {"set", "array", "counters"}


class _Res:
    """The ``.res`` of a dataflows resource, as ``join_aux`` reads it (``.name``)."""

    def __init__(self, name):
        self.name = name


class _ResourceShim:
    """Stands in for a dataflows resource: has ``.res.name`` and iterates rows."""

    def __init__(self, name, rows):
        self.res = _Res(name)
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _Pkg:
    def __init__(self, descriptor):
        self.descriptor = descriptor


class _PackageShim:
    """Stands in for a dataflows ``PackageWrapper``: ``.pkg.descriptor`` + iterates
    its resources (in datapackage order)."""

    def __init__(self, descriptor, resources):
        self.pkg = _Pkg(descriptor)
        self._resources = resources

    def __iter__(self):
        return iter(self._resources)


def _reject_array_aggregators(fields):
    for spec in fields.values():
        if isinstance(spec, dict) and spec.get("aggregate") in _ARRAY_AGGS:
            raise NotImplementedError(
                f"DuckDB backend join: array-producing aggregator "
                f"{spec.get('aggregate')!r} is not supported in Phase 0 (its list/"
                f"tuple value would not round-trip byte-exactly through VARCHAR "
                f"storage). Supported aggregators: sum, avg, median, max, min, "
                f"first, last, any, count."
            )


@register
class Join(Processor):
    name = "join"
    order_effect = "reset"

    def update_schema(self, schema, params):
        # Not used for the multi-resource apply; present for contract completeness.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        source = params.get("source") or {}
        target = params.get("target") or {}
        source_name = source["name"]
        target_name = target["name"]
        source_key = source["key"]
        target_key = target.get("key")
        fields = copy.deepcopy(params.get("fields", {}) or {})
        _reject_array_aggregators(fields)
        full = params.get("full", None)
        mode = params.get("mode", "half-outer")
        # standard_flows.join defaults source_delete to False (the dataflows
        # primitive itself defaults True) -- mirror standard_flows exactly.
        source_delete = source.get("delete", False)

        names = list(engine.resources)
        if source_name not in names:
            raise Exception(
                f"join source resource ({source_name}) not found; resources={names}"
            )
        if target_name not in names:
            raise Exception(
                f"join target resource ({target_name}) not found; resources={names}"
            )

        # NEVER-OOM: feed the LIVE join_aux LAZY typed-row streams and drain its
        # output straight into a fresh table -- nothing materializes the source,
        # target, or joined result in Python. join_aux keeps the join index in a
        # disk-backed KVFile, so the only bounded in-memory state is the fetch
        # chunks + insert batch. Only source and target are handed to join_aux (the
        # two resources it touches); every other resource keeps its state and slot.
        #
        # cast-in (VARCHAR -> typed) is LAZY (cast_rows over rows_iter), so the
        # aggregator arithmetic still sees the same typed values as the dataflows
        # lane, through the one casting source. rows_iter reads on an independent
        # cursor, so streaming the joined output into a NEW table on the main
        # connection (reingest_stream) cannot clobber the still-in-flight reads of
        # the source/target it is derived from. Source MUST precede target in the
        # package (the dataflows join contract).
        descriptor = {"resources": []}
        shims = []
        for n in (source_name, target_name):
            schema = engine.resources[n].schema
            descriptor["resources"].append(
                {"name": n, "schema": {"fields": copy.deepcopy(schema)}}
            )
            typed = cast_rows(engine.rows_iter(n), schema)  # lazy generator
            shims.append(_ResourceShim(n, typed))

        package = _PackageShim(descriptor, shims)

        # Drive the LIVE join generator. next() runs process_datapackage (which
        # mutates ``descriptor`` in place: drops the source if source_delete,
        # appends join fields to the target schema, expands/orders fields) and
        # advances past its ``yield package.pkg``. The remaining yields are one
        # row-iterable per OUTPUT resource, in the mutated-descriptor order.
        func = _live_join_aux(
            source_name, source_key, source_delete,
            target_name, target_key, fields, full, mode,
        )
        gen = func(package)
        next(gen)
        new_descs = descriptor["resources"]

        # Consume each output iterable IN ORDER, FULLY, before the next: the
        # source's indexer (which populates the join KVFile as a side effect) is
        # yielded before the target's process_target (which reads it), so this
        # preserves the source-before-target dependency. We iterate ``gen`` itself
        # (indexing ``new_descs`` by position) so the loop runs it to StopIteration
        # -- that trailing step executes join_aux's db.close() cleanup (a ``zip``
        # against the shorter ``new_descs`` would leave the KVFile unclosed). The
        # target output streams into a fresh table via reingest_stream (format-out
        # -> VARCHAR on the fly); the source pass-through (when not deleted) is
        # drained for its side effect and keeps its existing VARCHAR state.
        for i, it in enumerate(gen):
            desc = new_descs[i]
            if desc["name"] == target_name:
                out_schema = desc["schema"]["fields"]
                engine.reingest_stream(target_name, format_out_iter(it, out_schema), out_schema)
            else:
                for _ in it:  # drain source pass-through (indexing side effect)
                    pass

        # Reflect source.delete. The target was replaced IN PLACE by
        # reingest_stream (dict key kept -> original slot preserved); other
        # resources are untouched. So dropping the source is all that remains, and
        # relative order is preserved automatically.
        if source_delete:
            engine.resources.pop(source_name, None)
