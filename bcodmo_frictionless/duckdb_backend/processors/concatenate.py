"""
Processor: bcodmo_pipeline_processors.concatenate.

SEMANTICS
---------
Behavior: merges several source resources into ONE new target resource. Each target
field collects values from the source field(s) mapped to it (``fields`` param);
unmapped target fields default to ``""``; optional ``include_source_names`` columns
record each row's origin (resource name / path / file). The source resources are
removed and the target inserted where the first source was. Row order in the target
is the concatenation order of the sources (order_effect = "reset" -- the target gets
a fresh ``__rownum__``).

Params:
  * ``fields``: ``{target_field: [source_field, ...] | None}`` mapping.
  * ``sources``: which resources to concatenate (name or list).
  * ``target``: ``{"name": ...}`` (default name "concat").
  * ``include_source_names``: ``[{"type": "resource"|"path"|"file", "column_name": ...}]``
    (plus the deprecated ``include_source_name``/``source_field_name`` form).
  * ``missing_values``: recorded on the target schema.

Edge cases mirror the live processor: a ``sources`` pattern matching nothing raises;
a target name colliding with an existing resource raises; a source field explicitly
listed but absent everywhere raises; an empty-array target field matching no source
raises; a source row with no mapped values raises.

TIER: structural / multi-resource. Overrides ``apply``. ROW logic is the LIVE
``concatenator`` reused verbatim (fed via a tiny dataflows-resource shim), so
value mapping / source-name columns / the "no values" error cannot diverge. The
target SCHEMA is re-derived here (mirroring the live ``concatenate`` func) and
pinned by the byte-identical differential test -- the same model as every leaf
processor's ``update_schema``.

NOT CARRIED (Phase 0): per-source ``primaryKey`` (the field-list schema does not
track it) and redis progress. ``path``/``file`` source columns come out as the live
concatenator produces them for in-memory resources (no ``dpp:streamedFrom`` -> None),
which is byte-identical to the dataflows lane because it is the SAME code.
"""

import copy

from bcodmo_frictionless.bcodmo_pipeline_processors.concatenate import (
    concatenator as _live_concatenator,
)

from ..processor import Processor, register


class _Res:
    """The ``.res`` of a dataflows resource, as ``concatenator`` reads it."""

    def __init__(self, name, descriptor):
        self.name = name
        self._Resource__current_descriptor = descriptor


class _ResourceShim:
    """Stands in for a dataflows resource: has ``.res`` and iterates its rows."""

    def __init__(self, name, rows, descriptor):
        self.res = _Res(name, descriptor)
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


def _resolve_include_source_names(params):
    """Mirror the live ``flow()``: support the deprecated single-name form."""
    include = params.get("include_source_names", []) or []
    if params.get("include_source_name", False) and not len(include):
        include = [
            {
                "type": params.get("include_source_name", False),
                "column_name": params.get("source_field_name", "source_name"),
            }
        ]
    return include


def _match_sources(all_names, sources):
    """Resource names to concatenate, in datapackage order (None -> all)."""
    if sources is None:
        return list(all_names)
    if isinstance(sources, (list, tuple)):
        return [n for n in all_names if n in sources]
    return [n for n in all_names if n == sources]


def _plan_target(source_schemas, params):
    """Re-derivation of the live ``concatenate`` schema transform. Returns
    ``(target_name, target_fields, field_mapping, all_target_fields, include)``.

    ``source_schemas`` is the ordered list of matched sources' field lists.
    Mirrors bcodmo_pipeline_processors/concatenate.py lines ~78-169."""
    fields = copy.deepcopy(params.get("fields", {}))
    target = copy.deepcopy(params.get("target", {})) or {}
    include = _resolve_include_source_names(params)

    if "name" not in target:
        target["name"] = "concat"
    if not target["name"]:
        raise Exception("The concatenate target name cannot be empty")
    target_name = target["name"]

    # source field name -> target field name (plus each target maps to itself)
    field_mapping = {}
    empty_array_fields = set()
    explicit_source_fields = set()
    for target_field, source_fields in fields.items():
        if source_fields is not None:
            if len(source_fields) == 0:
                empty_array_fields.add(target_field)
            for source_field in source_fields:
                if source_field in field_mapping:
                    raise RuntimeError(
                        "Duplicate appearance of %s (%r)" % (source_field, field_mapping)
                    )
                field_mapping[source_field] = target_field
                explicit_source_fields.add(source_field)
        if target_field in field_mapping:
            raise RuntimeError("Duplicate appearance of %s" % target_field)
        field_mapping[target_field] = target_field

    all_target_fields = sorted(fields.keys())
    needed_fields = sorted(fields.keys())
    for source in include:
        if source["column_name"] in needed_fields:
            raise Exception(
                f"source_field_name \"{source['column_name']}\" field name already exists"
            )

    target_fields = []
    available_source_fields = set()
    for schema_fields in source_schemas:
        for field in schema_fields:
            field = dict(field)
            orig_name = field["name"]
            available_source_fields.add(orig_name)
            if orig_name in field_mapping:
                name = field_mapping[orig_name]
                if name not in needed_fields:
                    continue
                target_fields.append(field)
                field["name"] = name
                needed_fields.remove(name)

    unmatched_empty = empty_array_fields & set(needed_fields)
    if unmatched_empty:
        raise Exception(
            f"The following fields were passed with an empty array but do not match "
            f"any fields in the source data: {sorted(unmatched_empty)}"
        )
    unmatched_explicit = explicit_source_fields - available_source_fields
    if unmatched_explicit:
        raise Exception(
            f"The following source fields were explicitly listed but do not exist "
            f"in any of the source resources: {sorted(unmatched_explicit)}. "
            f"Available source fields: {sorted(available_source_fields)}"
        )

    for name in needed_fields:
        target_fields.append(dict(name=name, type="string"))
    for source in include:
        target_fields.append({"name": source["column_name"], "type": "string"})

    return target_name, target_fields, field_mapping, all_target_fields, include


@register
class Concatenate(Processor):
    name = "bcodmo_pipeline_processors.concatenate"
    order_effect = "reset"

    def update_schema(self, schema, params):
        # Not used for the multi-resource apply; present for contract completeness.
        return [dict(f) for f in schema]

    def process_rows(self, rows, params, schema=None):
        return iter(rows)

    def apply(self, engine, params):
        all_names = list(engine.resources)
        matched = _match_sources(all_names, params.get("sources"))
        if not matched:
            raise Exception(
                f"Source resource pattern {params.get('sources')!r} did not match any "
                f"resources in datapackage. Available resources: {all_names}"
            )

        source_schemas = [engine.resources[n].schema for n in matched]
        target_name, target_fields, field_mapping, all_target_fields, include = (
            _plan_target(source_schemas, params)
        )
        if target_name in engine.resources and target_name not in matched:
            raise Exception(
                f"Name of concatenate target ({target_name}) cannot match an "
                f"existing resource name ({target_name})"
            )

        # NEVER-OOM ROW logic: the live concatenator over LAZY dataflows-resource
        # shims -- source rows stream in (independent cursors via rows_iter) and the
        # concatenated target streams out into a fresh table via reingest_stream, so
        # neither the sources nor the merged output is materialized. concatenator
        # only MOVES values (no cast). Because the target may itself be one of the
        # matched sources, reingest_stream writes a NEW table before we drop the
        # sources' old state -- and the sources are only dropped AFTER the stream is
        # fully consumed (below), so their reads complete first.
        shims = [_ResourceShim(n, engine.rows_iter(n), {}) for n in matched]
        stream = _live_concatenator(shims, all_target_fields, field_mapping, include)
        engine.reingest_stream(target_name, stream, target_fields)

        # Rebuild the resource order: the target takes the first source's slot; the
        # other matched sources are dropped; non-matched keep their positions. The
        # dict comprehension only keeps ``new_order`` names, so dropping is implicit
        # (the target is already registered by reingest_stream above).
        new_order = []
        inserted = False
        for n in all_names:
            if n in matched:
                if not inserted:
                    new_order.append(target_name)
                    inserted = True
            else:
                new_order.append(n)
        if target_name not in new_order:  # target has no source among matched
            new_order.append(target_name)
        engine.resources = {n: engine.resources[n] for n in new_order}
