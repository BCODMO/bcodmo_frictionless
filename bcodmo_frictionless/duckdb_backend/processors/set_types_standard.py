"""
Processor: set_types (STANDARD dataflows processor -- bare name, distinct from
``bcodmo_pipeline_processors.set_types``).

SEMANTICS
---------
For each ``types`` entry ``name -> options``: if ``options`` is not None, the
matched field descriptors are updated with ``options`` (type/format/...) and the
values cast to that type; if ``options`` is None, the matched field is DELETED.
When ``types`` is absent the step is a no-op. Field names match by regex or
literally per ``regex``. Row count/order preserved (order_effect = "keep").

REFERENCE NOTE: this is a faithful mirror of ``standard_flows.set_types``, which
wraps the ``dataflows.set_type`` primitive (``set_type as _set_type``;
``field.update(options)`` + ``schema_validator`` cast) and, for a None option,
``delete_fields([name], resources=...)``. Crucially, standard_flows' delete branch
passes NO regex arg, so it uses delete_fields' default ``regex=True`` regardless of
the step's ``regex`` flag -- ``_transform`` mirrors exactly that (set branch honors
``regex``; delete branch is always regex=True). The differential harness cannot
import standard_flows directly (it pulls in AWS Secrets Manager on import), so it
builds its reference from the same ``dataflows.set_type``/``delete_fields``
primitives standard_flows wraps.

TIER: schema / deferred-cast (like ``bcodmo_pipeline_processors.set_types``). The
DuckDB lane stores VARCHAR and casts at the UDF boundary / dump via the shared
``casting`` module, so this is SCHEMA-ONLY (``apply`` override): update field
descriptors (or drop them), leave stored values untouched. Correctness is proven
END TO END by the byte-identical dump, not a per-step ``to_sql`` gate.

Phase-0 note: ``dataflows.set_type`` asserts a matched field exists across ALL
matched resources; we raise per-resource on the single-resource path the harness
exercises (standard set_types is used per-resource / resources=None in practice).
"""

import re

from ..processor import Processor, register


def _pattern(name, regex):
    return re.compile(f"^{name}$" if regex else f"^{re.escape(name)}$")


def _transform(schema, types, regex):
    out = [dict(f) for f in schema]
    for name, options in types.items():
        if options is None:
            # delete_fields branch: standard_flows.set_types calls
            # ``delete_fields([name], resources=...)`` WITHOUT a regex arg, so it
            # uses delete_fields' default (regex=True) regardless of the step's
            # ``regex`` flag. Drop matched fields (no raise if none match).
            pat = _pattern(name, True)
            out = [f for f in out if not pat.match(f["name"])]
            continue
        # set_type branch honors the step's ``regex`` flag.
        pat = _pattern(name, regex)
        matched_any = False
        for f in out:
            if pat.match(f["name"]):
                matched_any = True
                f.update(options)
        if not matched_any:
            raise Exception(f"Failed to find field {name} in schema")
    return out


@register
class SetTypesStandard(Processor):
    name = "set_types"
    order_effect = "keep"

    def update_schema(self, schema, params):
        if "types" not in params:
            return [dict(f) for f in schema]
        return _transform(schema, params["types"], params.get("regex", True))

    def process_rows(self, rows, params, schema=None):
        # Source-of-truth cast to the UPDATED types (delegates to the shared
        # casting = frictionless schema_validator), for the UDF/dump boundary.
        from ..casting import cast_rows

        if "types" not in params:
            return iter(rows)
        if schema is None:
            raise ValueError("set_types.process_rows requires the input schema")
        return cast_rows(rows, self.update_schema(schema, params))

    def apply(self, engine, params):
        # Schema/deferred-cast tier: update (or drop) field descriptors, leave
        # VARCHAR values. The cast materializes at the UDF boundary / dump.
        if "types" not in params:
            return
        for name in engine.matched(params):
            st = engine.resources[name]
            engine.resources[name] = engine.state(
                name, st.relation, self.update_schema(st.schema, params)
            )
