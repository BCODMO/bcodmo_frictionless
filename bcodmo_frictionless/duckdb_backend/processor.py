"""
The single-source Processor contract.

Each processor family is authored ONCE as a ``Processor`` subclass. Both engines
are thin adapters over it:

  * dataflows lane  -> ``update_schema`` on the datapackage, then ``process_rows``
  * DuckDB lane     -> ``update_schema`` on the tracked schema, then ``to_sql``
                       if defined, else ``process_rows`` wrapped as a UDF.

``process_rows`` is THE source of truth for row logic. ``to_sql`` is an optional,
opt-in performance override; when present it MUST be a verified mirror of
``process_rows`` (enforced by the equivalence/fuzz tests -- see PLAN.md §3).

Schema representation (Phase 0): a list of frictionless field dicts,
``[{"name": ..., "type": ..., ...}, ...]``.
"""

from __future__ import annotations

import itertools
from typing import Iterable, Iterator, Optional


REGISTRY: dict[str, "Processor"] = {}

_alias_counter = itertools.count()


def next_alias(prefix):
    """A process-unique virtual-table alias for ``DuckDBPyRelation.query``.

    Every ``to_sql`` that wraps its input via ``rel.query(name, sql)`` MUST use a
    fresh name -- reusing a name that appears inside ``rel``'s own (lazy)
    definition makes DuckDB raise 'infinite recursion detected' when the same
    processor runs twice in a chain."""
    return f"_{prefix}_{next(_alias_counter)}"


def register(cls):
    """Class decorator: instantiate and register a Processor by its ``name``."""
    if not getattr(cls, "name", None):
        raise ValueError(f"{cls.__name__} must set a `name` matching the spec `run:` value")
    REGISTRY[cls.name] = cls()
    return cls


class Processor:
    #: the spec ``run:`` value this handles, e.g.
    #: "bcodmo_pipeline_processors.boolean_add_computed_field"
    name: Optional[str] = None

    #: effect on row order for the DuckDB lane: "keep" (preserve __rownum__) or
    #: "reset" (this step defines a new order, e.g. join/sort). See PLAN.md §5.
    order_effect: str = "keep"

    # -- schema (SHARED by both engines: the one place schema logic lives) ----
    def update_schema(self, schema: list, params: dict) -> list:
        """Return the new frictionless field list after this step."""
        raise NotImplementedError

    # -- row logic (THE single source of truth) ------------------------------
    def process_rows(self, rows: Iterable[dict], params: dict, schema: list = None) -> Iterator[dict]:
        """Transform an iterable of row dicts. Reused verbatim by the dataflows
        lane and (as a UDF) by the DuckDB lane.

        ``schema`` is the INPUT frictionless field list. Most processors ignore
        it; type-dependent ones (e.g. set_types casting) need it to know each
        field's declared type. It is optional so callers that don't have a schema
        (unit tests on pure-string data) can omit it."""
        raise NotImplementedError

    # -- optional native fast path (verified mirror of process_rows) ---------
    def to_sql(self, con, rel, params: dict, schema: list):
        """Return a DuckDBPyRelation implementing this step natively, or None to
        fall back to a UDF wrapping ``process_rows`` (the default).

        Relations carry a hidden ``__rownum__`` column; native ``to_sql`` for a
        row processor must preserve it (use ``SELECT * REPLACE/EXCLUDE/RENAME``
        star modifiers, which propagate it automatically)."""
        return None

    # -- engine dispatch (default: per-resource row processor) ---------------
    def apply(self, engine, params: dict) -> None:
        """Apply this step to the engine's matched resources. The default handles
        single-resource row processors; structural/multi-resource processors
        (join, duplicate, concatenate, load, dump, rename_resource, ...) override
        this to operate on ``engine.resources`` directly."""
        for name in engine.matched(params):
            st = engine.resources[name]
            new_schema = self.update_schema(st.schema, params)
            native = self.to_sql(engine.con, st.relation, params, st.schema)
            new_rel = (
                native if native is not None
                else engine.udf_map(st.relation, self, params, st.schema, new_schema)
            )
            engine.resources[name] = engine.state(name, new_rel, new_schema)
