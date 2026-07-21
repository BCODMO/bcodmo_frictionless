"""
Chained DuckDB engine.

Holds a connection and a ``{resource_name: ResourceState}`` map. Each relation
carries a hidden ``__rownum__`` BIGINT column (a normal column, not in the
tracked frictionless ``schema``) so row order survives across steps: native
``to_sql`` star-projections propagate it, and dump does ``ORDER BY __rownum__``.
See PLAN.md §5.

Row processors are dispatched via ``Processor.apply`` (default per-resource loop,
native ``to_sql`` else UDF-default). Structural processors override ``apply``.

Phase-0 note: input is ingested all-VARCHAR (matching bcodmo load's
``cast_strategy=strings``); ``set_types`` casts later. The UDF-default path
(``udf_map``) is a materialize-map for now; Phase 1 makes it a chunked Arrow UDF.
"""

import duckdb

from .processor import REGISTRY
from .casting import cast_rows, format_out_rows


class ResourceState:
    __slots__ = ("name", "relation", "schema", "descriptor")

    def __init__(self, name, relation, schema, descriptor=None):
        self.name = name
        self.relation = relation
        self.schema = schema
        # The full frictionless resource descriptor as produced by ``load``
        # (path, profile, format, dpp:streamedFrom/streaming, schema). Carried so
        # a terminal ``dump`` can reproduce a byte-identical datapackage.json.
        # ``None`` for resources born without one (in-memory ingest, join/
        # concatenate targets) -- dump synthesizes a minimal descriptor then.
        self.descriptor = descriptor


def _s(v):
    return None if v is None else str(v)


def to_rows(rel):
    """Materialize a DuckDBPyRelation to a list of row dicts."""
    cols = [d[0] for d in rel.description]
    return [dict(zip(cols, r)) for r in rel.fetchall()]


def run_udf(con, rel, proc, params, schema=None):
    """Single-step UDF path (the process_rows source of truth on a relation's
    rows). Used by unit tests; the chained Engine uses ``Engine.udf_map``."""
    return list(proc.process_rows(to_rows(rel), params, schema))


class Engine:
    def __init__(self, threads=4, memory_limit=None, temp_directory=None):
        self.con = duckdb.connect()
        self.con.execute(f"SET threads TO {threads}")
        if memory_limit:
            self.con.execute(f"SET memory_limit='{memory_limit}'")
        if temp_directory:
            self.con.execute(f"SET temp_directory='{temp_directory}'")
        # Dedicated write cursor (independent result state, shared in-memory
        # catalog) for table CREATE/INSERT, so a structural op can build its output
        # table while a lazy read streams on the main connection. See _fill_table.
        self._wc = self.con.cursor()
        self.resources: dict[str, ResourceState] = {}
        self._seq = 0

    # -- helpers used by Processor.apply ------------------------------------
    def state(self, name, relation, schema, descriptor=None):
        # Auto-carry the load-time descriptor across same-name schema/relation
        # swaps, so leaf processors (which call state() without a descriptor)
        # don't drop it. The descriptor's own fields go stale, but dump always
        # re-stamps them from the live tracked ``schema``, so that's harmless.
        if descriptor is None and name in self.resources:
            descriptor = self.resources[name].descriptor
        return ResourceState(name, relation, schema, descriptor)

    def matched(self, params):
        """Resource names this step applies to (None ⇒ all)."""
        res = params.get("resources")
        names = list(self.resources)
        if res is None:
            return names
        if isinstance(res, (list, tuple)):
            return [n for n in names if n in res]
        return [n for n in names if n == res]

    def view(self, rel):
        self._seq += 1
        vname = f"_v{self._seq}"
        rel.create_view(vname, replace=True)
        return vname

    # -- ingest -------------------------------------------------------------
    def _fill_table(self, tbl, cols, rows, batch):
        """Create ``tbl`` (``__rownum__`` BIGINT + VARCHAR ``cols``) and stream
        ``rows`` (dicts) into it in bounded batches, assigning ``__rownum__`` by
        enumeration. Returns nothing; only ``batch`` rows are ever held in Python.

        One multi-row INSERT ... VALUES per batch (a single prepared statement
        binding batch*ncols params) -- ~18x faster than per-row executemany, and
        NULL-safe: a ``?`` bound to None becomes SQL NULL.

        Writes run on the dedicated write cursor ``self._wc`` so they don't clobber
        an in-flight lazy read on the main connection (the structural read-while-
        write case). The table lands in the shared catalog, visible to self.con."""
        coldefs = ", ".join([f'"{c}" VARCHAR' for c in cols])
        self._wc.execute(
            f'CREATE OR REPLACE TABLE "{tbl}" ("__rownum__" BIGINT, {coldefs})'
        )
        ncols = len(cols) + 1
        row_ph = "(" + ",".join(["?"] * ncols) + ")"
        full_stmt = f'INSERT INTO "{tbl}" VALUES ' + ",".join([row_ph] * batch)

        def _flush(buf):
            if not buf:
                return
            stmt = full_stmt if len(buf) == batch else (
                f'INSERT INTO "{tbl}" VALUES ' + ",".join([row_ph] * len(buf))
            )
            self._wc.execute(stmt, [v for row in buf for v in row])

        buf = []
        i = 0
        for r in rows:
            buf.append([i] + [_s(r.get(c)) for c in cols])
            i += 1
            if len(buf) >= batch:
                _flush(buf)
                buf = []
        _flush(buf)

    def ingest_iter(self, name, rows, schema, batch=10000, descriptor=None):
        """Build a base relation for ``name`` from a (possibly streaming) iterable
        of Python rows (all-VARCHAR) + a hidden ``__rownum__``.

        Bounded batches so an arbitrarily large source (e.g. a big ``load``) never
        has to be fully materialized -- the DuckDB table spills to disk under
        ``memory_limit``/``temp_directory``. This is the never-OOM ingest seam
        ``load`` feeds. ``ingest_rows`` is the eager wrapper. NOTE: the backing
        table is ``_ingest_{name}``; do not use this to re-ingest a resource that
        is being READ concurrently (see ``reingest_stream``)."""
        cols = [f["name"] for f in schema]
        self._fill_table(f"_ingest_{name}", cols, rows, batch)
        rel = self.con.sql(f'SELECT * FROM "_ingest_{name}"')
        self.resources[name] = ResourceState(
            name, rel, [dict(f) for f in schema], descriptor
        )
        return self.resources[name]

    def reingest_stream(self, name, rows, schema, batch=10000, descriptor=None):
        """Like ``ingest_iter`` but writes to a FRESH uniquely-named table, so it
        is safe to call while ``name`` (its old table) is still being streamed
        from -- the structural-op case: a join/sort reads its input lazily from
        DuckDB while its output streams into a new table on the same connection
        (an independent read cursor, see ``rows_iter``, makes this collision-free).
        The old table is left for the connection to reclaim; the resource is
        repointed at the new relation (carrying the prior descriptor by default)."""
        cols = [f["name"] for f in schema]
        tbl = f"_reingest_{name}_{self._next()}"
        self._fill_table(tbl, cols, rows, batch)
        rel = self.con.sql(f'SELECT * FROM "{tbl}"')
        if descriptor is None and name in self.resources:
            descriptor = self.resources[name].descriptor
        self.resources[name] = ResourceState(
            name, rel, [dict(f) for f in schema], descriptor
        )
        return self.resources[name]

    def ingest_rows(self, name, rows, schema, descriptor=None):
        """Eager convenience wrapper over ``ingest_iter`` for in-memory row lists
        (unit tests / the differential harness)."""
        return self.ingest_iter(name, rows, schema, descriptor=descriptor)

    # -- UDF-default path (materialize-map; Phase 1 -> chunked Arrow UDF) ----
    def udf_map(self, relation, proc, params, schema=None, out_schema=None):
        cols = [d[0] for d in relation.description]
        rows = [dict(zip(cols, r)) for r in relation.fetchall()]
        rownums = [r.pop("__rownum__", i) for i, r in enumerate(rows)]
        # Cast VARCHAR storage -> typed BEFORE process_rows, so a bcodmo processor
        # sees the SAME typed values the dataflows lane feeds it (set_types casts
        # inline there; here the cast is deferred, so we materialize it at the UDF
        # boundary). Uses the shared casting module = frictionless schema_validator.
        if schema:
            rows = list(cast_rows(rows, schema))
        out = list(proc.process_rows(rows, params, schema))
        assert len(out) == len(rownums), (
            f"{proc.name}: UDF-default expects a 1:1 row map "
            f"({len(rownums)} in, {len(out)} out); use a native to_sql for "
            f"filtering/expanding processors."
        )
        # Serialize typed outputs back to VARCHAR-safe forms that re-cast
        # identically (temporals via the OUTPUT field's strftime format). Inverse
        # of the cast-in above; uses the post-step schema for the new field types.
        if out_schema:
            out = format_out_rows(out, out_schema)
        for r, rn in zip(out, rownums):
            r["__rownum__"] = rn
        return self._relation_from_rows(out)

    def _relation_from_rows(self, rows):
        cols = list(rows[0].keys()) if rows else ["__rownum__"]
        data_cols = [c for c in cols if c != "__rownum__"]
        tbl = f"_udf{self._next()}"
        coldefs = ", ".join(['"__rownum__" BIGINT'] + [f'"{c}" VARCHAR' for c in data_cols])
        self.con.execute(f'CREATE OR REPLACE TABLE "{tbl}" ({coldefs})')
        order = ["__rownum__"] + data_cols
        self.con.executemany(
            f'INSERT INTO "{tbl}" VALUES ({", ".join(["?"] * len(order))})',
            [[r.get(c) if c == "__rownum__" else _s(r.get(c)) for c in order] for r in rows],
        )
        return self.con.sql(f'SELECT * FROM "{tbl}"')

    def _next(self):
        self._seq += 1
        return self._seq

    # -- run ----------------------------------------------------------------
    def apply(self, step):
        REGISTRY[step["run"]].apply(self, step["parameters"])

    def run(self, steps):
        for step in steps:
            self.apply(step)

    # -- output -------------------------------------------------------------
    def rows_iter(self, name, chunk=10000):
        """Ordered output rows (schema fields only) as a LAZY stream, fetched
        from DuckDB in bounded chunks. Only ``chunk`` rows are held in Python at
        once, so egress is never-OOM: a huge resource (its DuckDB table already
        disk-spilled under ``memory_limit``) streams out without materializing.
        The ORDER BY __rownum__ sort is DuckDB's, out-of-core when needed."""
        st = self.resources[name]
        proj = ", ".join([f'"{f["name"]}"' for f in st.schema]) or "*"
        v = self.view(st.relation)
        # Read on the MAIN connection: relations and their ``.query()`` virtual-table
        # aliases (rename_fields_regex etc.) are bound to the connection that built
        # them, so a separate cursor can't resolve them. Instead the WRITE side
        # (_fill_table) runs on an independent cursor, so a structural op streaming
        # its output into a new table while this resource is still being read is
        # collision-free (only one active result per connection).
        cur = self.con.execute(f'SELECT {proj} FROM {v} ORDER BY __rownum__')
        cols = [d[0] for d in cur.description]
        while True:
            batch = cur.fetchmany(chunk)
            if not batch:
                break
            for r in batch:
                yield dict(zip(cols, r))

    def rows(self, name):
        """Ordered output rows (schema fields only) as a list, for equivalence
        checks. Eager wrapper over ``rows_iter``."""
        return list(self.rows_iter(name))

    def typed_rows_iter(self, name, chunk=10000):
        """Ordered output rows with values CAST to the tracked schema types, as a
        LAZY stream. The deferred-cast egress boundary: VARCHAR storage streams
        out of DuckDB in chunks (``rows_iter``) and the cast is applied on the fly
        via the shared ``casting`` module (frictionless ``schema_validator``, also
        lazy) -- so the whole load->dump path stays memory-bounded while producing
        byte-identical output to the dataflows lane."""
        from .casting import cast_rows

        st = self.resources[name]
        return cast_rows(self.rows_iter(name, chunk), st.schema)

    def typed_rows(self, name):
        """Ordered, cast output rows as a list. Eager wrapper over
        ``typed_rows_iter`` (used by the equivalence harness)."""
        return list(self.typed_rows_iter(name))

    def csv_bytes(self, name):
        """Byte-exact CSV of a resource: cast at output (deferred), then serialize
        with the production ``CustomCSVFormat``. See ``dump.py``."""
        from .dump import to_csv_bytes

        st = self.resources[name]
        return to_csv_bytes(st.schema, self.typed_rows(name))

    def to_csv(self, name, path):
        st = self.resources[name]
        proj = ", ".join([f'"{f["name"]}"' for f in st.schema]) or "*"
        v = self.view(st.relation)
        self.con.execute(
            f"COPY (SELECT {proj} FROM {v} ORDER BY __rownum__) "
            f"TO '{path}' (HEADER, FORMAT CSV)"
        )
