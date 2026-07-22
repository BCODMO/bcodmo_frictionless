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
(``udf_map``) STREAMS in bounded chunks (cast-in -> process_rows -> format-out ->
insert), so leaf row processors are never-OOM like load/dump/join/sort.
"""

import itertools
import os
from collections import deque

import duckdb

from .processor import REGISTRY
from .casting import cast_rows, format_out_iter


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
    # Default to SINGLE-THREADED DuckDB. Rationale: a bcodmo pipeline ends in
    # ``dump_to_s3``, whose ``S3Dumper`` uses a billiard ``Pool`` that ``os.fork()``s.
    # Forking a multi-threaded process is a classic deadlock trap -- the child can
    # hang on a lock a parent thread held at fork time. DuckDB's worker-thread pool
    # (``threads>1``) makes that intermittent hang real (observed in the test suite
    # when a dump forks after engine work). Single-threaded DuckDB removes those
    # worker threads, bringing fork-safety to parity with the dataflows lane (which
    # forks the same dumper with no DuckDB threads). Override via env
    # ``LAMINAR_DUCKDB_THREADS`` only in environments with no forking dumper.
    def __init__(self, threads=None, memory_limit=None, temp_directory=None):
        if threads is None:
            threads = int(os.environ.get("LAMINAR_DUCKDB_THREADS", "1"))
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
        # Package-level frictionless descriptor metadata (title/name/custom keys),
        # as ``update_package`` sets it; folded into the datapackage.json at dump.
        # Excludes ``resources`` (those come from the per-resource state).
        self.package_descriptor: dict = {}
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

        ``__rownum__`` is assigned by enumeration. See ``_batch_insert`` for the
        insert mechanics."""
        coldefs = ", ".join([f'"{c}" VARCHAR' for c in cols])
        self._wc.execute(
            f'CREATE OR REPLACE TABLE "{tbl}" ("__rownum__" BIGINT, {coldefs})'
        )
        param_rows = (
            [i] + [_s(r.get(c)) for c in cols] for i, r in enumerate(rows)
        )
        self._batch_insert(tbl, len(cols) + 1, param_rows, batch)

    def _batch_insert(self, tbl, ncols, param_rows, batch):
        """Insert an iterable of param-lists (each of length ``ncols``, first value
        = ``__rownum__``) into ``tbl`` via one multi-row ``INSERT ... VALUES`` per
        batch -- a single prepared statement binding ``batch*ncols`` params, ~18x
        faster than per-row executemany and NULL-safe (a ``?`` bound to None becomes
        SQL NULL). Writes run on the dedicated write cursor ``self._wc`` so they
        don't clobber an in-flight lazy read on the main connection (the read-while-
        write case). Only ``batch`` param-lists are ever buffered in Python."""
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
        for pr in param_rows:
            buf.append(pr)
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

    # -- UDF-default path (STREAMING 1:1 row map) --------------------------
    def udf_map(self, relation, proc, params, schema=None, out_schema=None, chunk=10000):
        """Apply a single-resource 1:1 row processor (``process_rows``) to a
        relation WITHOUT materializing it -- the never-OOM leaf path.

        Input rows stream from the relation in bounded chunks (main connection),
        are cast VARCHAR->typed lazily (so the processor sees the same typed values
        the dataflows lane feeds it), pass through ``process_rows`` as ONE
        continuous lazy call (any internal row-order/state therefore spans the whole
        resource, matching dataflows), are formatted back to VARCHAR-safe storage
        lazily, and stream into a fresh table (write cursor). Each output row's
        hidden ``__rownum__`` is carried 1:1 from its input via a small deque
        (order_effect='keep'). Only ``chunk`` rows are ever in flight.

        The default path is for 1:1 row maps; a filtering/expanding processor must
        provide a native ``to_sql`` instead. The deque pairing enforces that: a
        mismatch between input and output counts raises."""
        v = self.view(relation)
        cur = self.con.execute(f"SELECT * FROM {v}")
        in_cols = [d[0] for d in cur.description]
        rownum_q = deque()

        def _input():
            while True:
                batch = cur.fetchmany(chunk)
                if not batch:
                    break
                for r in batch:
                    d = dict(zip(in_cols, r))
                    rownum_q.append(d.pop("__rownum__", None))
                    yield d

        src = cast_rows(_input(), schema) if schema else _input()
        out = proc.process_rows(src, params, schema)
        formatted = format_out_iter(out, out_schema) if out_schema else out

        def _with_rownum():
            for row in formatted:
                if not rownum_q:
                    raise AssertionError(
                        f"{proc.name}: UDF-default produced MORE rows than it "
                        f"consumed; a filtering/expanding processor needs a native "
                        f"to_sql."
                    )
                r = dict(row)
                r["__rownum__"] = rownum_q.popleft()
                yield r

        rel = self._stream_udf_table(_with_rownum(), out_schema, chunk)
        if rownum_q:
            raise AssertionError(
                f"{proc.name}: UDF-default consumed MORE rows than it produced "
                f"({len(rownum_q)} unmatched); a filtering processor needs a native "
                f"to_sql."
            )
        return rel

    def _stream_udf_table(self, rows, out_schema, chunk):
        """Stream ``rows`` (each carrying its own ``__rownum__`` + data cols) into a
        fresh ``_udf*`` table, PRESERVING each row's ``__rownum__`` (unlike
        ``_fill_table``, which enumerates). Columns come from ``out_schema`` when
        given (so the table has the right columns even for 0 rows); otherwise they
        are derived from the first output row (the schemaless unit-test path)."""
        it = iter(rows)
        if out_schema is not None:
            data_cols = [f["name"] for f in out_schema]
        else:
            try:
                first = next(it)
            except StopIteration:
                data_cols = []
            else:
                data_cols = [c for c in first.keys() if c != "__rownum__"]
                it = itertools.chain([first], it)
        tbl = f"_udf{self._next()}"
        coldefs = ", ".join(
            ['"__rownum__" BIGINT'] + [f'"{c}" VARCHAR' for c in data_cols]
        )
        self._wc.execute(f'CREATE OR REPLACE TABLE "{tbl}" ({coldefs})')
        param_rows = (
            [r.get("__rownum__")] + [_s(r.get(c)) for c in data_cols] for r in it
        )
        self._batch_insert(tbl, len(data_cols) + 1, param_rows, chunk)
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
