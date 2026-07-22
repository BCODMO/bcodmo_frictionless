"""
Never-OOM validation for the DuckDB backend.

The backend's core promise is that a ``load -> transform -> dump`` pipeline stays
memory-bounded no matter how big the input -- storage is VARCHAR in DuckDB (which
spills to ``temp_directory`` under ``memory_limit``), ingest appends in bounded
batches, and egress streams typed rows in bounded chunks. Nothing materializes the
whole resource in Python.

The load descriptor for these tests is a wide-ish CSV of ``DUCKDB_OOM_ROWS`` rows
(default 400k; override the env var to stress harder). Load runs through the LIVE
bcodmo loader (tabulator), so this also exercises the real streaming ingest path.

The load parser itself is the slow part (fidelity, not memory), so keep the row
count modest unless deliberately stress-testing.

MEMORY MEASUREMENT
------------------
RSS is sampled from ``/proc/self/statm`` in a background thread. Absolute RSS is
noisy (allocator retention, interpreter, DuckDB buffers), so the load-bearing
assertion is RELATIVE: streaming egress must peak well below the same data fully
materialized into a Python list -- the exact anti-pattern the lazy ``typed_rows_iter``
replaced. That comparison cancels environmental baseline noise.
"""

import csv
import os
import threading
import time

import pytest

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.engine import Engine

ROWS = int(os.environ.get("DUCKDB_OOM_ROWS", "400000"))
_PAGE = os.sysconf("SC_PAGE_SIZE")

# RSS-peak assertions only carry signal at scale: below this the full-materialization
# yardstick is a few MB and process noise dominates, so a smaller DUCKDB_OOM_ROWS
# (used to run the suite fast) checks correctness only, not the memory ratio.
_MEM_MEANINGFUL = ROWS >= 200_000


def _rss_mb():
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * _PAGE / 1e6


class _PeakSampler:
    """Context manager: tracks peak RSS delta (MB) above entry baseline while its
    body runs, sampling in a background thread."""

    def __enter__(self):
        self._base = _rss_mb()
        self.peak = 0.0
        self._stop = False
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def _run(self):
        while not self._stop:
            self.peak = max(self.peak, _rss_mb() - self._base)
            time.sleep(0.005)

    def __exit__(self, *exc):
        self._stop = True
        self._t.join()


def _make_csv(path, n):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["col1", "col2", "col3", "col4"])
        for i in range(n):
            w.writerow([f"row{i}", i % 1000, i * 1.5, "12/29/19"])


@pytest.fixture(scope="module")
def big_csv(tmp_path_factory):
    p = tmp_path_factory.mktemp("oom") / "big.csv"
    _make_csv(str(p), ROWS)
    return str(p)


def _loaded_engine(big_csv, spill_dir, memory_limit="256MB"):
    eng = Engine(memory_limit=memory_limit, temp_directory=str(spill_dir))
    eng.run([
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": big_csv, "name": "res", "format": "csv"}},
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"types": {"col2": {"type": "integer"}, "col3": {"type": "number"}}}},
    ])
    return eng


def test_streaming_egress_is_memory_bounded(big_csv, tmp_path):
    """Streaming ``typed_rows_iter`` must peak far below materializing the whole
    resource into a Python list -- proving egress is chunked, not materialized."""
    eng = _loaded_engine(big_csv, tmp_path)

    # Lazy stream: only a bounded chunk of typed rows is ever alive in Python.
    with _PeakSampler() as streamed:
        count = sum(1 for _ in eng.typed_rows_iter("res"))

    # Eager materialization of the SAME data: the whole typed list in Python. This
    # is the anti-pattern the lazy path replaced -- used here only as a yardstick.
    with _PeakSampler() as materialized:
        full = eng.typed_rows("res")

    assert count == ROWS
    assert len(full) == ROWS
    if _MEM_MEANINGFUL:
        # The whole point: streaming holds O(chunk) rows, materializing holds O(N).
        assert streamed.peak < materialized.peak * 0.6, (
            f"streaming peak {streamed.peak:.0f}MB not clearly below "
            f"materialized peak {materialized.peak:.0f}MB -- egress may be materializing"
        )
        # And the streamed peak must be small in absolute terms (chunk-bounded), i.e.
        # it must NOT scale with the ~ROWS*400B a full Python list would occupy.
        assert streamed.peak < max(120.0, materialized.peak * 0.5), streamed.peak


def test_lazy_egress_equals_eager(big_csv, tmp_path):
    """The lazy stream must yield byte-for-byte the same typed rows as the eager
    path at scale -- so the memory win costs nothing in correctness. (Content
    correctness vs the dataflows lane is pinned by the differential suite; this
    extends that guarantee to the streaming path.)"""
    eng = _loaded_engine(big_csv, tmp_path)
    eager = eng.typed_rows("res")
    n = 0
    for streamed_row, eager_row in zip(eng.typed_rows_iter("res"), eager):
        assert streamed_row == eager_row
        n += 1
    assert n == len(eager) == ROWS


def test_completes_under_tight_memory_limit(big_csv, tmp_path):
    """The pipeline must run to completion (correct row count, ordered) under a
    memory_limit far smaller than a full Python materialization of the dataset,
    with temp_directory available for DuckDB to spill to if its working set
    exceeds the limit. Bump DUCKDB_OOM_ROWS to force actual spilling."""
    eng = _loaded_engine(big_csv, tmp_path, memory_limit="100MB")
    it = eng.typed_rows_iter("res")
    first = next(it)
    count = 1 + sum(1 for _ in it)
    assert count == ROWS
    assert first["col1"] == "row0" and first["col2"] == 0  # __rownum__ order held


# -- leaf UDF processor -----------------------------------------------------

def test_large_udf_leaf_is_memory_bounded(big_csv, tmp_path):
    """A leaf UDF processor (round_fields -> the streaming udf_map path) over a
    large resource must stay memory-bounded: input streams from DuckDB in chunks,
    process_rows runs as one lazy pass, and the output streams into a fresh table
    -- nothing materializes the resource in Python."""
    eng = _loaded_engine(big_csv, tmp_path)  # col3 -> number

    with _PeakSampler() as udf:
        eng.run([{"run": "bcodmo_pipeline_processors.round_fields",
                  "parameters": {"fields": [{"name": "col3", "digits": 1}]}}])

    rows = eng.typed_rows_iter("res")
    first = next(rows)
    count = 1 + sum(1 for _ in rows)
    assert count == ROWS
    assert first["col3"] == 0  # row0: col3 = 0*1.5 = 0.0 rounded to 1 digit
    if _MEM_MEANINGFUL:
        assert udf.peak < max(160.0, ROWS * 400 / 1e6 * 0.5), udf.peak


# -- structural op: large sort ---------------------------------------------

def test_large_sort_is_memory_bounded(big_csv, tmp_path):
    """A sort over a large resource must stay memory-bounded: the live _sorter is
    an external merge sort (KVFile), fed a lazy typed stream that carries each
    row's original VARCHAR alongside its sort key, and the sorted output streams
    into a fresh table. Neither the input nor the reordered output materializes,
    and the key/VARCHAR pairing holds ~1 row (not the resource)."""
    eng = _loaded_engine(big_csv, tmp_path)  # col2 -> integer, col3 -> number

    with _PeakSampler() as sorted_:
        eng.run([{"run": "sort", "parameters": {"sort-by": "{col2}"}}])

    rows = eng.typed_rows_iter("res")
    first = next(rows)
    count = 1 + sum(1 for _ in rows)
    assert count == ROWS
    assert first["col2"] == 0  # ascending: smallest col2 first (values 0..999)
    if _MEM_MEANINGFUL:
        assert sorted_.peak < max(160.0, ROWS * 400 / 1e6 * 0.5), sorted_.peak


# -- structural op: large join ---------------------------------------------

_KEYS = 1000  # distinct join keys in the source lookup


@pytest.fixture(scope="module")
def join_csvs(tmp_path_factory):
    d = tmp_path_factory.mktemp("oomjoin")
    target = d / "target.csv"
    source = d / "source.csv"
    with open(target, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["k", "tval"])
        for i in range(ROWS):
            w.writerow([i % _KEYS, i])
    with open(source, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["k", "sval"])
        for k in range(_KEYS):
            w.writerow([k, k * 10])
    return str(target), str(source)


def test_large_join_is_memory_bounded(join_csvs, tmp_path):
    """A half-outer join over a large target must stay memory-bounded: join_aux
    keeps its index in a disk-backed KVFile, the target is read as a lazy stream
    (independent cursor), and the joined output streams into a fresh table -- so
    neither the target nor the ~ROWS-row output is ever materialized in Python.
    This is the read-while-write path (reading the target while writing its
    replacement), which a naive materializing join would blow up on."""
    target, source = join_csvs
    eng = Engine(memory_limit="200MB", temp_directory=str(tmp_path))
    eng.run([
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": source, "name": "source", "format": "csv"}},
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": target, "name": "target", "format": "csv"}},
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"resources": "source", "types": {"sval": {"type": "number"}}}},
    ])

    with _PeakSampler() as joined:
        eng.run([{"run": "join", "parameters": {
            "source": {"name": "source", "key": "{k}", "delete": True},
            "target": {"name": "target", "key": "{k}"},
            "fields": {"ssum": {"name": "sval", "aggregate": "sum"}},
            "mode": "half-outer",
        }}])

    # Correctness: every target row survives (half-outer), source was deleted, and
    # each row's ssum is the source value for its key (k -> k*10).
    assert "source" not in eng.resources
    rows = eng.typed_rows_iter("target")
    first = next(rows)
    count = 1 + sum(1 for _ in rows)
    assert count == ROWS
    assert first["ssum"] == 0  # k=0 -> sval 0

    # A materializing join would hold target + output (~2*ROWS rows) in Python;
    # streaming holds only KVFile (disk) + fetch/insert chunks. Bounded well below
    # what full materialization (~2*ROWS*~400B) would cost.
    if _MEM_MEANINGFUL:
        assert joined.peak < max(160.0, ROWS * 400 / 1e6 * 0.5), joined.peak
