import errno
import gc
import glob
import importlib
import os
import pickle
import shutil
import tempfile
import weakref

import pytest
from dataflows import Flow

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

# The package re-exports `duplicate` as the processor's flow() function, which
# shadows the submodule attribute - reach the module (for SpillingRowBuffer and
# its module globals) explicitly.
duplicate_module = importlib.import_module(
    "bcodmo_frictionless.bcodmo_pipeline_processors.duplicate"
)
SpillingRowBuffer = duplicate_module.SpillingRowBuffer


def sample_data():
    return [{"col1": i, "name": f"row{i}"} for i in range(50)]


def dup_temp_files():
    # The scratch files SpillingRowBuffer creates on spill; used to assert
    # nothing leaks (and that the in-memory path creates nothing at all).
    return set(glob.glob(os.path.join(tempfile.gettempdir(), "bcodmo_duplicate_*.pickle")))


# ---------------------------------------------------------------------------
# Functional: single, multi, duplicate_to_end, empty list
# (small samples stay in memory under the default threshold - no temp files)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_single():
    before = dup_temp_files()
    rows, dp, _ = Flow(
        sample_data(),
        duplicate({"source": "res_1", "target-name": "res_1_copy"}),
    ).results()
    resources = dp.descriptor["resources"]
    assert [r["name"] for r in resources] == ["res_1", "res_1_copy"]
    assert [r["path"] for r in resources] == ["res_1.csv", "res_1_copy.csv"]
    assert rows[0] == sample_data()
    assert rows[1] == sample_data()
    assert dup_temp_files() == before  # nothing left behind


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_multi():
    before = dup_temp_files()
    rows, dp, _ = Flow(
        sample_data(),
        duplicate(
            {"source": "res_1", "multi": True, "target_names": ["dupA", "dupB", "dupC"]}
        ),
    ).results()
    resources = dp.descriptor["resources"]
    assert [r["name"] for r in resources] == ["res_1", "dupA", "dupB", "dupC"]
    assert [r["path"] for r in resources] == [
        "res_1.csv",
        "dupA.csv",
        "dupB.csv",
        "dupC.csv",
    ]
    # every resource - source and all copies - holds identical rows
    for resource_rows in rows:
        assert resource_rows == sample_data()
    assert dup_temp_files() == before


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_multi_to_end():
    rows, dp, _ = Flow(
        sample_data(),
        duplicate(
            {
                "source": "res_1",
                "multi": True,
                "duplicate_to_end": True,
                "target_names": ["endA", "endB"],
            }
        ),
    ).results()
    # copies are appended after all other resources rather than following source
    assert [r["name"] for r in dp.descriptor["resources"]] == ["res_1", "endA", "endB"]
    for resource_rows in rows:
        assert resource_rows == sample_data()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_multi_empty_list():
    before = dup_temp_files()
    # No target names -> source passes through untouched, no buffer, no leak.
    rows, dp, _ = Flow(
        sample_data(),
        duplicate({"source": "res_1", "multi": True, "target_names": []}),
    ).results()
    assert [r["name"] for r in dp.descriptor["resources"]] == ["res_1"]
    assert rows[0] == sample_data()
    assert dup_temp_files() == before


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_spills_and_still_correct(monkeypatch):
    # Force spilling for the whole flow (tiny threshold): the duplicated copy
    # must still match the source exactly, and the scratch file must be gone.
    monkeypatch.setattr(duplicate_module, "_default_spill_threshold", lambda: 1024)
    before = dup_temp_files()
    rows, dp, _ = Flow(
        sample_data(),
        duplicate({"source": "res_1", "target-name": "res_1_copy"}),
    ).results()
    assert rows[0] == sample_data()
    assert rows[1] == sample_data()
    assert dup_temp_files() == before  # spill file cleaned up


# ---------------------------------------------------------------------------
# In-memory phase: small buffer never touches the disk
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_buffer_stays_in_memory_below_threshold(monkeypatch):
    before = dup_temp_files()
    reserved = []
    # If the code ever tried to reserve disk, we'd see a fallocate call.
    monkeypatch.setattr(os, "posix_fallocate", lambda *a: reserved.append(a))

    buf = SpillingRowBuffer(spill_threshold=10 * 1024 * 1024)  # 10 MiB, ample
    try:
        expected = [{"i": i, "s": "x" * 100} for i in range(200)]
        for row in expected:
            buf.write(row)
        buf.done_writing()

        assert buf._spilled is False
        assert buf.path is None          # never created a temp file
        assert not reserved              # never reserved disk
        assert dup_temp_files() == before
        # replayable purely from memory, more than once
        assert list(buf.read()) == expected
        assert list(buf.read()) == expected
    finally:
        buf.close()
    assert dup_temp_files() == before


# ---------------------------------------------------------------------------
# Spill transition: rows written before AND after the spill replay in order
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_buffer_spills_when_threshold_exceeded():
    before = dup_temp_files()
    buf = SpillingRowBuffer(spill_threshold=1024)  # 1 KiB -> spills partway
    try:
        expected = [{"i": i, "s": "x" * 100} for i in range(200)]
        for row in expected:
            buf.write(row)
        buf.done_writing()

        assert buf._spilled is True
        assert buf.path is not None and os.path.exists(buf.path)
        # surplus reservation released: file size == real bytes written
        assert os.path.getsize(buf.path) == buf._written
        # every row, from both the in-memory prefix and the on-disk tail
        assert list(buf.read()) == expected
    finally:
        buf.close()
    assert not os.path.exists(buf.path)
    assert dup_temp_files() == before


# ---------------------------------------------------------------------------
# SpillingRowBuffer disk path: reservation, truncation, read-back, cleanup
# (spill_threshold=0 forces the on-disk path from the first row)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_row_buffer_reserves_and_truncates(monkeypatch):
    real_fallocate = os.posix_fallocate
    calls = []

    def spy(fd, offset, length):
        calls.append((offset, length))
        return real_fallocate(fd, offset, length)

    monkeypatch.setattr(os, "posix_fallocate", spy)

    buf = SpillingRowBuffer(spill_threshold=0)
    try:
        expected = [{"i": i, "s": "x" * 100} for i in range(200)]
        for row in expected:
            buf.write(row)
        buf.done_writing()

        # space was genuinely reserved via fallocate, first chunk up front
        assert calls, "posix_fallocate was never called"
        assert calls[0] == (0, duplicate_module.RESERVE_CHUNK_SIZE)
        # the surplus reservation was released: file size == real bytes written
        assert os.path.getsize(buf.path) == buf._written
        # rows read back in order, unchanged
        assert list(buf.read()) == expected
    finally:
        buf.close()

    assert not os.path.exists(buf.path)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_row_buffer_row_larger_than_chunk(monkeypatch):
    # A single row bigger than the reservation chunk must still be accommodated.
    monkeypatch.setattr(duplicate_module, "RESERVE_CHUNK_SIZE", 1024)
    buf = SpillingRowBuffer(spill_threshold=0)
    try:
        big = {"blob": "y" * 50_000}
        buf.write(big)
        buf.done_writing()
        assert list(buf.read()) == [big]
    finally:
        buf.close()
    assert not os.path.exists(buf.path)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_row_buffer_finalizer_backstop():
    # An abandoned buffer (no close()) still has its spill file removed on GC.
    buf = SpillingRowBuffer(spill_threshold=0)
    buf.write({"a": 1})  # forces the spill file into existence
    path = buf.path
    assert path is not None and os.path.exists(path)
    del buf
    gc.collect()
    assert not os.path.exists(path)


# ---------------------------------------------------------------------------
# Shared (process-wide) memory budget
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_buffers_share_one_global_budget(monkeypatch):
    # Two buffers alive at once (two duplicate steps, or duplicate_to_end) must
    # not each hold the whole budget - the second spills because of what the
    # first is already holding, not because of its own rows.
    monkeypatch.setattr(duplicate_module, "_default_spill_threshold", lambda: 20_000)
    row = {"s": "x" * 200}
    first = SpillingRowBuffer()
    second = SpillingRowBuffer()
    try:
        for _ in range(40):
            first.write(row)
        assert first._spilled is False  # still under the shared budget alone
        for _ in range(40):
            second.write(row)
        assert second._spilled is True
        # ...and it spilled well before its own rows reached the budget.
        assert second._written < 20_000
        second.done_writing()
        assert list(second.read()) == [row] * 40
        assert list(first.read()) == [row] * 40
    finally:
        first.close()
        second.close()
    assert SpillingRowBuffer._in_memory_bytes == 0


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_closing_returns_memory_to_the_shared_budget():
    before = SpillingRowBuffer._in_memory_bytes
    buf = SpillingRowBuffer(spill_threshold=10 * 1024 * 1024)
    buf.write({"a": "x" * 1000})
    assert SpillingRowBuffer._in_memory_bytes > before
    buf.close()
    assert SpillingRowBuffer._in_memory_bytes == before


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_abandoned_buffer_returns_memory_to_the_shared_budget():
    # No close(): the charge is still handed back when the buffer is collected,
    # otherwise a failed pipeline would permanently shrink the budget.
    before = SpillingRowBuffer._in_memory_bytes
    buf = SpillingRowBuffer(spill_threshold=10 * 1024 * 1024)
    buf.write({"a": "x" * 1000})
    assert SpillingRowBuffer._in_memory_bytes > before
    del buf
    gc.collect()
    assert SpillingRowBuffer._in_memory_bytes == before


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_memory_accounting_includes_object_overhead():
    # Narrow rows cost substantially more than their payload (bytes header plus
    # the list slot); counting only the payload spills far too late.
    rows = [{"i": i} for i in range(500)]
    payload = sum(
        len(pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)) for row in rows
    )
    buf = SpillingRowBuffer(spill_threshold=10 * 1024 * 1024)
    try:
        for row in rows:
            buf.write(row)
        assert buf._mem_bytes > payload * 1.3
    finally:
        buf.close()


# ---------------------------------------------------------------------------
# Byte-budget override (DUPLICATE_SPILL_THRESHOLD)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_default_threshold_env_override(monkeypatch):
    monkeypatch.setenv("DUPLICATE_SPILL_THRESHOLD", "12345")
    assert duplicate_module._default_spill_threshold() == 12345


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_no_threshold_env_means_pressure_driven(monkeypatch):
    monkeypatch.delenv("DUPLICATE_SPILL_THRESHOLD", raising=False)
    # None = no byte budget at all; spilling is decided by memory pressure.
    assert duplicate_module._default_spill_threshold() is None


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_malformed_threshold_env_ignored(monkeypatch):
    monkeypatch.setenv("DUPLICATE_SPILL_THRESHOLD", "not-a-number")
    assert duplicate_module._default_spill_threshold() is None


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_threshold_env_spills_deterministically(monkeypatch):
    # What the deterministic (test / pinned-operator) path has to guarantee: the
    # budget is process-global and the pressure monitor is not consulted at all.
    monkeypatch.setenv("DUPLICATE_SPILL_THRESHOLD", "20000")
    monkeypatch.setattr(
        duplicate_module,
        "_memory_pressure",
        lambda force=False: pytest.fail("pressure consulted despite a byte budget"),
    )
    row = {"s": "x" * 200}
    buf = SpillingRowBuffer()
    try:
        for _ in range(200):
            buf.write(row)
        buf.done_writing()
        assert buf._spilled is True
        assert list(buf.read()) == [row] * 200
    finally:
        buf.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_full_flow_spills_via_threshold_env(monkeypatch):
    monkeypatch.setenv("DUPLICATE_SPILL_THRESHOLD", "512")
    before = dup_temp_files()
    rows, dp, _ = Flow(
        sample_data(),
        duplicate({"source": "res_1", "target-name": "res_1_copy"}),
    ).results()
    assert rows[0] == sample_data()
    assert rows[1] == sample_data()
    assert dup_temp_files() == before


# ---------------------------------------------------------------------------
# Pressure monitor: headroom margin, victim selection, sample cache
# (the raw readings are faked, so none of this depends on the real cgroup)
# ---------------------------------------------------------------------------
MIB = 1024 * 1024


@pytest.fixture
def pressure(monkeypatch):
    """Drive the pressure monitor from a fake container reading.

    `pressure.headroom` is what the monitor will next measure as free; setting
    it low puts the process under pressure. The sample cache is cleared so each
    test starts from a known state.
    """

    class Fake:
        def __init__(self):
            self.limit = 8192 * MIB
            self.headroom = 4096 * MIB
            self.samples = 0

        def working_set(self):
            self.samples += 1
            return self.limit - self.headroom

    fake = Fake()
    # Isolate the victim registry from any buffer another test left alive.
    monkeypatch.setattr(SpillingRowBuffer, "_live", weakref.WeakSet())
    monkeypatch.delenv("DUPLICATE_SPILL_THRESHOLD", raising=False)
    monkeypatch.delenv("DUPLICATE_SPILL_HEADROOM", raising=False)
    monkeypatch.setattr(duplicate_module, "_pressure_sample", None)
    monkeypatch.setattr(
        duplicate_module, "_container_memory_limit_bytes", lambda: fake.limit
    )
    monkeypatch.setattr(duplicate_module, "_cgroup_v2_working_set", fake.working_set)
    monkeypatch.setattr(duplicate_module, "_cgroup_v1_working_set", lambda: None)
    monkeypatch.setattr(
        duplicate_module,
        "_mem_available_bytes",
        lambda: pytest.fail("fell through to the bare-host reading"),
    )
    # Sampling is unconditional in these tests; the 0.5s cache has its own.
    monkeypatch.setattr(duplicate_module, "PRESSURE_SAMPLE_INTERVAL", 0)
    yield fake
    duplicate_module._pressure_sample = None


def fill(buf, n_rows, row_bytes=1000):
    row = {"s": "x" * row_bytes}
    for _ in range(n_rows):
        buf.write(row)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_no_pressure_never_spills(pressure, monkeypatch):
    # Plenty of headroom: nothing caps the buffer, so it grows well past the
    # victim threshold without ever touching the disk.
    monkeypatch.setattr(duplicate_module, "MIN_SPILL_THRESHOLD", 1024)
    pressure.headroom = 4096 * MIB
    before = dup_temp_files()
    buf = SpillingRowBuffer()
    try:
        assert SpillingRowBuffer._budget_bytes is None  # no reserved budget
        assert buf._spill_threshold is None
        fill(buf, 2000)
        assert buf._spilled is False
        assert buf.path is None
        assert buf._mem_bytes > 1024 * 1024
        assert dup_temp_files() == before
    finally:
        buf.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_pressure_spills_largest_buffer_first(pressure, monkeypatch):
    monkeypatch.setattr(duplicate_module, "MIN_SPILL_THRESHOLD", 4096)
    big = SpillingRowBuffer()
    small = SpillingRowBuffer()
    try:
        fill(big, 400)
        fill(small, 20)
        assert big._mem_bytes > small._mem_bytes >= 4096
        assert (big._spilled, small._spilled) == (False, False)

        # Squeeze: the next write finds headroom under the margin. Spilling the
        # big buffer is enough, so the small one must survive.
        freed_by_big = big._mem_bytes

        def recover():
            pressure.headroom = 4096 * MIB
            return pressure.limit - pressure.headroom

        pressure.headroom = 1 * MIB
        monkeypatch.setattr(
            duplicate_module,
            "_cgroup_v2_working_set",
            lambda: recover() if big._spilled else pressure.working_set(),
        )
        small.write({"s": "y" * 1000})

        assert big._spilled is True
        assert small._spilled is False
        assert big._written >= freed_by_big * 0.5  # everything it held is on disk
        big.done_writing()
        assert list(big.read()) == [{"s": "x" * 1000}] * 400
    finally:
        big.close()
        small.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_pressure_spills_every_eligible_buffer_when_needed(pressure, monkeypatch):
    # Pressure that never lets up: both buffers spill, largest first, and then
    # relief gives up rather than looping.
    monkeypatch.setattr(duplicate_module, "MIN_SPILL_THRESHOLD", 4096)
    big = SpillingRowBuffer()
    small = SpillingRowBuffer()
    try:
        fill(big, 400)
        fill(small, 20)
        order = []
        real_spill = duplicate_module.SpillingRowBuffer._spill

        def spy(self):
            order.append(self)
            return real_spill(self)

        monkeypatch.setattr(duplicate_module.SpillingRowBuffer, "_spill", spy)
        pressure.headroom = 1 * MIB
        small.write({"s": "y" * 1000})
        assert order == [big, small]
    finally:
        big.close()
        small.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_buffer_below_min_spill_threshold_is_never_a_victim(pressure):
    # 32MiB default MIN_SPILL_THRESHOLD, tiny buffer: spilling it would cost a
    # file and a reservation and buy back nothing.
    tiny = SpillingRowBuffer()
    try:
        fill(tiny, 10)
        assert tiny._mem_bytes < duplicate_module.MIN_SPILL_THRESHOLD
        pressure.headroom = 1 * MIB
        tiny.write({"s": "z" * 1000})
        assert tiny._spilled is False
        assert tiny.path is None
    finally:
        tiny.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_spill_headroom_env_override(pressure, monkeypatch):
    monkeypatch.setattr(duplicate_module, "MIN_SPILL_THRESHOLD", 4096)
    # 2 GiB of headroom is above the default margin, so nothing spills...
    pressure.headroom = 2048 * MIB
    buf = SpillingRowBuffer()
    try:
        fill(buf, 200)
        assert buf._spilled is False
        # ...but not if the operator demands 4 GiB of slack.
        monkeypatch.setenv("DUPLICATE_SPILL_HEADROOM", str(4096 * MIB))
        buf.write({"s": "x" * 1000})
        assert buf._spilled is True
    finally:
        buf.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_spill_headroom_margin_defaults(monkeypatch):
    monkeypatch.delenv("DUPLICATE_SPILL_HEADROOM", raising=False)
    monkeypatch.setattr(
        duplicate_module, "_container_memory_limit_bytes", lambda: 2048 * MIB
    )
    # Small worker: the floor wins.
    assert duplicate_module._spill_headroom_margin() == 512 * MIB
    # Big worker: the fraction wins, so it keeps proportionally more slack.
    monkeypatch.setattr(
        duplicate_module, "_container_memory_limit_bytes", lambda: 30720 * MIB
    )
    assert duplicate_module._spill_headroom_margin() == int(30720 * MIB * 0.10)
    # No detectable limit at all: still the floor, never zero.
    monkeypatch.setattr(duplicate_module, "_container_memory_limit_bytes", lambda: None)
    assert duplicate_module._spill_headroom_margin() == 512 * MIB


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_malformed_headroom_env_ignored(monkeypatch):
    monkeypatch.setattr(
        duplicate_module, "_container_memory_limit_bytes", lambda: 2048 * MIB
    )
    for bad in ("", "lots", "3.5", "-1x"):
        monkeypatch.setenv("DUPLICATE_SPILL_HEADROOM", bad)
        assert duplicate_module._spill_headroom_margin() == 512 * MIB


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_pressure_reading_is_cached(pressure, monkeypatch):
    # The per-row cost must be a comparison, not three file reads.
    monkeypatch.setattr(duplicate_module, "PRESSURE_SAMPLE_INTERVAL", 30)
    buf = SpillingRowBuffer()
    try:
        fill(buf, 500)
        assert pressure.samples == 1
        # ...and a forced sample (what a spill does) bypasses the cache.
        duplicate_module._memory_pressure(force=True)
        assert pressure.samples == 2
    finally:
        buf.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_pressure_cache_expires(pressure, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(duplicate_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(duplicate_module, "PRESSURE_SAMPLE_INTERVAL", 0.5)
    buf = SpillingRowBuffer()
    try:
        buf.write({"a": 1})
        assert pressure.samples == 1
        clock[0] += 0.4
        buf.write({"a": 2})
        assert pressure.samples == 1
        clock[0] += 0.2
        buf.write({"a": 3})
        assert pressure.samples == 2
    finally:
        buf.close()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_unmeasurable_memory_never_spills(monkeypatch):
    # Nothing readable anywhere: no pressure can be proven, so stay in RAM
    # rather than spilling every buffer on a machine we can't measure.
    monkeypatch.delenv("DUPLICATE_SPILL_THRESHOLD", raising=False)
    monkeypatch.setattr(duplicate_module, "_pressure_sample", None)
    monkeypatch.setattr(duplicate_module, "_cgroup_v2_working_set", lambda: None)
    monkeypatch.setattr(duplicate_module, "_cgroup_v1_working_set", lambda: None)
    monkeypatch.setattr(duplicate_module, "_mem_available_bytes", lambda: None)
    under_pressure, headroom, _ = duplicate_module._memory_pressure(force=True)
    assert headroom is None
    assert under_pressure is False
    duplicate_module._pressure_sample = None


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_working_set_excludes_inactive_page_cache(monkeypatch):
    # Reclaimable page cache is not pressure; counting it would spill constantly
    # on any worker that has read a large file.
    monkeypatch.setattr(duplicate_module, "_read_int_file", lambda path: 1_000_000)
    monkeypatch.setattr(
        duplicate_module, "_read_stat_field", lambda path, field: 300_000
    )
    assert duplicate_module._cgroup_v2_working_set() == 700_000
    assert duplicate_module._cgroup_v1_working_set() == 700_000
    # A stat file without the field at all: fall back to the raw usage.
    monkeypatch.setattr(duplicate_module, "_read_stat_field", lambda path, field: None)
    assert duplicate_module._cgroup_v2_working_set() == 1_000_000


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_read_stat_field_parses_cgroup_file(tmp_path):
    stat = tmp_path / "memory.stat"
    stat.write_text("anon 400000\ninactive_file 300000\nslab 1234\n")
    assert duplicate_module._read_stat_field(str(stat), "inactive_file") == 300000
    assert duplicate_module._read_stat_field(str(stat), "nope") is None
    assert duplicate_module._read_stat_field(str(tmp_path / "missing"), "anon") is None


# ---------------------------------------------------------------------------
# Out of space: error + cleanup, via fallocate and via the statvfs fallback
# (force spilling so the disk path is exercised)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_out_of_space(monkeypatch):
    monkeypatch.setattr(duplicate_module, "_default_spill_threshold", lambda: 0)

    def full(fd, offset, length):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(os, "posix_fallocate", full)

    before = dup_temp_files()
    with pytest.raises(Exception) as exc_info:
        Flow(
            sample_data(),
            duplicate({"source": "res_1", "target-name": "copy"}),
        ).results()
    assert "Not enough disk space" in str(exc_info.value)
    # the partial scratch file was cleaned up
    assert dup_temp_files() == before


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_fallocate_unsupported_fallback(monkeypatch):
    # Filesystem without fallocate -> best-effort statvfs check still fails fast.
    monkeypatch.setattr(duplicate_module, "_default_spill_threshold", lambda: 0)

    def unsupported(fd, offset, length):
        raise OSError(errno.EOPNOTSUPP, "operation not supported")

    class FakeUsage:
        free = 1024  # only 1KB free

    monkeypatch.setattr(os, "posix_fallocate", unsupported)
    monkeypatch.setattr(shutil, "disk_usage", lambda path: FakeUsage())

    before = dup_temp_files()
    with pytest.raises(Exception) as exc_info:
        Flow(
            sample_data(),
            duplicate({"source": "res_1", "target-name": "copy"}),
        ).results()
    assert "Not enough disk space" in str(exc_info.value)
    assert dup_temp_files() == before
