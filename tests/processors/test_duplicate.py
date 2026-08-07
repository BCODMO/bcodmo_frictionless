import errno
import gc
import glob
import importlib
import os
import shutil
import tempfile

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
# Threshold detection
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_default_threshold_env_override(monkeypatch):
    monkeypatch.setenv("DUPLICATE_SPILL_THRESHOLD", "12345")
    assert duplicate_module._default_spill_threshold() == 12345


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_default_threshold_is_positive_and_clamped(monkeypatch):
    monkeypatch.delenv("DUPLICATE_SPILL_THRESHOLD", raising=False)
    monkeypatch.delenv("DUPLICATE_SPILL_MEMORY_FRACTION", raising=False)
    t = duplicate_module._default_spill_threshold()
    assert isinstance(t, int)
    assert duplicate_module.MIN_SPILL_THRESHOLD <= t <= duplicate_module.MAX_SPILL_THRESHOLD


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
