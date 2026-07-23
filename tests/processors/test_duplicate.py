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
# shadows the submodule attribute - reach the module (for RowFileBuffer and its
# module globals) explicitly.
duplicate_module = importlib.import_module(
    "bcodmo_frictionless.bcodmo_pipeline_processors.duplicate"
)
RowFileBuffer = duplicate_module.RowFileBuffer


def sample_data():
    return [{"col1": i, "name": f"row{i}"} for i in range(50)]


def dup_temp_files():
    # The scratch files RowFileBuffer creates; used to assert nothing leaks.
    return set(glob.glob(os.path.join(tempfile.gettempdir(), "bcodmo_duplicate_*.pickle")))


# ---------------------------------------------------------------------------
# Functional: single, multi, duplicate_to_end, empty list
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


# ---------------------------------------------------------------------------
# RowFileBuffer: reservation, truncation, read-back, cleanup
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_row_file_buffer_reserves_and_truncates(monkeypatch):
    real_fallocate = os.posix_fallocate
    calls = []

    def spy(fd, offset, length):
        calls.append((offset, length))
        return real_fallocate(fd, offset, length)

    monkeypatch.setattr(os, "posix_fallocate", spy)

    buf = RowFileBuffer()
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
def test_row_file_buffer_row_larger_than_chunk(monkeypatch):
    # A single row bigger than the reservation chunk must still be accommodated.
    monkeypatch.setattr(duplicate_module, "RESERVE_CHUNK_SIZE", 1024)
    buf = RowFileBuffer()
    try:
        big = {"blob": "y" * 50_000}
        buf.write(big)
        buf.done_writing()
        assert list(buf.read()) == [big]
    finally:
        buf.close()
    assert not os.path.exists(buf.path)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_row_file_buffer_finalizer_backstop():
    # An abandoned buffer (no close()) still has its temp file removed on GC.
    buf = RowFileBuffer()
    path = buf.path
    buf.write({"a": 1})
    assert os.path.exists(path)
    del buf
    gc.collect()
    assert not os.path.exists(path)


# ---------------------------------------------------------------------------
# Out of space: error + cleanup, via fallocate and via the statvfs fallback
# ---------------------------------------------------------------------------
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_duplicate_out_of_space(monkeypatch):
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
