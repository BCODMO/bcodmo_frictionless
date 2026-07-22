"""
Tests for DuckDB-lane progress reporting (``duckdb_backend.progress``) and its wiring
through the engine + runner.

Progress is best-effort telemetry, so the contract under test is: the right redis
keys get the right values (current step + phase, ingest row counts, memory/spill
snapshot), it is a complete no-op without a cache_id/redis, and the background
sampler starts/stops cleanly and survives the fork hooks. A dict-backed fake redis
stands in for a real one (the reporter only needs set/get/sadd/sscan_iter).
"""

import csv
import json
import os

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend import progress, runner
from bcodmo_frictionless.duckdb_backend.engine import Engine, _parse_bytes


class FakeRedis:
    """Minimal in-memory redis stand-in: strings + sets, enough for the reporter and
    the pipeline.py reader. Values are stored as ``bytes`` like real redis."""

    def __init__(self):
        self.kv = {}
        self.sets = {}

    def set(self, key, value, ex=None):
        if not isinstance(value, (bytes, bytearray)):
            value = str(value).encode()
        self.kv[key] = bytes(value)

    def get(self, key):
        return self.kv.get(key)

    def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(values)

    def sscan_iter(self, key):
        for member in self.sets.get(key, set()):
            yield member

    def delete(self, key):
        self.kv.pop(key, None)
        self.sets.pop(key, None)


def _make_csv(path, n=25000):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["a", "b"])
        for i in range(n):
            w.writerow([f"r{i}", i * 1.5])
    return str(path)


# -- _parse_bytes ----------------------------------------------------------
def test_parse_bytes():
    assert _parse_bytes("256MB") == 256_000_000
    assert _parse_bytes("1.5 GiB") == int(1.5 * 1024 ** 3)
    assert _parse_bytes("488.2 MiB") == int(488.2 * 1024 ** 2)
    assert _parse_bytes("0 bytes") == 0
    assert _parse_bytes(None) == 0
    assert _parse_bytes(1024) == 1024
    assert _parse_bytes("garbage") == 0


# -- reporter --------------------------------------------------------------
def test_reporter_writes_step_rows_memory():
    fake = FakeRedis()
    rep = progress.ProgressReporter("c1", fake)

    rep.step(2, 5, "bcodmo_pipeline_processors.round_fields", "running")
    step = json.loads(fake.get(progress.step_key("c1")).decode())
    assert step == {
        "index": 2, "total": 5,
        "run": "bcodmo_pipeline_processors.round_fields",
        "phase": "running", "label": "round_fields",
    }

    rep.rows("res", 12345)
    assert int(fake.get(progress.rows_key("c1", "res")).decode()) == 12345

    rep.write_memory({"memory_used": 10, "memory_limit": 100})
    assert json.loads(fake.get(progress.mem_key("c1")).decode())["memory_used"] == 10


def test_reporter_is_noop_without_redis():
    rep = progress.ProgressReporter("c1", None)
    # None of these should raise.
    rep.step(0, 1, "load", "ingesting")
    rep.rows("res", 5)
    rep.write_memory({"memory_used": 1})


def test_make_reporter_none_without_cache_id():
    assert progress.make_reporter(None) is None
    assert progress.make_reporter("") is None


def test_make_reporter_none_without_redis(monkeypatch):
    # No REDIS_PROGRESS_URL -> get_redis_connection returns None -> no reporter.
    monkeypatch.delenv("REDIS_PROGRESS_URL", raising=False)
    assert progress.make_reporter("c1") is None


def test_all_keys():
    keys = progress.all_keys("c1", ["res", "other"])
    assert progress.step_key("c1") in keys
    assert progress.mem_key("c1") in keys
    assert progress.rows_key("c1", "res") in keys
    assert progress.rows_key("c1", "other") in keys


# -- engine.memory_stats ---------------------------------------------------
def test_memory_stats_shape():
    eng = Engine(memory_limit="256MB")
    st = eng.memory_stats()
    assert set(st) >= {
        "memory_used", "memory_limit", "temp_used", "temp_files", "by_tag", "ts"
    }
    assert st["memory_limit"] == 256_000_000
    assert isinstance(st["memory_used"], int)
    assert isinstance(st["by_tag"], list)


# -- end-to-end through the runner -----------------------------------------
def test_execute_reports_steps_and_rows(tmp_path):
    src = _make_csv(tmp_path / "in.csv")
    fake = FakeRedis()
    rep = progress.ProgressReporter("c1", fake)
    steps = [
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": src, "name": "res", "format": "csv"}},
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"types": {"b": {"type": "number"}}}},
        {"run": "bcodmo_pipeline_processors.round_fields",
         "parameters": {"fields": [{"name": "b", "digits": 1}]}},
    ]
    eng = runner.execute(steps, memory_limit="256MB", reporter=rep)

    # Current-step key reflects the LAST step (0-based index 2 of 3), phase running.
    step = json.loads(fake.get(progress.step_key("c1")).decode())
    assert step["index"] == 2 and step["total"] == 3
    assert step["run"] == "bcodmo_pipeline_processors.round_fields"
    assert step["phase"] == "running"

    # Ingest reported the full row count and a memory snapshot landed.
    assert int(fake.get(progress.rows_key("c1", "res")).decode()) == 25000
    mem = json.loads(fake.get(progress.mem_key("c1")).decode())
    assert mem["memory_limit"] == 256_000_000
    assert len(eng.resources) == 1


def test_execute_without_cache_id_is_silent(tmp_path):
    """No cache_id / reporter -> the engine's reporter stays None and nothing breaks
    (the reporter path is fully skipped, not a no-op object per batch)."""
    src = _make_csv(tmp_path / "in.csv", n=100)
    steps = [
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": src, "name": "res", "format": "csv"}},
    ]
    eng = runner.execute(steps)
    assert eng.reporter is None
    assert len(eng.resources) == 1


def test_phase_derivation():
    assert runner._phase("bcodmo_pipeline_processors.load") == "ingesting"
    assert runner._phase("standard_load_multiple") == "ingesting"
    assert runner._phase("bcodmo_pipeline_processors.dump_to_s3") == "dumping"
    assert runner._phase("bcodmo_pipeline_processors.round_fields") == "running"
    assert runner._phase("sort") == "running"


# -- sampler ---------------------------------------------------------------
def test_sampler_start_stop_reentrant():
    fake = FakeRedis()
    rep = progress.ProgressReporter("c1", fake)
    eng = Engine(memory_limit="256MB")
    sampler = progress.MemorySampler(eng, rep, interval=0.05)
    sampler.start()
    sampler.start()  # idempotent -- no second thread
    # Give it a beat to sample at least once.
    import time
    time.sleep(0.15)
    sampler.stop()
    sampler.stop()  # idempotent
    # A memory snapshot was written.
    assert fake.get(progress.mem_key("c1")) is not None


def test_sampler_fork_pause_is_idempotent():
    fake = FakeRedis()
    rep = progress.ProgressReporter("c1", fake)
    eng = Engine(memory_limit="256MB")
    sampler = progress.MemorySampler(eng, rep, interval=0.05)
    sampler.start()
    # Simulate the at-fork sequence directly; must not raise or hang.
    sampler._pause_for_fork()
    sampler._pause_for_fork()  # idempotent
    sampler._resume_after_fork()
    sampler.stop()
    # Child-disable path.
    sampler._disable_in_child()
    sampler.start()  # disabled -> no-op
    assert sampler._disabled is True
