"""
DuckDB-lane progress reporting.

The dataflows lane reports pipeline progress to redis (resource lifecycle flags +
a live row counter during the dump); the web UI reads it over SSE. Because the
DuckDB lane drives the *live* ``load.flow`` / ``S3Dumper``, it already emits those
same keys for free. This module adds the three signals the DuckDB engine uniquely
knows and dataflows never surfaced:

  1. **which processor is running**  -> ``{cache_id}-duckdb-step``
  2. **ingest-into-SQL row counts**  -> ``{cache_id}-duckdb-rows-{resource}``
  3. **memory vs disk spill**        -> ``{cache_id}-duckdb-mem``

All keys live under a ``duckdb-*`` sub-namespace so the existing per-resource keys
(and the current UI panel) are untouched; ``get_pipeline_status`` folds them into an
additive ``duckdb`` object on the SSE payload. Everything degrades to a silent no-op
when there is no redis / cache_id, so the engine is unaffected outside a real run.

FORK SAFETY (see MemorySampler): the background sampler is a live thread, and a live
thread across ``os.fork()`` can deadlock the child on the glibc malloc-arena lock --
the same class of bug that made DuckDB ``threads>1`` hang the suite (see engine.py).
The DuckDB lane forks in the dumper (billiard Pool) and possibly the live loader, so
the sampler MUST NOT be alive across a fork. It is (a) run only during the fork-free
transform window, and (b) joined around every fork via process-wide
``os.register_at_fork`` hooks as defense in depth.
"""

import json
import logging
import os
import threading
import time
import weakref

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_connection,
    REDIS_EXPIRES,
)


# -- redis key helpers (duckdb sub-namespace) -------------------------------
def step_key(cache_id):
    return f"{cache_id}-duckdb-step"


def rows_key(cache_id, resource):
    return f"{cache_id}-duckdb-rows-{resource}"


def mem_key(cache_id):
    return f"{cache_id}-duckdb-mem"


def all_keys(cache_id, resources):
    """Every duckdb-lane key for a run, for redis cleanup after the run."""
    keys = [step_key(cache_id), mem_key(cache_id)]
    keys += [rows_key(cache_id, r) for r in resources]
    return keys


class ProgressReporter:
    """Writes DuckDB-lane progress to redis. Every method is a safe no-op when
    ``redis_conn`` is ``None`` (no ``REDIS_PROGRESS_URL`` configured), so callers
    never have to guard. Values expire with the same TTL as the dataflows keys."""

    def __init__(self, cache_id, redis_conn):
        self.cache_id = cache_id
        self.redis = redis_conn

    def _set(self, key, value):
        if self.redis is None:
            return
        try:
            self.redis.set(key, value, ex=REDIS_EXPIRES)
        except Exception as e:  # progress is best-effort; never fail a run for it
            logging.debug("duckdb progress set failed (%s): %s", key, e)

    def step(self, index, total, run, phase, label=None):
        """Record the currently-running step (0-based ``index`` of ``total``)."""
        self._set(
            step_key(self.cache_id),
            json.dumps({
                "index": index,
                "total": total,
                "run": run,
                "phase": phase,
                "label": label or (run.split(".")[-1] if run else run),
            }),
        )

    def rows(self, resource, count):
        """Record rows written into DuckDB for ``resource`` so far (ingest /
        re-ingest). Resets naturally per fill: a new fill's first batch overwrites
        the prior total, so the UI shows a fresh climb when a resource is rewritten."""
        self._set(rows_key(self.cache_id, resource), int(count))

    def write_memory(self, stats):
        """Record a memory/spill snapshot (see Engine.memory_stats)."""
        self._set(mem_key(self.cache_id), json.dumps(stats))


def make_reporter(cache_id):
    """Build a ProgressReporter for ``cache_id`` from the configured redis, or
    ``None`` when there's no cache_id or no redis -- in which case the engine skips
    all progress work entirely (rather than calling into a no-op object per batch)."""
    if not cache_id:
        return None
    redis_conn = get_redis_connection()
    if redis_conn is None:
        return None
    return ProgressReporter(cache_id, redis_conn)


# -- process-wide fork hooks ------------------------------------------------
# A single set of os.register_at_fork hooks dispatches to every active sampler, so
# no matter which subsystem forks (dumper / loader) no sampler thread is alive
# across the fork. Registered once, lazily, on the first sampler start.
_ACTIVE_SAMPLERS = weakref.WeakSet()
_FORK_HOOKS_REGISTERED = False
_HOOKS_LOCK = threading.Lock()


def _ensure_fork_hooks():
    global _FORK_HOOKS_REGISTERED
    with _HOOKS_LOCK:
        if _FORK_HOOKS_REGISTERED:
            return
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(
                before=_fork_before,
                after_in_parent=_fork_after_parent,
                after_in_child=_fork_after_child,
            )
        _FORK_HOOKS_REGISTERED = True


def _fork_before():
    for s in list(_ACTIVE_SAMPLERS):
        s._pause_for_fork()


def _fork_after_parent():
    for s in list(_ACTIVE_SAMPLERS):
        s._resume_after_fork()


def _fork_after_child():
    for s in list(_ACTIVE_SAMPLERS):
        s._disable_in_child()


class MemorySampler:
    """Daemon thread that refreshes ``{cache_id}-duckdb-mem`` every ``interval`` s
    from ``engine.memory_stats()`` -- giving the UI a smooth memory/disk-spill gauge
    even during a long single step (a big sort/join spilling to disk).

    Lifecycle is owned by the runner: started entering the transform window, stopped
    (joined) before the terminal dump. The loop sleeps in small slices so ``stop()``
    and the fork hooks join it in well under the interval.

    FORK SAFETY: while active, the sampler is in a process-wide registry; the
    ``os.register_at_fork`` hooks pause+join it before any fork and resume it in the
    parent (never in the child). Any future forking transform is therefore covered
    without having to know about it here."""

    def __init__(self, engine, reporter, interval=0.5, on_sample=None):
        self.engine = engine
        self.reporter = reporter
        self.interval = interval
        self.on_sample = on_sample  # optional hook (e.g. logging)
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()
        self._was_running = False
        self._disabled = False

    def start(self):
        with self._lock:
            if self._disabled or (self._thread and self._thread.is_alive()):
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="duckdb-mem-sampler", daemon=True
            )
            self._thread.start()
        _ACTIVE_SAMPLERS.add(self)
        _ensure_fork_hooks()

    def _run(self):
        while not self._stop.is_set():
            self._sample_once()
            slept = 0.0
            while slept < self.interval and not self._stop.is_set():
                time.sleep(0.05)
                slept += 0.05

    def _sample_once(self):
        try:
            stats = self.engine.memory_stats()
            self.reporter.write_memory(stats)
            if self.on_sample:
                self.on_sample(stats)
        except Exception as e:  # never let sampling kill anything
            logging.debug("duckdb memory sample failed: %s", e)

    def _join(self):
        t = self._thread
        if t:
            t.join(timeout=2.0)

    def stop(self):
        with self._lock:
            self._stop.set()
            t = self._thread
            self._thread = None
        if t:
            t.join(timeout=2.0)
        _ACTIVE_SAMPLERS.discard(self)

    # -- fork hook callbacks (run in the process that is forking) -----------
    def _pause_for_fork(self):
        with self._lock:
            self._was_running = bool(self._thread and self._thread.is_alive())
            self._stop.set()
            t = self._thread
            self._thread = None
        if t:
            t.join(timeout=2.0)

    def _resume_after_fork(self):
        if self._was_running and not self._disabled:
            self.start()

    def _disable_in_child(self):
        # In a forked child (billiard worker) the sampler must never run -- the child
        # doesn't own the DuckDB connection and must not touch it.
        self._disabled = True
        self._stop.set()
        self._thread = None
        _ACTIVE_SAMPLERS.discard(self)
