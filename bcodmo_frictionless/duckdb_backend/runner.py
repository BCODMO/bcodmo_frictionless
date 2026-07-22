"""
Pipeline runner: execute a bcodmo pipeline spec on the DuckDB engine.

This is the engine-side half of the laminar_server router seam. laminar_server
owns the ORCHESTRATION (cache_id / pipeline_spec injection, the summary dump to the
results bucket, redis cleanup, UI error mapping); this module owns only what it
means to RUN a prepared list of steps on the DuckDB backend and read the result --
so all engine logic stays in ``duckdb_backend`` and laminar_server keeps a thin,
delegating branch.

Contract mirrors the dataflows lane closely enough to be a drop-in for supported
pipelines: same spec format in, same resources out. ``is_supported`` is the safety
gate -- a pipeline the DuckDB backend can't fully honor falls back to dataflows,
so enabling the engine can never fail a run it wouldn't have failed before.
"""

import datetime
import decimal

from .engine import Engine
from .processor import REGISTRY


class StepError(Exception):
    """A step failed during engine execution. Carries the 0-based ``index`` and
    ``run`` name so the caller can attribute the error to a specific step (like
    the dataflows lane's ``_LaminarStepError``)."""

    def __init__(self, index, run, cause):
        self.index = index
        self.run = run
        self.cause = cause
        super().__init__(str(cause))


def is_supported(steps):
    """True iff every step can run on the DuckDB backend: each ``run`` maps to a
    registered processor and no step requests a dataflows-only feature.

    Conservative by design -- any unknown run-name or a ``checkpoint`` (a
    dataflows/EFS feature the engine doesn't implement) disqualifies the whole
    pipeline, so the caller cleanly falls back to the dataflows lane rather than
    running a partially-supported spec."""
    return not unsupported_reasons(steps)


def unsupported_reasons(steps):
    """Diagnostic companion to ``is_supported``: return a list of human-readable
    reasons (one per disqualifying step) why ``steps`` can't run on the DuckDB
    backend, or ``[]`` if it can. Lets the caller REFUSE an explicit ``duckdb``
    request with a message that names the offending step(s), instead of silently
    falling back to the dataflows lane."""
    reasons = []
    for index, step in enumerate(steps):
        n = index + 1  # 1-based, matching the UI's step numbering
        if not isinstance(step, dict):
            reasons.append(f"step {n}: not a mapping ({type(step).__name__})")
            continue
        run = step.get("run")
        if step.get("checkpoint"):
            reasons.append(
                f"step {n} ({run}): uses 'checkpoint', a dataflows/EFS-only feature"
            )
        if run not in REGISTRY:
            reasons.append(
                f"step {n} ({run}): no DuckDB processor is registered for this run-name"
            )
    return reasons


def execute(steps, memory_limit=None, temp_directory=None, threads=None):
    """Run already-prepared ``steps`` on a fresh Engine and return it.

    ``memory_limit`` + ``temp_directory`` arm DuckDB's out-of-core spill so the run
    is never-OOM (see tests/test_never_oom.py). ``threads`` defaults to Engine's
    fork-safe single-threaded default (the pipeline ends in a fork-based dump).
    Raises ``StepError`` on the first failing step so the caller can attribute it;
    the partially-built engine is discarded."""
    eng = Engine(
        threads=threads, memory_limit=memory_limit, temp_directory=temp_directory
    )
    for index, step in enumerate(steps):
        try:
            eng.apply(step)
        except Exception as cause:
            raise StepError(index, step.get("run", "unknown"), cause) from cause
    return eng


def iter_sample(engine, name, size):
    """Yield up to ``size`` typed rows (dicts) of a resource, in __rownum__ order,
    for the UI sample -- streamed, so a huge resource costs only ``size`` rows."""
    it = engine.typed_rows_iter(name)
    for _, row in zip(range(size), it):
        yield row


def build_sample(engine, size=25):
    """The UI sample the laminar read path expects: a list, one entry per resource
    (in order), each ``[header, *rows]`` with up to ``size`` rows. Values are
    coerced exactly as the dataflows lane's sample does -- Decimal -> float and
    datetime -> its output ``format`` (else isoformat) -- so the two engines yield
    the same sample. Streamed: only ``size`` rows per resource are ever realized."""
    out = []
    for name in engine.resources:
        fields = engine.resources[name].schema
        rows = [[f["name"] for f in fields]]
        for row in iter_sample(engine, name, size):
            new_row = []
            for f in fields:
                val = row.get(f["name"])
                if isinstance(val, decimal.Decimal):
                    val = float(val)
                if isinstance(val, datetime.datetime):
                    fmt = f.get("format")
                    val = val.strftime(fmt) if fmt and fmt != "default" else val.isoformat()
                new_row.append(val)
            rows.append(new_row)
        out.append(rows)
    return out
