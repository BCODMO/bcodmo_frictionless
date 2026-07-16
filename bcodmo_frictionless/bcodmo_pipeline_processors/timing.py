"""
Lightweight per-processor speed instrumentation.

Every pipeline processor streams rows lazily (pull based), so the wall-clock
"time spent in a step" is interleaved with every other step - you can't just
wrap a step and time it, because pulling a row through step N also pulls it
through every step upstream of N.

To attribute time to a single processor we measure, per resource, three things:

  * ``work``  - the time this processor's own row transformation takes, i.e.
                the interval between receiving a row from upstream and producing
                the corresponding output row. This EXCLUDES time blocked waiting
                for upstream (pull) and time suspended after yielding while
                downstream consumes the row. This is THE per-processor number.
  * ``pull``  - the (cumulative) time this processor sat blocked in ``next()``
                waiting for the upstream chain to hand it a row. Because pulls
                cascade, this is roughly "how long everything upstream took".
  * ``rows``  - rows in / rows out (differ for filters and expanders).

Usage in a processor that yields a per-resource generator::

    from .timing import StepTimer

    for rows in package:
        if matcher.match(rows.res.name):
            timer = StepTimer("convert_date", rows.res.name)
            yield timer.wrap(process_resource(timer.rows(rows), ...))
        else:
            yield rows

``timer.rows(upstream)`` wraps the upstream row iterator (measures pull + counts
rows_in); ``timer.wrap(produced)`` wraps the produced generator (measures work +
counts rows_out) and prints a one-line ``[STEP-TIMING]`` summary when the
resource is exhausted.

Processors can add detailed sub-metrics from inside their hot loop with
``timer.add(label, seconds)`` (a named timer) and ``timer.bump(counter, n)`` (a
named counter); both are appended to the summary line.

The ``work`` decomposition is exact for 1-in / 1-out processors (convert_date,
set_types, add_computed_field, split_column, rename_fields, ...). For filters a
dropped row's work lands in ``pull`` rather than ``work`` (it is cheap), and
rows_in vs rows_out shows the drop.

Disable all timing with the env var ``LAMINAR_STEP_TIMING=0``.
"""

import os
import time
from collections import defaultdict

_ENABLED = os.environ.get("LAMINAR_STEP_TIMING", "1") not in (
    "0",
    "false",
    "False",
    "no",
    "",
)

PREFIX = "[STEP-TIMING]"


def enabled():
    return _ENABLED


class StepTimer:
    """Times one processor's work on one resource. See module docstring."""

    def __init__(self, processor, resource):
        self.processor = processor
        self.resource = resource
        self.rows_in = 0
        self.rows_out = 0
        self.pull = 0.0
        self.work = 0.0
        self.extra = defaultdict(float)  # named sub-timings, seconds
        self.counters = defaultdict(int)  # named counters
        self._t_pull_end = None

    # -- upstream (pull) wrapper -------------------------------------------
    def rows(self, upstream):
        """Wrap the upstream row iterator; feed the result to the transform."""
        if not _ENABLED:
            return upstream
        return self._rows(upstream)

    def _rows(self, upstream):
        it = iter(upstream)
        while True:
            t0 = time.perf_counter()
            try:
                row = next(it)
            except StopIteration:
                return
            now = time.perf_counter()
            self.pull += now - t0
            self.rows_in += 1
            self._t_pull_end = now
            yield row

    # -- output (work) wrapper ---------------------------------------------
    def wrap(self, produced):
        """Wrap the produced row generator; yield this to downstream."""
        if not _ENABLED:
            return produced
        return self._wrap(produced)

    def _wrap(self, produced):
        try:
            for row in produced:
                now = time.perf_counter()
                if self._t_pull_end is not None:
                    self.work += now - self._t_pull_end
                    self._t_pull_end = None
                self.rows_out += 1
                yield row
        finally:
            self.log()

    # -- detailed sub-metrics ----------------------------------------------
    def add(self, label, seconds):
        self.extra[label] += seconds

    def bump(self, counter, n=1):
        self.counters[counter] += n

    # -- output ------------------------------------------------------------
    def log(self):
        if not _ENABLED:
            return
        parts = [
            f"{PREFIX} {self.processor}",
            f"resource={self.resource}",
            f"rows_in={self.rows_in}",
            f"rows_out={self.rows_out}",
            f"work={self.work:.3f}s",
            f"pull={self.pull:.3f}s",
        ]
        for label, seconds in self.extra.items():
            parts.append(f"{label}={seconds:.3f}s")
        for counter, n in self.counters.items():
            parts.append(f"{counter}={n}")
        print(" ".join(parts), flush=True)
