import copy
import errno
import os
import pickle
import shutil
import struct
import sys
import tempfile
import threading
import time
import weakref

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    BlockingStepProgress,
)


# Size (bytes) of the userspace read/write buffers used when streaming the
# duplicated rows to/from the local scratch file. 1MiB keeps syscall overhead
# negligible without holding a meaningful amount of the resource in memory.
FILE_BUFFER_SIZE = 1 << 20

# Granularity (bytes) at which disk space is reserved ahead of the write cursor.
# We don't know the final size of the buffer up front (rows are streamed), so we
# grow the reservation a chunk at a time, always staying at least one chunk ahead
# of what's actually been written. 64MiB keeps the number of fallocate syscalls
# small (a few dozen for a multi-GB resource) while never over-reserving by more
# than one chunk. Tune with the DUPLICATE_RESERVE_CHUNK env var.
RESERVE_CHUNK_SIZE = int(os.environ.get("DUPLICATE_RESERVE_CHUNK", 64 * 1024 * 1024))


# Buffers stay in RAM until the *container* is close to its memory limit;
# spilling is triggered by measured memory pressure, not by a pre-declared
# budget. Spill I/O goes to network-attached storage, so it is expensive and
# worth deferring: a static fraction of RAM is a reservation, wasting most of a
# 30 GiB worker while still having to be conservative enough for a 2 GiB one.
#
# The measurement is the container's working set (usage minus reclaimable page
# cache) against its limit. That figure already includes everything else in the
# process - KVFile caches, pandas frames, the interpreter - so no cross-processor
# coordination is needed: whoever grows last sees the pressure and gives way.
#
# Invariants of the pressure model:
#   * We only ever compare headroom (limit - working set) against a margin; we
#     never claim a share of RAM up front.
#   * The margin is what absorbs other consumers' growth between samples, so it
#     must be comfortably larger than what the process can allocate in the ~0.5s
#     the reading is cached for.
#   * A spill only helps if the freed blobs are actually released, so the blob
#     list is dropped as it is written out and the next sample is forced fresh.
#   * Spilling a buffer smaller than MIN_SPILL_THRESHOLD buys back nothing worth
#     the disk I/O, so such buffers are never chosen as victims.

# Minimum headroom (bytes) tolerated before buffers start spilling, when no
# limit-relative margin is bigger. Override with DUPLICATE_SPILL_HEADROOM.
DEFAULT_SPILL_HEADROOM = 512 * 1024 * 1024

# ...and the same margin expressed relative to the container limit; the larger
# of the two wins, so a big worker keeps proportionally more slack.
SPILL_HEADROOM_FRACTION = 0.10

# How long (seconds) a pressure reading is reused. Long enough that the per-row
# cost is a comparison rather than three file reads, short enough that the
# margin covers what can be allocated in between.
PRESSURE_SAMPLE_INTERVAL = 0.5

# Never spill below this: tiny copies should just stay in RAM.
MIN_SPILL_THRESHOLD = 32 * 1024 * 1024

# Bytes a list costs per element it holds (one pointer on 64-bit CPython). The
# blob's own size is measured with sys.getsizeof, which includes the bytes
# object header; len(blob) alone undercounts a narrow row by 1.3-2x and would
# let the buffer overshoot a byte budget before spilling.
LIST_SLOT_BYTES = 8


def _read_int_file(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _physical_memory_bytes():
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        return None


def _container_memory_limit_bytes():
    """Best-effort detection of how much memory this process may use: the
    container's cgroup limit if one is set, otherwise the host's physical RAM.
    Returns None if nothing could be determined."""
    phys = _physical_memory_bytes()

    # cgroup v2 (unified hierarchy): "max" means unlimited.
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw and raw != "max":
            v = int(raw)
            if v > 0:
                return min(v, phys) if phys else v
    except (OSError, ValueError):
        pass

    # cgroup v1: an "unlimited" limit is a huge sentinel value, so treat
    # anything >= physical RAM as "no container limit".
    v = _read_int_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if v and v > 0:
        if phys and v >= phys:
            return phys
        return v

    return phys


def _default_spill_threshold():
    """The hard process-global byte budget from DUPLICATE_SPILL_THRESHOLD, or
    None to let memory pressure decide when to spill.

    Setting the env var pins behaviour: every live buffer's in-memory bytes are
    counted against this single figure and they spill as soon as the total
    crosses it, with the pressure monitor bypassed entirely."""
    override = os.environ.get("DUPLICATE_SPILL_THRESHOLD")
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    return None


def _spill_headroom_margin():
    """How little free memory the container may have before buffers spill."""
    override = os.environ.get("DUPLICATE_SPILL_HEADROOM")
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    limit = _container_memory_limit_bytes()
    if limit and limit > 0:
        return max(DEFAULT_SPILL_HEADROOM, int(limit * SPILL_HEADROOM_FRACTION))
    return DEFAULT_SPILL_HEADROOM


def _read_stat_field(path, field):
    """One `key value` line out of a cgroup memory.stat file."""
    try:
        with open(path) as f:
            for line in f:
                key, _, value = line.partition(" ")
                if key == field:
                    return int(value.strip())
    except (OSError, ValueError):
        return None
    return None


def _cgroup_v2_working_set():
    """cgroup v2 working set: current usage minus the inactive page cache, which
    the kernel can reclaim without any allocation failing."""
    usage = _read_int_file("/sys/fs/cgroup/memory.current")
    if usage is None:
        return None
    inactive_file = _read_stat_field("/sys/fs/cgroup/memory.stat", "inactive_file")
    return max(0, usage - (inactive_file or 0))


def _cgroup_v1_working_set():
    usage = _read_int_file("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    if usage is None:
        return None
    inactive_file = _read_stat_field(
        "/sys/fs/cgroup/memory/memory.stat", "total_inactive_file"
    )
    return max(0, usage - (inactive_file or 0))


def _mem_available_bytes():
    """MemAvailable from /proc/meminfo (kB), the kernel's own estimate of what
    can still be allocated without swapping. Used only off cgroups."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _sample_memory_headroom():
    """Bytes this process could still allocate before hitting its limit, or None
    if nothing could be measured (in which case we never spill on pressure)."""
    working_set = _cgroup_v2_working_set()
    if working_set is None:
        working_set = _cgroup_v1_working_set()
    if working_set is not None:
        limit = _container_memory_limit_bytes()
        if limit and limit > 0:
            return max(0, limit - working_set)
    # No cgroup accounting (or no limit to compare against): on a bare host
    # MemAvailable is already the headroom figure.
    return _mem_available_bytes()


# Guards the cached pressure reading. Buffers are written from processor
# generators, which dataflows may drive from more than one thread.
_pressure_lock = threading.Lock()
# (monotonic timestamp, headroom, margin) of the last sample, or None.
_pressure_sample = None


def _memory_pressure(force=False):
    """(under_pressure, headroom, margin) for the container, sampled at most
    every PRESSURE_SAMPLE_INTERVAL seconds unless `force`d (after a spill, when
    the cached figure is known to be stale)."""
    global _pressure_sample
    now = time.monotonic()
    if not force:
        with _pressure_lock:
            sample = _pressure_sample
        if sample is not None and now - sample[0] < PRESSURE_SAMPLE_INTERVAL:
            _, headroom, margin = sample
            return (headroom is not None and headroom < margin), headroom, margin

    headroom = _sample_memory_headroom()
    margin = _spill_headroom_margin()
    with _pressure_lock:
        _pressure_sample = (now, headroom, margin)
    return (headroom is not None and headroom < margin), headroom, margin


def _relieve_memory_pressure():
    """Spill live buffers, largest first, until the container has headroom again.

    Largest-first because one spill of the biggest buffer frees more than every
    small one put together, and each spill costs a file plus its disk
    reservation. Re-samples after every spill (forced, so the freed memory is
    actually observed) and stops as soon as the margin is clear."""
    under_pressure, _, _ = _memory_pressure()
    while under_pressure:
        victim = _largest_spillable_buffer()
        if victim is None:
            # Nothing left worth spilling; the pressure is someone else's.
            return
        victim._spill()
        under_pressure, _, _ = _memory_pressure(force=True)


def _largest_spillable_buffer():
    with SpillingRowBuffer._budget_lock:
        candidates = [b for b in SpillingRowBuffer._live if not b._spilled]
    victim = None
    for buf in candidates:
        if buf._mem_bytes < MIN_SPILL_THRESHOLD:
            continue
        if victim is None or buf._mem_bytes > victim._mem_bytes:
            victim = buf
    return victim


def _blob_memory_cost(blob):
    """Approximate resident cost of holding one pickled row in the buffer list."""
    return sys.getsizeof(blob) + LIST_SLOT_BYTES


class _MemoryCharge:
    """The in-memory bytes one buffer currently owes the shared budget.

    Kept in its own object, not on the buffer, so a finalizer can hand the bytes
    back without holding a reference that would keep the buffer alive.
    """

    __slots__ = ("bytes",)

    def __init__(self):
        self.bytes = 0


def _release_charge(charge):
    # Invariant: the shared total is the sum of the live charges, so every
    # charged byte is given back exactly once - on spill, on close, or (for an
    # abandoned buffer) from the finalizer.
    if not charge.bytes:
        return
    with SpillingRowBuffer._budget_lock:
        SpillingRowBuffer._in_memory_bytes -= charge.bytes
    charge.bytes = 0


def _unlink_quietly(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _cleanup_quietly(writer, path):
    # Finalizer helper: close the still-open writer (so an abandoned buffer
    # doesn't leak the handle and emit a ResourceWarning) and remove the file.
    try:
        writer.close()
    except Exception:
        pass
    _unlink_quietly(path)


class SpillingRowBuffer:
    """Buffers the source resource's rows so duplicate can replay them as the
    copy, holding them in memory while the container has room and spilling to
    local disk when it doesn't.

    duplicate has to hand the same stream of rows to two resources - the
    original (which flows straight downstream) and the copy - but a resource is
    a one-shot generator, so the rows have to be parked somewhere in between.

    Rows are stored as length-prefixed pickle blobs, kept in a list in memory -
    no temp file is even created, so most duplicates never touch the disk. When
    the container runs low on memory the buffered blobs are flushed to a
    length-prefixed pickle file on local scratch, the in-memory list is
    released, and every subsequent row streams straight to disk. Either way the
    read side replays the rows in insertion order.

    Spilling is driven by measured pressure, not by a reserved budget: on each
    write we compare the container's headroom against a margin (see
    _memory_pressure) and, if it's short, spill the largest live buffer, then
    the next largest, until the margin is clear. Several buffers are routinely
    alive at once (two duplicate steps, or duplicate_to_end holding one buffer
    per source resource until the end of the package), and the one that notices
    the pressure is not necessarily the one worth spilling - hence the shared
    registry and largest-first victim selection.

    DUPLICATE_SPILL_THRESHOLD replaces all of that with a hard process-global
    byte budget: the class then tracks the in-memory bytes of every live buffer
    and a buffer spills as soon as the total crosses the budget, with the
    pressure monitor bypassed. `spill_threshold` does the same for one buffer
    alone. Both exist to make spilling deterministic (tests, or an operator
    pinning behaviour).

    (This replaced an in-memory KVFile buffer whose LRU cache had to hold every
    row and fell off a cliff into per-row SQLite queries once the row count
    exceeded the cache size, and then a pure write-through-to-disk buffer that
    paid disk I/O even for tiny copies. Sequential pickle I/O to local disk is
    only a couple percent slower than the in-memory path, since both are
    dominated by the pickle (de)serialization cost.)

    Once spilled, disk space is genuinely *reserved* as the buffer grows, via
    posix_fallocate, rather than just checked: the blocks are committed to this
    file, so a second duplicate running on the same worker sees the space as
    taken and the two compete for real free blocks instead of both
    optimistically assuming there's room. If the reservation can't be satisfied
    we raise ENOSPC immediately (having released everything reserved so far)
    instead of writing a partial file. On filesystems that don't support
    fallocate we fall back to a best-effort statvfs free-space check
    (non-reserving, but still fails fast).
    """

    # Process-wide state shared by every live instance, all guarded by the lock:
    # `_live` is the registry pressure relief picks victims from, `_budget_bytes`
    # is the optional hard ceiling on `_in_memory_bytes` (the sum of the unspilled
    # bytes held by all buffers), None when spilling is pressure-driven.
    # Reentrant: a garbage collection triggered while the lock is held can run a
    # buffer's finalizer, which takes the same lock to hand its bytes back.
    _budget_lock = threading.RLock()
    _budget_bytes = None
    _in_memory_bytes = 0
    _live = weakref.WeakSet()

    def __init__(self, spill_threshold=None):
        # The global budget is refreshed per buffer rather than cached at import
        # so that the env var (and tests patching _default_spill_threshold) are
        # picked up, and so a stale budget can never outlive the buffers it was
        # computed for.
        default = _default_spill_threshold()
        # An explicit threshold caps this buffer alone; the shared budget, when
        # one is configured, still applies on top of it.
        self._spill_threshold = (
            spill_threshold if spill_threshold is not None else default
        )
        # In-memory phase: raw pickle blobs and the bytes they've charged against
        # the shared total.
        self._mem = []
        self._charge_state = _MemoryCharge()
        self._mem_finalizer = weakref.finalize(
            self, _release_charge, self._charge_state
        )
        self._spilled = False
        self._closed = False
        # Disk phase state (created lazily on spill).
        self.path = None
        self._dir = None
        self._writer = None
        self._fd = None
        self._written = 0
        self._reserved = 0
        self._can_fallocate = True
        self._finalizer = None
        with SpillingRowBuffer._budget_lock:
            SpillingRowBuffer._budget_bytes = default
            SpillingRowBuffer._live.add(self)

    # -- shared memory accounting (only used while unspilled) ----------------

    @property
    def _mem_bytes(self):
        return self._charge_state.bytes

    def _charge(self, n):
        """Account for `n` more in-memory bytes; True if they can stay in RAM as
        far as the configured byte budgets are concerned (pressure is a separate
        question, and only asked when no budget is configured)."""
        self._charge_state.bytes += n
        with SpillingRowBuffer._budget_lock:
            SpillingRowBuffer._in_memory_bytes += n
            total = SpillingRowBuffer._in_memory_bytes
            budget = SpillingRowBuffer._budget_bytes
        if self._spill_threshold is not None and (
            self._charge_state.bytes > self._spill_threshold
        ):
            return False
        return budget is None or total <= budget

    def _release(self):
        _release_charge(self._charge_state)

    def _unregister(self):
        with SpillingRowBuffer._budget_lock:
            SpillingRowBuffer._live.discard(self)

    # -- disk-space reservation (only used once spilled) --------------------

    def _free_bytes(self):
        return shutil.disk_usage(self._dir).free

    def _out_of_space(self, needed):
        free = self._free_bytes()
        self.close()  # release whatever we'd reserved before erroring out
        raise OSError(
            errno.ENOSPC,
            f"Not enough disk space to buffer duplicate: need at least "
            f"{needed} more bytes but only {free} free on {self._dir}",
            self.path,
        )

    def _reserve(self, total):
        # Ensure at least `total` bytes are reserved for the spill file.
        if total <= self._reserved:
            return
        if self._can_fallocate:
            try:
                os.posix_fallocate(self._fd, 0, total)
                self._reserved = total
                return
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    self._out_of_space(total - self._reserved)
                # Filesystem doesn't support fallocate (e.g. some network mounts):
                # drop to the best-effort check for the rest of this buffer.
                self._can_fallocate = False
        # Fallback: can't truly reserve, so just refuse to proceed if the disk
        # can't currently hold the additional space.
        if self._free_bytes() < (total - self._reserved):
            self._out_of_space(total - self._reserved)
        self._reserved = total

    def _open_spill_file(self):
        fd, self.path = tempfile.mkstemp(prefix="bcodmo_duplicate_", suffix=".pickle")
        os.close(fd)
        self._dir = os.path.dirname(self.path) or "."
        self._writer = open(self.path, "wb", buffering=FILE_BUFFER_SIZE)
        self._fd = self._writer.fileno()
        # Backstop: if the buffer is abandoned (pipeline error, early GC) without
        # a clean close(), still close the writer and remove the temp file. Bound
        # to the writer and `path` - not `self` - so it doesn't keep the buffer
        # alive (the file object holds no reference back to this buffer).
        self._finalizer = weakref.finalize(
            self, _cleanup_quietly, self._writer, self.path
        )
        # Reserve the first chunk now so an already-full disk fails fast.
        self._reserve(RESERVE_CHUNK_SIZE)

    def _write_frame(self, blob):
        n = len(blob) + 4  # 4-byte length header + payload
        if self._written + n > self._reserved:
            # Grow the reservation to the next chunk boundary above what we need,
            # keeping roughly a chunk of headroom in front of the write cursor.
            target = ((self._written + n) // RESERVE_CHUNK_SIZE + 1) * RESERVE_CHUNK_SIZE
            self._reserve(target)
        self._writer.write(struct.pack("<I", len(blob)))
        self._writer.write(blob)
        self._written += n

    def _spill(self):
        # Transition from the in-memory phase to the on-disk phase: open the
        # file, flush everything buffered so far in order, then free the memory.
        if self._spilled:
            return
        freed = self._mem_bytes
        self._open_spill_file()
        # Drop each blob as soon as it's on disk rather than after the loop: the
        # point of spilling is to give memory back *now*, and the next pressure
        # sample is taken as soon as this returns.
        mem, self._mem = self._mem, None
        for i, blob in enumerate(mem):
            self._write_frame(blob)
            mem[i] = None
        mem.clear()
        self._release()
        self._unregister()
        self._spilled = True
        print(
            f"duplicate: spilling {freed} in-memory bytes to {self.path} "
            f"(byte budget {SpillingRowBuffer._budget_bytes}, buffer limit "
            f"{self._spill_threshold})"
        )

    # -- public API ---------------------------------------------------------

    def write(self, row):
        blob = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        if self._spilled:
            self._write_frame(blob)
            return
        self._mem.append(blob)
        within_budget = self._charge(_blob_memory_cost(blob))
        if not within_budget:
            self._spill()
        elif SpillingRowBuffer._budget_bytes is None:
            # No configured budget: the container's own memory pressure decides,
            # and the victim may well be a different (larger) buffer than this.
            _relieve_memory_pressure()

    def done_writing(self):
        # Nothing to finalise in the in-memory phase - the list is already
        # complete. Once spilled, flush and trim the reservation padding.
        if self._writer is not None:
            self._writer.flush()
            # Release the surplus we reserved beyond the actual bytes written, and
            # give the file a correct size so read() sees EOF in the right place
            # (fallocate padded it out past the real data).
            try:
                os.ftruncate(self._fd, self._written)
            except OSError:
                pass
            self._writer.close()
            self._writer = None

    def read(self):
        if self._spilled:
            with open(self.path, "rb", buffering=FILE_BUFFER_SIZE) as f:
                while True:
                    header = f.read(4)
                    if not header:
                        break
                    (length,) = struct.unpack("<I", header)
                    yield pickle.loads(f.read(length))
        else:
            for blob in self._mem:
                yield pickle.loads(blob)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._mem = None
        self._release()
        self._unregister()
        self._mem_finalizer.detach()
        if self._writer is not None:
            try:
                self._writer.close()
            except OSError:
                pass
            self._writer = None
        if self.path is not None:
            _unlink_quietly(self.path)
        if self._finalizer is not None:
            self._finalizer.detach()


def saver(resource, buf, cache_id=None):
    # Buffering every source row (in memory, then spilling to disk once the
    # threshold is crossed) is a per-row step that runs as the resource streams
    # downstream; publish the number of rows buffered so far so the frontend can
    # see it building up (and that it's alive vs stalled).
    progress = BlockingStepProgress(cache_id, resource.res.name, "duplicate")
    count = 0
    try:
        for row in resource:
            yield row
            buf.write(row)
            count += 1
            progress.update(count)
        buf.done_writing()
    except Exception:
        # A write/reservation failure (or an upstream error) means no copy will
        # ever be replayed from this buffer - drop it and free its disk space.
        buf.close()
        raise
    finally:
        progress.finish()


def loader(buf, close=True):
    # A single buffer may be replayed into several copies (multi mode), so the
    # caller decides when the buffer's temp file can finally be removed - only
    # the last replay should close (and delete) it.
    try:
        yield from buf.read()
    finally:
        if close:
            buf.close()


def duplicate(
    source=None,
    target_name=None,
    target_path=None,
    batch_size=1000,
    duplicate_to_end=False,
    multi=False,
    target_names=None,
    cache_id=None,
):
    def func(package):
        source_ = source
        if source_ is None:
            source_ = package.pkg.descriptor['resources'][0]['name']

        # Build the list of copies to create as (name, path) pairs. In multi
        # mode the user supplies a list of names; otherwise it's the single
        # legacy target-name/target-path pair.
        if multi:
            targets = [(name, name + '.csv') for name in (target_names or [])]
        else:
            target_name_ = target_name
            if target_name_ is None:
                target_name_ = source_ + '_copy'
            target_path_ = target_path
            if target_path_ is None:
                target_path_ = target_name_ + '.csv'
            targets = [(target_name_, target_path_)]

        def traverse_resources(resources):
            new_res_list = []
            for res in resources:
                yield res
                if res['name'] == source_:
                    for target_name_, target_path_ in targets:
                        new_res = copy.deepcopy(res)
                        new_res['name'] = target_name_
                        new_res['path'] = target_path_
                        if duplicate_to_end:
                            new_res_list.append(new_res)
                        else:
                            yield new_res
            for res in new_res_list:
                yield res

        descriptor = package.pkg.descriptor
        descriptor['resources'] = list(traverse_resources(descriptor['resources']))
        yield package.pkg

        deferred_bufs = []
        for resource in package:
            if resource.res.name == source_ and targets:
                buf = SpillingRowBuffer()
                yield saver(resource, buf, cache_id=cache_id)
                if duplicate_to_end:
                    deferred_bufs.append(buf)
                else:
                    # Replay the buffer once per copy, closing (deleting) it
                    # only after the final replay.
                    for i in range(len(targets)):
                        yield loader(buf, close=(i == len(targets) - 1))
            else:
                yield resource
        for buf in deferred_bufs:
            for i in range(len(targets)):
                yield loader(buf, close=(i == len(targets) - 1))

    return func


def load_lazy_json(resources):
    # Source rows loaded from a checkpoint arrive as lazily-parsed json wrappers.
    # Unwrap them to their evaluated dicts as they stream, matching the behaviour
    # of the standard dataflows duplicate wrapper.
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield (
                    row.inner if hasattr(row, "inner") else row for row in rows
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        load_lazy_json(parameters.get("source")),
        duplicate(
            parameters.get("source"),
            parameters.get("target-name"),
            parameters.get("target-path"),
            parameters.get("batch_size", 1000),
            parameters.get("duplicate_to_end", False),
            parameters.get("multi", False),
            parameters.get("target_names"),
            cache_id=parameters.get("cache_id"),
        ),
    )
