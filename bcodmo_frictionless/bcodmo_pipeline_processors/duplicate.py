import copy
import errno
import os
import pickle
import shutil
import struct
import tempfile
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


# Once the rows held in memory grow past this many bytes, the buffer stops
# holding them in RAM and spills to local disk. The default is a fraction of the
# worker's detected memory limit (see _default_spill_threshold), so a small
# instance (e.g. a 2 GiB worker) spills early and can't OOM, while a larger
# instance keeps more in memory and often avoids the disk entirely. Pin an
# absolute budget with DUPLICATE_SPILL_THRESHOLD (bytes) or change the fraction
# with DUPLICATE_SPILL_MEMORY_FRACTION.
DEFAULT_SPILL_MEMORY_FRACTION = 0.25

# Clamp the auto-computed threshold: never spill below MIN (tiny copies should
# just stay in RAM) and never hold more than MAX in memory even on a huge box
# (spilling there is cheap, and we don't want duplicate hoarding many GB).
MIN_SPILL_THRESHOLD = 32 * 1024 * 1024
MAX_SPILL_THRESHOLD = 1024 * 1024 * 1024

# Used only when no memory limit can be detected at all.
FALLBACK_SPILL_THRESHOLD = 256 * 1024 * 1024


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
    """Bytes of in-memory rows to tolerate before spilling to disk."""
    override = os.environ.get("DUPLICATE_SPILL_THRESHOLD")
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass

    fraction = DEFAULT_SPILL_MEMORY_FRACTION
    frac_env = os.environ.get("DUPLICATE_SPILL_MEMORY_FRACTION")
    if frac_env:
        try:
            fraction = float(frac_env)
        except ValueError:
            pass

    limit = _container_memory_limit_bytes()
    if limit and limit > 0:
        return min(MAX_SPILL_THRESHOLD, max(MIN_SPILL_THRESHOLD, int(limit * fraction)))
    return FALLBACK_SPILL_THRESHOLD


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
    copy, holding them in memory while small and spilling to local disk once
    they exceed a memory budget.

    duplicate has to hand the same stream of rows to two resources - the
    original (which flows straight downstream) and the copy - but a resource is
    a one-shot generator, so the rows have to be parked somewhere in between.

    Rows are stored as length-prefixed pickle blobs. While the running total
    stays under `spill_threshold` they're kept in a list in memory - no temp
    file is even created, so small duplicates never touch the disk. The first
    time the total would exceed the threshold we spill: the buffered blobs are
    flushed to a length-prefixed pickle file on local scratch, the in-memory
    list is released, and every subsequent row streams straight to disk. Either
    way the read side replays the rows in insertion order.

    The threshold defaults to a fraction of the worker's memory limit (see
    _default_spill_threshold), so a small instance spills sooner and won't OOM,
    while a large instance keeps more in RAM and often avoids the disk entirely.

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

    def __init__(self, spill_threshold=None):
        self._spill_threshold = (
            spill_threshold
            if spill_threshold is not None
            else _default_spill_threshold()
        )
        # In-memory phase: raw pickle blobs and their framed byte total.
        self._mem = []
        self._mem_bytes = 0
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
        self._open_spill_file()
        for blob in self._mem:
            self._write_frame(blob)
        self._mem = None
        self._mem_bytes = 0
        self._spilled = True
        print(
            f"duplicate: in-memory buffer exceeded {self._spill_threshold} bytes, "
            f"spilling to {self.path}"
        )

    # -- public API ---------------------------------------------------------

    def write(self, row):
        blob = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        if self._spilled:
            self._write_frame(blob)
            return
        self._mem.append(blob)
        self._mem_bytes += len(blob) + 4
        if self._mem_bytes > self._spill_threshold:
            self._spill()

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
        self._mem_bytes = 0
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
