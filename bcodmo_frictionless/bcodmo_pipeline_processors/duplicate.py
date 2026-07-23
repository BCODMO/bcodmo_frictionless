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
    KVFileBuildProgress,
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


def _unlink_quietly(path):
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


class RowFileBuffer:
    """Buffers every row of the source resource on local disk so it can be
    replayed as the duplicate copy.

    duplicate has to hand the same stream of rows to two resources - the
    original (which flows straight downstream) and the copy - but a resource is
    a one-shot generator, so the rows have to be parked somewhere in between.
    This streams them to a length-prefixed pickle file on local scratch and
    reads them back in insertion order, which keeps memory flat (~O(1)) instead
    of holding the whole resource in RAM.

    (This replaced an in-memory KVFile buffer whose LRU cache had to hold every
    row - multiple GB for a large resource - and fell off a cliff into per-row
    SQLite queries once the row count exceeded the cache size. Sequential
    pickle I/O to local disk is only a couple percent slower than the in-memory
    path, since both are dominated by the pickle (de)serialization cost.)

    Disk space is genuinely *reserved* as the buffer grows, via posix_fallocate,
    rather than just checked: the blocks are committed to this file, so a second
    duplicate running on the same worker sees the space as taken and the two
    compete for real free blocks instead of both optimistically assuming there's
    room. If the reservation can't be satisfied we raise ENOSPC immediately
    (having released everything reserved so far) instead of writing a partial
    file. On filesystems that don't support fallocate we fall back to a
    best-effort statvfs free-space check (non-reserving, but still fails fast).
    """

    def __init__(self):
        fd, self.path = tempfile.mkstemp(prefix="bcodmo_duplicate_", suffix=".pickle")
        os.close(fd)
        self._dir = os.path.dirname(self.path) or "."
        self._writer = open(self.path, "wb", buffering=FILE_BUFFER_SIZE)
        self._fd = self._writer.fileno()
        self._written = 0
        self._reserved = 0
        self._can_fallocate = True
        self._closed = False
        # Backstop: if the buffer is abandoned (pipeline error, early GC) without
        # a clean close(), still remove the temp file. Bound to `path` only - not
        # `self` - so it doesn't keep the buffer alive.
        self._finalizer = weakref.finalize(self, _unlink_quietly, self.path)
        # Reserve the first chunk now so an already-full disk fails fast, before
        # we stream any rows.
        self._reserve(RESERVE_CHUNK_SIZE)

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
        # Ensure at least `total` bytes are reserved for this file.
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

    def write(self, row):
        b = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
        n = len(b) + 4  # 4-byte length header + payload
        if self._written + n > self._reserved:
            # Grow the reservation to the next chunk boundary above what we need,
            # keeping roughly a chunk of headroom in front of the write cursor.
            target = ((self._written + n) // RESERVE_CHUNK_SIZE + 1) * RESERVE_CHUNK_SIZE
            self._reserve(target)
        self._writer.write(struct.pack("<I", len(b)))
        self._writer.write(b)
        self._written += n

    def done_writing(self):
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
        with open(self.path, "rb", buffering=FILE_BUFFER_SIZE) as f:
            while True:
                header = f.read(4)
                if not header:
                    break
                (length,) = struct.unpack("<I", header)
                yield pickle.loads(f.read(length))

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            try:
                self._writer.close()
            except OSError:
                pass
            self._writer = None
        _unlink_quietly(self.path)
        self._finalizer.detach()


def saver(resource, buf, cache_id=None):
    # Buffering every source row to disk is a per-row step that runs as the
    # resource streams downstream; publish the number of rows buffered so far so
    # the frontend can see it building up (and that it's alive vs stalled).
    progress = KVFileBuildProgress(cache_id, resource.res.name, "duplicate")
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
                buf = RowFileBuffer()
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
