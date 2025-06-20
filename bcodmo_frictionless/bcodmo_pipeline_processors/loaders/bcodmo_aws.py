from tabulator import Loader
from tabulator import exceptions
from tabulator import helpers
from tabulator import config
from six.moves.urllib.parse import urlparse
import io
import threading
import queue
import time
from collections import deque

import time
import os
import io
import boto3
import base64
import zlib

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_connection,
    REDIS_PROGRESS_LOADING_START_FLAG,
    REDIS_PROGRESS_LOADING_DONE_FLAG,
    REDIS_EXPIRES,
)


class BcodmoAWS(Loader):
    options = [
        "s3_endpoint_url",
        "loader_cache_id",
        "loader_resource_name",
        "preloaded_chars",
        "_limit_rows",
    ]

    def __init__(
        self,
        bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
        s3_endpoint_url=None,
        loader_cache_id=None,
        loader_resource_name=None,
        preloaded_chars=None,
        _limit_rows=None,
    ):
        self.__bytes_sample_size = bytes_sample_size
        self.__s3_endpoint_url = (
            s3_endpoint_url
            or os.environ.get("S3_ENDPOINT_URL")
            or config.S3_DEFAULT_ENDPOINT_URL
        )
        self.__s3_client = boto3.resource("s3", endpoint_url=self.__s3_endpoint_url)

        self.__stats = None
        self.encoding = None

        self.loader_cache_id = loader_cache_id
        self.loader_resource_name = loader_resource_name
        self.preloaded_chars = preloaded_chars
        self.limit_rows = _limit_rows

    def _stream_load(self, source, mode="t", encoding=None):
        ###
        # This is the same as the previous load but allows streaming
        ###

        # Prepare bytes
        try:
            # print("Not using shared memory")
            start = time.time()
            parts = urlparse(source, allow_fragments=False)
            object = self.__s3_client.Object(
                bucket_name=parts.netloc, key=parts.path[1:]
            )
            bytes = BufferedS3ByteStream(object)

            if self.__stats:
                bytes = helpers.BytesStatsWrapper(bytes, self.__stats)
        except Exception as exception:
            raise exceptions.LoadingError(str(exception))

        # Return bytes
        if mode == "b":
            return bytes

        # Detect encoding
        if self.__bytes_sample_size:
            sample = bytes.read(self.__bytes_sample_size)
            bytes.seek(0)
            encoding = helpers.detect_encoding(sample, encoding)
            self.encoding = encoding

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars

    def load(self, source, mode="t", encoding=None):
        redis_conn = None
        if self.loader_cache_id is not None:
            redis_conn = get_redis_connection()
            if redis_conn is not None:
                redis_conn.set(
                    get_redis_progress_key(
                        self.loader_resource_name, self.loader_cache_id
                    ),
                    REDIS_PROGRESS_LOADING_START_FLAG,
                    ex=REDIS_EXPIRES,
                )
        if self.preloaded_chars is None:
            chars = self._stream_load(source, mode=mode, encoding=encoding)
        else:
            self.encoding = encoding
            chars = self.preloaded_chars
        if redis_conn is not None:
            redis_conn.set(
                get_redis_progress_key(self.loader_resource_name, self.loader_cache_id),
                REDIS_PROGRESS_LOADING_DONE_FLAG,
                ex=REDIS_EXPIRES,
            )
        return chars


# https://alexwlchan.net/2019/02/working-with-large-s3-objects/


class BufferedS3ByteStream(io.RawIOBase):
    def __init__(
        self, object, buffer_size=100 * 1024 * 1024, chunk_size=8 * 1024 * 1024
    ):
        self.object = object
        self.position = 0
        self.buffer_size = buffer_size  # 100MB default
        self.chunk_size = chunk_size  # 8MB chunks

        # Buffer management
        self.buffer = deque()  # List of (start_pos, data) tuples
        self.buffered_bytes = 0
        self.buffer_lock = threading.RLock()

        # Prefetch management
        self.prefetch_position = 0  # Next position to prefetch
        self.prefetch_thread = None
        self.prefetch_queue = queue.Queue(maxsize=1)  # Commands for prefetch thread
        self.stop_prefetch = threading.Event()

        # Start prefetch thread
        self._start_prefetch_thread()

        # Initial prefetch
        self._request_prefetch()

    def __repr__(self):
        return "<%s object=%r>" % (type(self).__name__, self.object)

    @property
    def size(self):
        return self.object.content_length

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset
        else:
            raise ValueError(
                "invalid whence (%r, should be %d, %d, %d)"
                % (whence, io.SEEK_SET, io.SEEK_CUR, io.SEEK_END)
            )

        # Request prefetch from new position
        self._request_prefetch()
        return self.position

    def _start_prefetch_thread(self):
        """Start the background prefetch thread."""
        self.prefetch_thread = threading.Thread(
            target=self._prefetch_worker, daemon=True
        )
        self.prefetch_thread.start()

    def _prefetch_worker(self):
        """Background thread that prefetches data."""
        while not self.stop_prefetch.is_set():
            try:
                # Wait for prefetch requests
                command = self.prefetch_queue.get(timeout=1.0)
                if command == "prefetch":
                    self._do_prefetch()
                elif command == "stop":
                    break
            except queue.Empty:
                continue

    def _request_prefetch(self):
        """Request prefetch from current position."""
        try:
            self.prefetch_queue.put_nowait("prefetch")
        except queue.Full:
            pass  # Already has a pending prefetch request

    def _do_prefetch(self):
        """Perform the actual prefetch operation."""
        with self.buffer_lock:
            # Check if we have space in buffer
            if self.buffered_bytes >= self.buffer_size:
                return

            # Determine what to prefetch
            prefetch_start = max(self.position, self.prefetch_position)

            # Don't prefetch if we already have data at this position
            if self._has_data_at_position(prefetch_start):
                # Find next position we don't have data for
                prefetch_start = self._find_next_missing_position(prefetch_start)
                if prefetch_start is None:
                    return

            # Calculate how much to prefetch
            available_buffer = self.buffer_size - self.buffered_bytes
            prefetch_size = min(
                self.chunk_size, available_buffer, self.size - prefetch_start
            )

            if prefetch_size <= 0:
                return

        # Fetch data (outside of lock to avoid blocking reads)
        try:
            range_header = (
                f"bytes={prefetch_start}-{prefetch_start + prefetch_size - 1}"
            )
            response = self.object.get(Range=range_header)
            data = response["Body"].read()

            # Add to buffer
            with self.buffer_lock:
                self.buffer.append((prefetch_start, data))
                self.buffered_bytes += len(data)
                self.prefetch_position = prefetch_start + len(data)

        except Exception as e:
            # Log error but don't crash the thread
            print(f"Prefetch error: {e}")

    def _has_data_at_position(self, position):
        """Check if we have data at the given position."""
        for start_pos, data in self.buffer:
            if start_pos <= position < start_pos + len(data):
                return True
        return False

    def _find_next_missing_position(self, start_pos):
        """Find the next position where we don't have data."""
        current_pos = start_pos
        while current_pos < self.size:
            if not self._has_data_at_position(current_pos):
                return current_pos
            # Find the end of current buffered chunk
            for start, data in self.buffer:
                if start <= current_pos < start + len(data):
                    current_pos = start + len(data)
                    break
        return None

    def _get_data_from_buffer(self, position, size):
        """Get data from buffer at position, return None if not available."""
        result = b""
        remaining = size
        current_pos = position

        while remaining > 0:
            found = False
            for start_pos, data in self.buffer:
                if start_pos <= current_pos < start_pos + len(data):
                    # Found matching chunk
                    offset_in_chunk = current_pos - start_pos
                    available_in_chunk = len(data) - offset_in_chunk
                    to_read = min(remaining, available_in_chunk)

                    result += data[offset_in_chunk : offset_in_chunk + to_read]
                    current_pos += to_read
                    remaining -= to_read
                    found = True
                    break

            if not found:
                # Data not available in buffer
                return None

        return result

    def _cleanup_buffer(self, up_to_position):
        """Remove buffer data that's been read."""
        with self.buffer_lock:
            new_buffer = deque()
            for start_pos, data in self.buffer:
                end_pos = start_pos + len(data)
                if end_pos > up_to_position:
                    if start_pos < up_to_position:
                        # Partial cleanup - keep the unread part
                        offset = up_to_position - start_pos
                        new_data = data[offset:]
                        new_buffer.append((up_to_position, new_data))
                        self.buffered_bytes -= offset
                    else:
                        # Keep entire chunk
                        new_buffer.append((start_pos, data))
                else:
                    # Remove entire chunk
                    self.buffered_bytes -= len(data)

            self.buffer = new_buffer

    def read(self, size=-1):
        if size == -1:
            # Read to EOF
            if self.position >= self.size:
                return b""
            size = self.size - self.position

        if size <= 0:
            return b""

        # Limit read to available data
        actual_size = min(size, self.size - self.position)

        with self.buffer_lock:
            # Try to get data from buffer
            data = self._get_data_from_buffer(self.position, actual_size)

        if data is None:
            # Data not in buffer, fetch directly
            range_header = f"bytes={self.position}-{self.position + actual_size - 1}"
            response = self.object.get(Range=range_header)
            data = response["Body"].read()

        # Update position
        old_position = self.position
        self.position += len(data)

        # Clean up buffer and request more prefetch
        self._cleanup_buffer(old_position)
        self._request_prefetch()

        return data

    def read1(self, size=-1):
        return self.read(size)

    def close(self):
        """Clean up resources."""
        self.stop_prefetch.set()
        try:
            self.prefetch_queue.put_nowait("stop")
        except queue.Full:
            pass

        if self.prefetch_thread and self.prefetch_thread.is_alive():
            self.prefetch_thread.join(timeout=1.0)

        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
