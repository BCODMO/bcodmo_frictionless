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
        "_stream_loading",
    ]

    def __init__(
        self,
        bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
        s3_endpoint_url=None,
        loader_cache_id=None,
        loader_resource_name=None,
        preloaded_chars=None,
        _limit_rows=None,
        _stream_loading=None,
    ):
        self.__bytes_sample_size = bytes_sample_size
        self.__s3_endpoint_url = (
            s3_endpoint_url
            or os.environ.get("S3_ENDPOINT_URL")
            or config.S3_DEFAULT_ENDPOINT_URL
        )

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
            parts = urlparse(source, allow_fragments=False)
            if mode == "b":
                # We don't stream files that are returned in bytes
                s3_client = boto3.client("s3", endpoint_url=self.__s3_endpoint_url)
                response = s3_client.get_object(
                    Bucket=parts.netloc, Key=parts.path[1:]
                )
                # https://github.com/frictionlessdata/tabulator-py/issues/271
                bytes = io.BufferedRandom(io.BytesIO())
                contents = response["Body"].read()
                bytes.write(contents)
                bytes.seek(0)

            else:
                bytes = BufferedS3ByteStream(self.__s3_endpoint_url, parts.netloc, parts.path[1:])#object)

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

import io
import boto3
import threading
import queue
from collections import deque
import time
import os


class BufferedS3ByteStream(io.RawIOBase):
    def __init__(
        self, s3_endpoint_url, bucket_name, key, buffer_size=100 * 1024 * 1024, chunk_size=8 * 1024 * 1024
    ):
        self.bucket_name = bucket_name
        self.key = key
        self.s3_client = boto3.client("s3", endpoint_url=s3_endpoint_url)

        self.position = 0
        self.buffer_size = buffer_size  # 100MB default
        self.chunk_size = chunk_size  # 8MB chunks
        self._size = None

        # Buffer management
        self.buffer = deque()  # List of (start_pos, data) tuples
        self.buffered_bytes = 0
        self.buffer_lock = threading.RLock()

        # Prefetch management
        self.prefetch_position = 0  # Next position to prefetch
        self.prefetch_thread = None
        self.stop_prefetch = threading.Event()
        self.position_changed = threading.Event()  # Signal when position changes

        # Use a condition variable for more efficient signaling
        self.buffer_condition = threading.Condition(self.buffer_lock)

        # Start prefetch thread with high priority
        self._start_prefetch_thread()

    def __repr__(self):
        return "<%s bucket=%s, key=%s>" % (type(self).__name__, self.bucket_name, self.key)

    @property
    def size(self):
        if self._size is None:
            object_info = self.s3_client.head_object(Bucket=self.bucket_name, Key=self.key)
            self._size = object_info['ContentLength']
        return self._size

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

        # Signal position change to prefetch thread
        self.position_changed.set()
        with self.buffer_condition:
            self.buffer_condition.notify()

        return self.position

    def _start_prefetch_thread(self):
        """Start the background prefetch thread with high priority."""
        self.prefetch_thread = threading.Thread(
            target=self._prefetch_worker, daemon=True, name="S3Prefetch"
        )

        self.prefetch_thread.start()

    def _prefetch_worker(self):
        """Background thread that aggressively prefetches data."""
        # Try to set higher priority for this thread
        try:
            if hasattr(os, 'nice'):
                # Lower nice value = higher priority (Unix)
                os.nice(-5)  # May require privileges
        except (OSError, AttributeError):
            pass

        # Continuously run until stopped
        while not self.stop_prefetch.is_set():
            try:
                # Check if we should prefetch more data
                should_prefetch = False
                prefetch_start = None
                prefetch_size = 0

                with self.buffer_condition:
                    # Wait briefly if buffer is full or no work to do
                    if self.buffered_bytes >= self.buffer_size:
                        # Buffer is full, wait for space
                        self.buffer_condition.wait(timeout=0.1)
                        continue

                    # Find next position to prefetch
                    current_pos = max(self.position, self.prefetch_position)
                    prefetch_start = self._find_next_prefetch_position(current_pos)

                    if prefetch_start is not None:
                        # Calculate optimal prefetch size
                        available_buffer = self.buffer_size - self.buffered_bytes
                        prefetch_size = min(
                            self.chunk_size,
                            available_buffer,
                            self.size - prefetch_start
                        )
                        should_prefetch = prefetch_size > 0
                        reached_eof = (prefetch_start + prefetch_size) >= self.size
                    else:
                        # No position to prefetch means we've likely reached EOF
                        reached_eof = True

                if should_prefetch:
                    # Perform prefetch outside of lock to avoid blocking reads
                    self._do_prefetch_at_position(prefetch_start, prefetch_size)
                else:
                   # No work to do - adjust wait time based on whether we reached EOF
                    if reached_eof:
                        # We've prefetched to EOF, sleep longer until position changes
                        time.sleep(0.5) # 0.5 second timeout
                        if self.position_changed.wait(timeout=0.05):
                            self.position_changed.clear()
                            reached_eof = False  # Reset since position changed
                    else:
                        # Normal case - brief wait for position changes or buffer space
                        if self.position_changed.wait(timeout=0.05):  # 50ms timeout
                            self.position_changed.clear()

            except Exception as e:
                # Log error but don't crash the thread
                print(f"Prefetch worker error: {e}")
                time.sleep(0.1)  # Brief pause on error

    def _find_next_prefetch_position(self, start_pos):
        """Find the next position where we should prefetch data."""
        # Start from the furthest point we've prefetched or current position
        current_pos = start_pos

        # Look for gaps in our buffer or extend beyond current buffer
        while current_pos < self.size:
            if not self._has_data_at_position(current_pos):
                return current_pos

            # Find end of current buffered region
            for start, data in self.buffer:
                if start <= current_pos < start + len(data):
                    current_pos = start + len(data)
                    break
            else:
                # No buffer found at current position, can prefetch here
                return current_pos

        return None  # Reached end of file

    def _do_prefetch_at_position(self, prefetch_start, prefetch_size):
        """Perform the actual prefetch operation at a specific position."""
        try:
            range_header = f"bytes={prefetch_start}-{prefetch_start + prefetch_size - 1}"

            # This is where the thread will be blocked on network I/O
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.key,
                Range=range_header
            )
            data = response["Body"].read()

            # Quickly add to buffer
            with self.buffer_condition:
                self.buffer.append((prefetch_start, data))
                self.buffered_bytes += len(data)
                self.prefetch_position = max(self.prefetch_position, prefetch_start + len(data))

                # Notify any waiting read operations
                self.buffer_condition.notify_all()

        except Exception as e:
            # Log error but don't crash the thread
            print(f"Prefetch error at position {prefetch_start}: {e}")

    def _has_data_at_position(self, position):
        """Check if we have data at the given position."""
        for start_pos, data in self.buffer:
            if start_pos <= position < start_pos + len(data):
                return True
        return False

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
        with self.buffer_condition:
            new_buffer = deque()
            bytes_removed = 0

            for start_pos, data in self.buffer:
                end_pos = start_pos + len(data)
                if end_pos > up_to_position:
                    if start_pos < up_to_position:
                        # Partial cleanup - keep the unread part
                        offset = up_to_position - start_pos
                        new_data = data[offset:]
                        new_buffer.append((up_to_position, new_data))
                        bytes_removed += offset
                    else:
                        # Keep entire chunk
                        new_buffer.append((start_pos, data))
                else:
                    # Remove entire chunk
                    bytes_removed += len(data)

            self.buffer = new_buffer
            self.buffered_bytes -= bytes_removed

            # Notify prefetch thread that buffer space is available
            if bytes_removed > 0:
                self.buffer_condition.notify()

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

        # Use a shorter lock duration to give prefetch thread more opportunities
        data = None
        with self.buffer_lock:
            # Try to get data from buffer
            data = self._get_data_from_buffer(self.position, actual_size)

        if data is None:
            #print(f"Grabbing data myself: {self.buffered_bytes / (1024*1024)} MB")
            # Data not in buffer, fetch directly
            # This is less optimal but ensures we don't block indefinitely
            range_header = f"bytes={self.position}-{self.position + actual_size - 1}"
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self.key,
                Range=range_header
            )
            data = response["Body"].read()
        else:
            pass
            #print(f"Got data from buffer: {self.buffered_bytes / (1024*1024)} MB")

        # Update position
        old_position = self.position
        self.position += len(data)

        # Clean up buffer and signal position change
        self._cleanup_buffer(old_position)
        self.position_changed.set()

        return data

    def read1(self, size=-1):
        return self.read(size)

    def close(self):
        """Clean up resources."""
        self.stop_prefetch.set()
        self.position_changed.set()  # Wake up the prefetch thread

        with self.buffer_condition:
            self.buffer_condition.notify_all()

        if self.prefetch_thread and self.prefetch_thread.is_alive():
            self.prefetch_thread.join(timeout=2.0)  # Longer timeout for cleanup

        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
