from tabulator import Loader
from tabulator import exceptions
from tabulator import helpers
from tabulator import config
from six.moves.urllib.parse import urlparse

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
        self.__s3_client = boto3.client("s3", endpoint_url=self.__s3_endpoint_url)
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
            response = self.__s3_client.get_object(
                Bucket=parts.netloc, Key=parts.path[1:]
            )
            # https://github.com/frictionlessdata/tabulator-py/issues/271
            bytes = io.BufferedRandom(io.BytesIO())
            row_count = 0
            if self.limit_rows:
                # We limit the number of rows being streamed
                while True:
                    contents = response["Body"].read(amt=1024)
                    if contents:
                        row_count += contents.count(b"\n")
                        bytes.write(contents)
                        # To be extra safe, we multiply by two and add 100. This is meant to deal with
                        # situations where there is a weird header/empty row situation at the beginning of the file
                        # which will later be filtered by the parser
                        if row_count >= self.limit_rows * 2 + 100:
                            break
                    else:
                        break
            else:
                contents = response["Body"].read()
                bytes.write(contents)
            bytes.seek(0)
            try:
                print(
                    f"Took {round(time.time() - start, 3)} to load in {os.path.basename(source)}"
                )
            except:
                pass

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
