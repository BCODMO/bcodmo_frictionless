from tabulator.loaders.aws import AWSLoader
from tabulator import config

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_connection,
    REDIS_PROGRESS_LOADING_START_FLAG,
    REDIS_PROGRESS_LOADING_DONE_FLAG,
    REDIS_EXPIRES,
)


class BcodmoAWS(AWSLoader):
    options = [
        "s3_endpoint_url",
        "loader_cache_id",
        "loader_resource_name",
        "preloaded_chars",
    ]

    def __init__(
        self,
        bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
        s3_endpoint_url=None,
        loader_cache_id=None,
        loader_resource_name=None,
        preloaded_chars=None,
    ):
        super(BcodmoAWS, self).__init__(s3_endpoint_url=s3_endpoint_url)
        self.loader_cache_id = loader_cache_id
        self.loader_resource_name = loader_resource_name
        self.preloaded_chars = preloaded_chars

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
            chars = super(BcodmoAWS, self).load(source, mode=mode, encoding=encoding)
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
