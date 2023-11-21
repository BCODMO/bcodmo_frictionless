from tabulator.loaders.aws import AWSLoader
from tabulator import config

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_connection,
    REDIS_PROGRESS_LOADING_START_FLAG,
    REDIS_PROGRESS_LOADING_DONE_FLAG,
)


class BcodmoAWS(AWSLoader):

    options = [
        "s3_endpoint_url",
        "loader_cache_id",
        "loader_resource_name",
    ]

    def __init__(
        self,
        bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
        s3_endpoint_url=None,
        loader_cache_id=None,
        loader_resource_name=None,
    ):
        super(BcodmoAWS, self).__init__(s3_endpoint_url=s3_endpoint_url)
        self.loader_cache_id = loader_cache_id
        self.loader_resource_name = loader_resource_name
        print(f"Got resource name in loader {loader_resource_name}")

    def load(self, source, mode="t", encoding=None):
        redis_conn = None
        if self.loader_cache_id is not None:
            redis_conn = get_redis_connection()
            redis_conn.set(
                get_redis_progress_key(self.loader_resource_name, self.loader_cache_id),
                REDIS_PROGRESS_LOADING_START_FLAG,
            )
        chars = super(BcodmoAWS, self).load(source, mode=mode, encoding=encoding)
        if redis_conn is not None:
            redis_conn.set(
                get_redis_progress_key(self.loader_resource_name, self.loader_cache_id),
                REDIS_PROGRESS_LOADING_DONE_FLAG,
            )
        return chars
