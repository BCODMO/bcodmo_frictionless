import redis
import os


def get_missing_values(res):
    return res.descriptor.get(
        "schema",
        {},
    ).get("missingValues", [""])


def get_redis_connection():
    redis_url = os.environ.get("REDIS_PROGRESS_URL", None)
    return redis.Redis.from_url(redis_url) if redis_url is not None else None


def get_redis_progress_key(resource, cache_id):
    return f"{cache_id}-{resource}-progress"


def get_redis_progress_resource_key(cache_id):
    return f"{cache_id}-resources"


REDIS_PROGRESS_INIT_FLAG = -1
REDIS_PROGRESS_LOADING_START_FLAG = -2
REDIS_PROGRESS_LOADING_DONE_FLAG = -3
REDIS_PROGRESS_SAVING_START_FLAG = -4
REDIS_PROGRESS_SAVING_DONE_FLAG = -5
REDIS_PROGRESS_DELETED_FLAG = -6
