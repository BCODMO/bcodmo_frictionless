import redis
import os
import time


def get_missing_values(res):
    return res.descriptor.get(
        "schema",
        {},
    ).get("missingValues", [""])


def get_redis_connection():
    redis_url = os.environ.get("REDIS_PROGRESS_URL", None)
    return redis.Redis.from_url(redis_url) if redis_url is not None else None


def get_redis_progress_key(resource, cache_id):
    # A flag for where in the pipeline we are
    return f"{cache_id}-{resource}-progress"


def get_redis_progress_resource_key(cache_id):
    # A list of all of the resources
    return f"{cache_id}-resources"


def get_redis_progress_num_parts_key(resource, cache_id):
    # The total number of parts to be uploaded
    return f"{cache_id}-{resource}-num-parts"


def get_redis_progress_parts_key(resource, cache_id):
    # A list of parts that have been succesfully uploaded
    return f"{cache_id}-{resource}-parts"


def get_redis_progress_join_key(resource, cache_id):
    # The size (rows/keys) of the in-memory KVFile buffer built so far for this
    # resource. Reported while a join/sort/duplicate is in its (blocking) buffer-
    # building phase. Named "-join" for historical reasons; now generic.
    return f"{cache_id}-{resource}-join"


REDIS_PROGRESS_INIT_FLAG = -1
REDIS_PROGRESS_LOADING_START_FLAG = -2
REDIS_PROGRESS_LOADING_DONE_FLAG = -3
REDIS_PROGRESS_SAVING_START_FLAG = -4
REDIS_PROGRESS_SAVING_DONE_FLAG = -5
REDIS_PROGRESS_DELETED_FLAG = -6
# A processor is building a blocking in-memory KVFile buffer for this resource -
# a join's key index, or a sort/duplicate's row buffer. The size built so far is
# stored under get_redis_progress_join_key. (Flag name kept for compatibility.)
REDIS_PROGRESS_JOINING_FLAG = -7

# 1 week expiration
REDIS_EXPIRES = 60 * 60 * 24 * 7

# How often (seconds) to publish buffer-building progress to redis. Matches the
# throttle used by dump_to_s3 so we don't hammer redis on every row.
PROGRESS_THROTTLE = 0.75


class KVFileBuildProgress:
    """
    Reports the growth of a blocking KVFile build - a join's key index, or a
    sort/duplicate's row buffer - to redis so the frontend can show it filling up
    (and that it's alive vs stalled).

    The count is published under a DEDICATED synthetic progress entry named
    "<resource> (<kind>)" rather than the resource's own -progress key. The
    resource is often streamed to the dump concurrently, and the dump writes
    row-count progress to the resource's real key ~every 0.75s, which would
    otherwise clobber our flag almost immediately. Nothing writes the synthetic
    name, so the flag + count survive the whole build.

    Call update(count) once per processed row/key (writes are throttled), then
    finish() when the build completes to remove the entry so its "building" badge
    doesn't linger. run's end-of-run cleanup is a backstop.
    """

    def __init__(self, cache_id, resource_name, kind):
        self.cache_id = cache_id
        self.count = 0
        self._timer = time.time()
        self.redis_conn = get_redis_connection() if cache_id else None
        self.progress_name = f"{resource_name} ({kind})"
        if self.redis_conn is not None:
            resource_set_key = get_redis_progress_resource_key(cache_id)
            self.redis_conn.sadd(resource_set_key, self.progress_name)
            self.redis_conn.expire(resource_set_key, REDIS_EXPIRES)
            self._progress_key = get_redis_progress_key(self.progress_name, cache_id)
            self._count_key = get_redis_progress_join_key(self.progress_name, cache_id)
            self.redis_conn.set(
                self._progress_key, REDIS_PROGRESS_JOINING_FLAG, ex=REDIS_EXPIRES
            )
            self.redis_conn.set(self._count_key, 0, ex=REDIS_EXPIRES)

    def update(self, count):
        self.count = count
        if (
            self.redis_conn is not None
            and time.time() - self._timer > PROGRESS_THROTTLE
        ):
            self.redis_conn.set(self._count_key, count, ex=REDIS_EXPIRES)
            self._timer = time.time()

    def finish(self):
        if self.redis_conn is not None:
            self.redis_conn.delete(self._progress_key)
            self.redis_conn.delete(self._count_key)
            self.redis_conn.srem(
                get_redis_progress_resource_key(self.cache_id), self.progress_name
            )
