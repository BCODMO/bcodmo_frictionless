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
    # The number of rows/keys a blocking step has processed so far for this
    # resource - either buffered (JOINING flag) or drained (DRAINING flag).
    # Named "-join" for historical reasons; now generic.
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
# A processor is reading rows back out of a blocking buffer (a sort's ordered
# scan, a join's dedup/full-outer key scan) or discarding them outright (a
# removed resource). Rows are being consumed but few or none reach the dump, so
# the dump's row counter does not move - without this the UI looks frozen. The
# number consumed so far is stored under get_redis_progress_join_key.
REDIS_PROGRESS_DRAINING_FLAG = -8

# 1 week expiration
REDIS_EXPIRES = 60 * 60 * 24 * 7

# How often (seconds) to publish row-count progress to redis. Shared by
# dump_to_s3's row counter and the blocking-step reporters below so the two
# cannot drift apart.
#
# This is one SET per resource per interval, not per row, so the cost is set by
# the interval alone and is negligible at this rate; the row loop itself only
# pays a time.time() comparison. It is deliberately well under the SSE poll
# interval (1s in laminar_server) -- the two throttles add up, and a writer
# slower than the reader is what makes a counter visibly stutter.
PROGRESS_THROTTLE = 0.25


class BlockingStepProgress:
    """
    Reports the row-by-row progress of a step that blocks the pipeline - a join's
    key index, a sort/duplicate's row buffer, the ordered scan back out of one of
    those buffers, or a removed resource being discarded - to redis so the
    frontend can show it moving (and that it's alive vs stalled).

    None of these phases move the dump's row counter, which is the only other
    thing that reports progress, so without this the UI sits on a frozen number.

    The count is published under a DEDICATED synthetic progress entry named
    "<resource> (<kind>)" rather than the resource's own -progress key. The
    resource is often streamed to the dump concurrently, and the dump writes
    row-count progress to the resource's real key ~every 0.75s, which would
    otherwise clobber our flag almost immediately. Nothing writes the synthetic
    name, so the flag + count survive the whole step.

    Call update(count) once per processed row/key (writes are throttled). A step
    that then reads its buffer back out calls start_draining() and keeps calling
    update() through the scan. Call finish() once the step is completely done -
    NOT when its build phase ends - so the badge doesn't disappear while the step
    is still blocking. run's end-of-run cleanup is a backstop.
    """

    def __init__(
        self, cache_id, resource_name, kind, flag=REDIS_PROGRESS_JOINING_FLAG
    ):
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
            self.redis_conn.set(self._progress_key, flag, ex=REDIS_EXPIRES)
            self.redis_conn.set(self._count_key, 0, ex=REDIS_EXPIRES)

    def start_draining(self):
        """
        Switch from the buffer-building phase to the read-back phase, restarting
        the count at zero. The synthetic entry keeps its name so the frontend can
        still tell which resource and which kind of step this is; only the flag
        changes, which is what the frontend words the message from.
        """
        self.count = 0
        if self.redis_conn is not None:
            self.redis_conn.set(
                self._progress_key, REDIS_PROGRESS_DRAINING_FLAG, ex=REDIS_EXPIRES
            )
            self.redis_conn.set(self._count_key, 0, ex=REDIS_EXPIRES)
            self._timer = time.time()

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


# The class covers more than KVFile builds now; the old name is kept so any
# out-of-tree caller keeps working.
KVFileBuildProgress = BlockingStepProgress
