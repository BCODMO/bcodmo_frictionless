from dataflows import Flow, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.utilities.flow_utils import spew_flow
import itertools
import os
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_progress_resource_key,
    get_redis_connection,
    REDIS_PROGRESS_DELETED_FLAG,
)

from dataflows.helpers.resource_matcher import ResourceMatcher


def rename_resource(old_resource, new_resource, cache_id=None):
    def func(package):
        if not old_resource or not new_resource:
            raise Exception(
                "Both old_resource and new_resource are required parameters in rename_resource"
            )
        matcher = ResourceMatcher([old_resource], package.pkg)
        for res in package.pkg.descriptor["resources"]:
            if matcher.match(res["name"]):
                old_name = res["name"]
                if cache_id is not None:
                    redis_conn = get_redis_connection()
                    progress_key = get_redis_progress_key(old_name, cache_id)
                    redis_conn.set(progress_key, REDIS_PROGRESS_DELETED_FLAG)
                    redis_conn.sadd(
                        get_redis_progress_resource_key(cache_id),
                        new_resource,
                    )
                res["name"] = new_resource
                res["path"] = res["path"].replace(old_resource, new_resource)

        yield package.pkg

        for rows in package:
            if matcher.match(rows.res.name):
                rows.res.descriptor["name"] = new_resource
                rows.res.commit()
            yield rows

    return func


def flow(parameters):
    return Flow(
        rename_resource(
            parameters.get("old_resource"),
            parameters.get("new_resource"),
            parameters.get("cache_id"),
        ),
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
