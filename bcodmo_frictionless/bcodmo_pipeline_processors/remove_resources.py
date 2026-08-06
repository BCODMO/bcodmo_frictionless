import itertools
import os
import collections

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    BlockingStepProgress,
    REDIS_PROGRESS_DRAINING_FLAG,
)


def remove_resources(resources=None, cache_id=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        resource_names = [res["name"] for res in package.pkg.descriptor["resources"]]
        if not any(matcher.match(name) for name in resource_names):
            raise Exception(
                f'Resource pattern {resources} did not match any resources in datapackage. '
                f'Available resources: {resource_names}'
            )
        new_resources = [
            res
            for res in package.pkg.descriptor["resources"]
            if not matcher.match(res["name"])
        ]
        package.pkg.descriptor["resources"] = new_resources
        package.pkg.commit()
        yield package.pkg

        # yield from package
        # return

        for rows in package:
            if matcher.match(rows.res.name):
                # A removed resource still has to be pulled through to the end -
                # its rows are read and thrown away. That means reading the whole
                # source file (and running every step in front of this one) while
                # emitting nothing, so the dump's row counter never moves and the
                # UI sits silent for the entire drain. Report the discard.
                progress = BlockingStepProgress(
                    cache_id,
                    rows.res.name,
                    "discarding",
                    flag=REDIS_PROGRESS_DRAINING_FLAG,
                )
                try:
                    discarded = 0
                    for _ in rows:
                        discarded += 1
                        progress.update(discarded)
                finally:
                    progress.finish()
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        remove_resources(
            resources=parameters.get("resources"),
            cache_id=parameters.get("cache_id"),
        )
    )
