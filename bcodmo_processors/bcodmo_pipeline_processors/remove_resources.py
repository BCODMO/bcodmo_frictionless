import itertools
import os
import collections

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def remove_resources(resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
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

        rows_list = []
        for rows in package:
            if matcher.match(rows.res.name):
                collections.deque(rows, maxlen=0)
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(remove_resources(resources=parameters.get("resources")))


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
