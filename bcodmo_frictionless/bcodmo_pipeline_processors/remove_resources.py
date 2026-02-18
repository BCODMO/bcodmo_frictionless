import itertools
import os
import collections

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher


def remove_resources(resources=None):
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

        rows_list = []
        for rows in package:
            if matcher.match(rows.res.name):
                collections.deque(rows, maxlen=0)
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(remove_resources(resources=parameters.get("resources")))
