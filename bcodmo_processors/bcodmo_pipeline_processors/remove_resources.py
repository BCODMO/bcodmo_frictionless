import itertools
import os

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow



def remove_resources(resources=None,):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        new_resources = [
            res
            for res in package.pkg.descriptor["resources"]
            if not matcher.match(res["name"])
        ]
        package.pkg.descriptor["resources"] = new_resources
        yield package.pkg

        res_list = []
        for resource in package:
            if matcher.match(resource.res.name):
                for row in resource:
                    # Make sure the iterator gets all the way through all of the rows before moving on
                    pass
            else:
                res_list.append(resource)
        for r in res_list:
            yield r

    return func


def flow(parameters):
    return Flow(remove_resources(parameters.get("resources"),),)

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
