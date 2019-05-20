from dataflows import Flow, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.utilities.flow_utils import spew_flow
import itertools
import os

from dataflows.helpers.resource_matcher import ResourceMatcher


def remove_resources(
    resources=None,
):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        new_resources = [
            res for res in package.pkg.descriptor['resources']
                if not matcher.match(res['name'])
        ]
        package.pkg.descriptor['resources'] = new_resources
        yield package.pkg

        it = iter(package)
        for resource in it:
            if matcher.match(resource.res.name):
                pass
            else:
                yield resource

    return func

def flow(parameters):
    return Flow(
        remove_resources(
            parameters.get('resources'),
        ),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)


