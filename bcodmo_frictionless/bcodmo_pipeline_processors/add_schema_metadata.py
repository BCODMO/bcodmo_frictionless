from dataflows import Flow, PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher


def add_schema_metadata(metadata, resources=None):
    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                resource["schema"].update(metadata)
        yield package.pkg
        yield from package

    return func


def flow(parameters):
    resources = parameters.get("resources", None)
    return Flow(add_schema_metadata(parameters, resources=resources))
