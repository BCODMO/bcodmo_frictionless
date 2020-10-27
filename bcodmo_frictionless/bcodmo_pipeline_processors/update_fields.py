import functools
from dataflows import Flow, PackageWrapper
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def update_fields(fields, resources=None):
    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                for field_name, props in fields.items():
                    fields_metadata = resource.get("schema", {}).get("fields", [])
                    field_metadata = None
                    for field in fields_metadata:
                        if field["name"] == field_name:
                            field_metadata = field
                            break
                    if not field_metadata:
                        raise Exception(
                            f'Field "{field_name}" not found in the datapackage'
                        )
                    field_metadata.update(props)

        yield package.pkg
        yield from package

    return func


def flow(parameters):
    fields = parameters.pop("fields", {})
    resources = parameters.get("resources", None)
    return Flow(update_fields(fields, resources=resources))


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
