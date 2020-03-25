from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging


def reorder_fields(fields, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_fields = resource['schema']['fields']
                package_field_names = [o['name'] for o in package_fields]

                new_fields_list = []
                for field in fields:
                    try:
                        field_index = package_field_names.index(field)
                    except ValueError:
                        raise Exception(f'Field {field} not found in the list of fields: {package_field_names}')
                    new_fields_list.append(package_fields[field_index])

                if len(new_fields_list) != len(package_fields):
                    raise Exception(f'Only {len(new_fields_list)} were passed in to the reorder_fields step, {len(package_fields)} required')
                resource['schema']['fields'] = new_fields_list
        yield package.pkg
        yield from package

    return func

def flow(parameters):
    return Flow(
        round_fields(
            parameters.get("fields", []),
            resources=parameters.get("resources", None),
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)

