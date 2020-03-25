import re
import sys

from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


def process_resource(rows, fields, pattern):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            for field in fields:
                if field not in row:
                    continue
                new_field_name = re.sub(
                    str(pattern['find']),
                    str(pattern['replace']),
                    str(field),
                )
                if new_field_name is not field and new_field_name in row:
                    raise Exception(f'New field name {new_field_name} already exists in row {list(row.keys())}')
                row[new_field_name] = row.pop(field, None)
            yield row
        except Exception as e:
            raise type(e)(
                str(e) +
                f' at row {row_counter}'
            ).with_traceback(sys.exc_info()[2])



def rename_fields_regex(fields, pattern, resources=None):
    def func(package):
        if not pattern:
            raise Exception('The "pattern" parameter is required')

        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                for field in fields:
                    new_field_name = re.sub(
                        str(pattern['find']),
                        str(pattern['replace']),
                        str(field),
                    )
                    package_fields = resource['schema']['fields']
                    for package_field in package_fields:
                        if package_field['name'] == field:
                            package_field['name'] = new_field_name
                    resource['schema']['fields'] = package_fields

        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield process_resource(
                    rows, fields, pattern,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        rename_fields(
            parameters.get("fields", []), parameters.get('pattern'), resources=parameters.get("resources")
        )
    )

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
