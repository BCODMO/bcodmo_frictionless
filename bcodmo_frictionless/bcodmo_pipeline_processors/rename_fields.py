import sys
from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows import Flow
import logging


def process_resource(rows, fields):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            new_row = dict((k, v) for k, v in row.items())
            for field in fields:
                old_field_name = field["old_field"]
                new_field_name = field["new_field"]
                if new_field_name in new_row:
                    raise Exception(
                        f"New field name {new_field_name} already exists in row {new_row.keys()}"
                    )
                new_row[new_field_name] = new_row.pop(old_field_name, None)
            yield new_row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def rename_fields(fields, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_fields = resource["schema"]["fields"]
                package_field_names = {f["name"] for f in package_fields}
                for field in fields:
                    old_field_name = field["old_field"]
                    new_field_name = field["new_field"]
                    if old_field_name not in package_field_names:
                        raise Exception(
                            f'Field "{old_field_name}" not found in resource "{resource["name"]}". '
                            f'Available fields: {sorted(package_field_names)}'
                        )
                    for package_field in package_fields:
                        if package_field["name"] == old_field_name:
                            package_field["name"] = new_field_name
                    resource["schema"]["fields"] = package_fields
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield process_resource(
                    rows,
                    fields,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        rename_fields(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
        )
    )
