import sys
import logging
import re

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from bcodmo_processors.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from bcodmo_processors.bcodmo_pipeline_processors.helper import get_missing_values


def process_resource(
    rows, fields, missing_values, delete_input=False, boolean_statement=None
):
    expression = get_expression(boolean_statement)

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_values)
        try:
            for field in fields:
                input_field = field["input_field"]
                if input_field not in row:
                    raise Exception(f"Input field {input_field} not found in row")
                row_value = row[input_field]
                output_fields = field["output_fields"]

                if not line_passed:
                    for output_field in output_fields:
                        row[output_field] = None
                    continue

                if delete_input and input_field not in output_fields:
                    del row[input_field]

                if row_value in missing_values or row_value is None:
                    for output_field in output_fields:
                        row[output_field] = row_value
                    continue
                row_value = str(row_value)

                pattern = field["pattern"]
                match = re.search(pattern, row_value)
                # Ensure there is a match
                if not match:
                    raise Exception(
                        f'Match not found for expression "{pattern}" and value "{row_value}"'
                    )
                groups = match.groups()
                if len(groups) != len(output_fields):
                    raise Exception(
                        f'Found a different number of matches to the number of output fields: "{groups}" and "{output_fields}"'
                    )
                for index in range(len(groups)):
                    string = groups[index]
                    output_field = output_fields[index]
                    row[output_field] = string

            yield row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def split_column(fields, delete_input=False, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)

        output_fields = []
        input_fields = []
        for field in fields:
            output_fields += field.get("output_fields", [])
            input_fields.append(field.get("input_field"))

        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                # Get the old fields
                package_fields = resource["schema"]["fields"]

                # Create a list of names and a lookup dict for the new fields
                new_field_names = [f for f in output_fields]
                new_fields_dict = {
                    f: {"name": f, "type": "string",} for f in output_fields
                }

                # Iterate through the old fields, updating where necessary to maintain order
                processed_fields = []
                for f in package_fields:
                    if (
                        delete_input
                        and f["name"] in input_fields
                        and f["name"] not in output_fields
                    ):
                        continue
                    if f["name"] in new_field_names:
                        processed_fields.append(new_fields_dict[f["name"]])
                        new_field_names.remove(f["name"])
                    else:
                        processed_fields.append(f)
                # Add new fields that were not added through the update
                for fname in new_field_names:
                    processed_fields.append(new_fields_dict[fname])

                # Add back to the datapackage
                resource["schema"]["fields"] = processed_fields

        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(
                    rows,
                    fields,
                    missing_values,
                    delete_input=delete_input,
                    boolean_statement=boolean_statement,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        split_column(
            parameters.get("fields", []),
            delete_input=parameters.get("delete_input", False),
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
