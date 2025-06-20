import sys
import functools
import collections
import logging
import time
import dateutil.parser
import datetime

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher


from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    NULL_VALUES,
    get_expression,
    math_expr,
    parse_boolean,
    parse_math,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def process_resource(rows, fields, missing_values):
    field_functions = []
    value_functions = []
    for index in range(len(fields)):
        field = fields[index]
        field_functions.append([])
        value_functions.append([])
        for function in field.get("functions", []):
            boolean_string = function.get("boolean", "")
            value_string = function.get("value", "")
            always_run = function.get("always_run", False)

            if always_run:
                # Send True to the function
                field_functions[index].append(True)
            else:
                if not boolean_string:
                    raise Exception(
                        f"Missing boolean string for function in boolean_add_computed_fields"
                    )

                # Parse the field boolean string
                field_expression = get_expression(boolean_string)
                field_functions[index].append(field_expression)

            # Parse the value boolean string
            if function.get("math_operation", False):
                value_expression = get_expression(value_string, math_expr)
                value_functions[index].append(value_expression)
            else:
                value_functions.append(None)

    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            new_row = dict((k, v) for k, v in row.items())
            for field_index in range(len(fields)):
                field = fields[field_index]

                functions = field.get("functions", [])
                for func_index in range(len(functions)):
                    function = functions[func_index]
                    expression = field_functions[field_index][func_index]

                    expression_true = (
                        expression
                        if isinstance(expression, bool)
                        else parse_boolean(
                            row_counter, expression, new_row, missing_values
                        )
                    )
                    if expression_true:
                        value_ = function.get("value", "")
                        if function.get("math_operation", False):
                            # Handle a mathematical equation in value
                            value_expression = value_functions[field_index][func_index]
                            new_col = parse_math(
                                row_counter, value_expression, new_row, missing_values
                            )
                        else:
                            new_val = value_.format(**row)
                            if new_val in NULL_VALUES:
                                new_val = None
                            new_col = new_val

                        field_type = field.get("type", None)
                        if field_type in ["datetime", "date", "time"]:
                            new_col = dateutil.parser.parse(new_col)
                            if field_type == "date":
                                new_col = new_col.date()
                            if field_type == "time":
                                new_col = new_col.strftime("%H:%M:%S")

                        new_row[field["target"]] = new_col
                    elif field["target"] not in new_row:
                        new_row[field["target"]] = None

            yield new_row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def boolean_add_computed_field(fields, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                # Get the old fields
                package_fields = resource["schema"]["fields"]

                # Create a list of names and a lookup dict for the new fields
                new_field_names = [f["target"] for f in fields]
                new_fields_dict = {
                    f["target"]: {
                        "name": f["target"],
                        "type": f.get("type", "string"),
                    }
                    for f in fields
                }

                # Iterate through the old fields, updating where necessary to maintain order
                processed_fields = []
                for f in package_fields:
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
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        boolean_add_computed_field(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
        )
    )
