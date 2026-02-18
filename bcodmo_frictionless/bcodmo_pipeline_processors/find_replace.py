import logging
import re
from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def _apply_function(function):
    def _func(m):
        final_string = ""
        prev_end_index = 0
        for group_num, group in enumerate(m.groups(), start=1):
            cur_start_index = m.start(group_num)

            # Add what happened between before this match and this match
            final_string += m.group()[prev_end_index:cur_start_index]

            cur_end_index = m.end(group_num)

            # Add this group, uppercase
            if function == "uppercase":
                final_string += m.group()[cur_start_index:cur_end_index].upper()
            elif function == "lowercase":
                final_string += m.group()[cur_start_index:cur_end_index].lower()
            else:
                raise Exception("Function was not uppercase or lowercase")

            prev_end_index = cur_end_index
        # Add the rest of the string
        final_string += m.group()[prev_end_index:]

        return final_string

    return _func


def _find_replace(rows, fields, missing_values, boolean_statement=None):
    expression = get_expression(boolean_statement)

    row_counter = 0
    for row in rows:
        row_counter += 1
        new_row = dict((k, v) for k, v in row.items())

        line_passed = check_line(expression, row_counter, new_row, missing_values)
        if line_passed:
            for field in fields:
                for pattern in field.get("patterns", []):
                    name = field.get("name", None)
                    find = pattern.get("find", None)
                    replace_function = pattern.get("replace_function", "string")
                    replace = pattern.get("replace", None)
                    replace_missing_values = pattern.get(
                        "replace_missing_values", False
                    )
                    if name == None:
                        raise Exception(
                            'The "name" parameter is required for the find_replace processor'
                        )
                    if find == None:
                        raise Exception(
                            'The "find" parameter is required for the find_replace processor'
                        )
                    if replace_function not in ["string", "uppercase", "lowercase"]:
                        raise Exception(
                            'The "replace_function" parameter must be one of string, uppercase, or lowercase'
                        )

                    if replace_function == "string" and replace == None:
                        raise Exception(
                            'The "replace" parameter is required for the find_replace processor if "replace_function" is string'
                        )
                    val = new_row.get(name, None)
                    if (
                        val != None and val not in missing_values
                    ) or replace_missing_values:
                        if replace_missing_values and val is None:
                            val = ""
                        if replace_function == "string":
                            new_row[name] = re.sub(
                                str(find),
                                str(replace),
                                str(val),
                            )
                        else:
                            new_row[name] = re.sub(
                                str(find),
                                _apply_function(replace_function),
                                str(val),
                            )
        yield new_row


def find_replace(fields, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_field_names = {f["name"] for f in resource["schema"]["fields"]}
                for field in fields:
                    name = field.get("name", None)
                    if name is not None and name not in package_field_names:
                        raise Exception(
                            f'Field "{name}" not found in resource "{resource["name"]}". '
                            f'Available fields: {sorted(package_field_names)}'
                        )
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield _find_replace(
                    rows, fields, missing_values, boolean_statement=boolean_statement
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        find_replace(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )
