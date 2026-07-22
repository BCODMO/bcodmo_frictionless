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
    compile_boolean,
    compile_math,
    get_expression,
    math_expr,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def _compile_field_plan(field):
    """
    Compile a field's functions once, up front, into a list of
    ``(predicate, apply_value)`` pairs.

    - ``predicate`` is either the literal ``True`` (an ``always_run`` function)
      or a callable ``(row_counter, new_row, missing_values) -> truthy``.
    - ``apply_value`` is a callable ``(row_counter, row, new_row, missing_values)
      -> new column value`` that resolves the function's ``value`` (a math
      expression or a str.format template) and applies the field's temporal type.

    The plan is returned in reverse function order so the row loop can stop at
    the first matching function. Because the original processor evaluated every
    function and let the last matching one win ("last match wins"), taking the
    first match while iterating in reverse produces the identical result while
    skipping the remaining branches.
    """
    field_type = field.get("type", None)

    def make_apply(function):
        value_string = function.get("value", "")
        if function.get("math_operation", False):
            value_expression = get_expression(value_string, math_expr)
            math_fn = compile_math(value_expression)

            def apply_value(row_counter, row, new_row, missing_values):
                new_col = math_fn(row_counter, new_row, missing_values)
                return _apply_field_type(new_col, field_type)

            return apply_value

        def apply_value(row_counter, row, new_row, missing_values):
            new_val = value_string.format(**row)
            if new_val in NULL_VALUES:
                new_val = None
            return _apply_field_type(new_val, field_type)

        return apply_value

    plan = []
    for function in field.get("functions", []):
        if function.get("always_run", False):
            predicate = True
        else:
            boolean_string = function.get("boolean", "")
            if not boolean_string:
                raise Exception(
                    f"Missing boolean string for function in boolean_add_computed_fields"
                )
            predicate = compile_boolean(get_expression(boolean_string))
        plan.append((predicate, make_apply(function)))

    # Reversed so the row loop can break on the first (i.e. last-defined) match.
    plan.reverse()
    return plan


def _apply_field_type(new_col, field_type):
    if field_type in ["datetime", "date", "time"]:
        new_col = dateutil.parser.parse(new_col)
        if field_type == "date":
            new_col = new_col.date()
        if field_type == "time":
            new_col = new_col.strftime("%H:%M:%S")
    return new_col


def process_resource(rows, fields, missing_values):
    # Compile every field's functions once, before touching any rows.
    field_plans = [(field["target"], _compile_field_plan(field)) for field in fields]

    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            new_row = dict((k, v) for k, v in row.items())
            for target, plan in field_plans:
                for predicate, apply_value in plan:
                    if predicate is True or predicate(
                        row_counter, new_row, missing_values
                    ):
                        new_row[target] = apply_value(
                            row_counter, row, new_row, missing_values
                        )
                        break
                else:
                    # No function matched; only default to None if the target
                    # isn't already present (matching the original behaviour of
                    # leaving an existing value untouched).
                    if target not in new_row:
                        new_row[target] = None

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
