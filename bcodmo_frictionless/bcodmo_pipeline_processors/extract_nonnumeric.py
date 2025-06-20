import sys
from datetime import datetime, timedelta
from dateutil.tz import tzoffset
from decimal import Decimal, InvalidOperation
import logging
import pytz
import re
import math

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def process_resource(rows, fields, missing_values, suffix=None, boolean_statement=None):
    expression = get_expression(boolean_statement)

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_values)
        try:
            for field in fields:
                new_field_name = f"{field}{suffix}"
                if not line_passed:
                    if new_field_name not in row:
                        row[new_field_name] = ""
                    continue

                # Check if  the field is numeric
                original_value = row.get(field, None)
                try:
                    if original_value is not None:
                        float(original_value)
                    is_numeric = True
                except ValueError:
                    is_numeric = False

                if is_numeric:
                    row[new_field_name] = ""
                else:
                    row[new_field_name] = row[field]
                    row[field] = None

            yield row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def extract_nonnumeric(fields, resources=None, suffix="_", boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                # Get the old fields
                package_fields = resource["schema"]["fields"]
                package_field_names = [f["name"] for f in package_fields]
                new_fields = []
                for field in package_fields:
                    new_fields.append(field)
                    if field.get("name", None) in fields:
                        new_field_name = f'{field.get("name")}{suffix}'
                        if new_field_name in package_field_names:
                            raise Exception(
                                f'The new field "{new_field_name}" already exists in the datapackage.'
                            )
                        new_fields.append(
                            {
                                "name": new_field_name,
                                "type": "string",
                            }
                        )

                # Add back to the datapackage
                resource["schema"]["fields"] = new_fields
        yield package.pkg

        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(
                    rows,
                    fields,
                    missing_values,
                    suffix=suffix,
                    boolean_statement=boolean_statement,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        extract_nonnumeric(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
            suffix=parameters.get("suffix", "_"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )
