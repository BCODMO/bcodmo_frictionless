import logging
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from dataflows.helpers.resource_matcher import ResourceMatcher
import re

from .boolean_processor_helper import (
    get_expression,
    check_line,
)
from .helper import get_missing_values


def _find_replace(rows, fields, missing_values, boolean_statement=None):
    expression = get_expression(boolean_statement)

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_values)
        if line_passed:
            for field in fields:
                for pattern in field.get("patterns", []):
                    row[field["name"]] = re.sub(
                        str(pattern["find"]),
                        str(pattern["replace"]),
                        str(row[field["name"]]),
                    )
        yield row


def find_replace(fields, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
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
