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
                    new_row[field["name"]] = re.sub(
                        str(pattern["find"]),
                        str(pattern["replace"]),
                        str(new_row[field["name"]]),
                    )
        yield new_row


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


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
