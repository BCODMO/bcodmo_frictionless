import sys
import functools
import collections
import logging
import time

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def _boolean_filter_rows(rows, missing_values, boolean_statement):
    expression = get_expression(boolean_statement)
    row_counter = 0
    for row in rows:
        row_counter += 1
        line_passed = check_line(expression, row_counter, row, missing_values)
        if line_passed:
            yield row


def boolean_filter_rows(resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg

        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield _boolean_filter_rows(rows, missing_values, boolean_statement)

            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        boolean_filter_rows(
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
