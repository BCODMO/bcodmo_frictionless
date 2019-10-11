import logging
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from dataflows.helpers.resource_matcher import ResourceMatcher
import re

from boolean_processor_helper import (
    get_expression,
    check_line,
)



def _find_replace(rows, parameters, missing_data_values):
    fields = parameters.get('fields', [])
    expression = get_expression(parameters.get('boolean_statement', None))

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_data_values)
        if line_passed:
            for field in fields:
                for pattern in field.get('patterns', []):
                    row[field['name']] = re.sub(
                        str(pattern['find']),
                        str(pattern['replace']),
                        str(row[field['name']]))
        yield row


def find_replace(parameters, datapackage, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        logging.info(datapackage)
        yield package.pkg
        for rows in package:
            missing_data_values = ['']
            for resource_datapackage in datapackage.get('resources', []):
                if resource_datapackage['name'] == rows.res.name:
                    missing_data_values = resource_datapackage.get(
                        'schema', {},
                    ).get(
                        'missingValues', ['']
                    )
                    break

            if matcher.match(rows.res.name):
                yield _find_replace(rows, parameters, missing_data_values)
            else:
                yield rows

    return func


def flow(parameters, datapackage):
    return Flow(
        find_replace(
            parameters,
            datapackage,
            resources=parameters.get('resources')
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.datapackage), ctx)
