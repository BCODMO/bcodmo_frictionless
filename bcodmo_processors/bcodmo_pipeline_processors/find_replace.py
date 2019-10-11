from dataflows import Flow, find_replace
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from dataflows.helpers.resource_matcher import ResourceMatcher
import re

from boolean_processor_helper import (
    get_expression,
    check_line,
)



def _find_replace(rows, parameters):
    fields = parameters.get('fields', [])
    expression = get_expression(parameters.get('boolean_statement', None))

    for row in rows:
        for field in fields:
            for pattern in field.get('patterns', []):
                row[field['name']] = re.sub(
                    str(pattern['find']),
                    str(pattern['replace']),
                    str(row[field['name']]))
        yield row


def find_replace(parameters, resources=None):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield _find_replace(rows, parameters)
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        find_replace(
            parameters,
            resources=parameters.get('resources')
        )
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
