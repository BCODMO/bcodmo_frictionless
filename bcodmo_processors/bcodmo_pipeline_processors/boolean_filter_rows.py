import sys
import functools
import collections
import logging
import time

from pyparsing import ParseException

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew

from boolean_processor_helper import (
    NULL_VALUES,
    boolean_expr,
    parse_boolean,
)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)

def process_resource(rows, missing_data_values):
    expressions = []
    for function in parameters.get('functions', []):
        boolean_string = function.get('boolean', '')
        try:
            expressions.append(
                boolean_expr.parseString(boolean_string)
            )
        except ParseException as e:
            raise type(e)(
                    f'Error parsing boolean string {boolean_string}. Make sure all strings are surrounded by \' \' and all fields are surrounded by {{ }}:\n'
                + str(e)
            ).with_traceback(sys.exc_info()[2])

    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            for expression in expressions:
                expression_true = parse_boolean(row_counter, expression, row, missing_data_values)
                if expression_true:
                    yield row
                    break
        except Exception as e:
            raise type(e)(
                str(e) +
                f' at row {row_counter}'
            ).with_traceback(sys.exc_info()[2])


def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            missing_data_values = ['']
            for resource_datapackage in datapackage.get('resources', []):
                if resource_datapackage['name'] == spec['name']:
                    missing_data_values = resource_datapackage.get(
                        'schema', {},
                    ).get(
                        'missingValues', ['']
                    )
                    break
            yield process_resource(resource, missing_data_values)



spew(datapackage, process_resources(resource_iterator))
