import sys
import functools
import collections
import logging
import time

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew

from boolean_processor_helper import (
    get_expression,
    check_line,
)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)

def process_resource(rows, missing_data_values):
    expression = get_expression(parameters.get('boolean_statement', None))

    row_counter = 0
    for row in rows:
        row_counter += 1
        line_passed = check_line(expression, row_counter, row, missing_data_values)
        if line_passed:
            yield row


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
