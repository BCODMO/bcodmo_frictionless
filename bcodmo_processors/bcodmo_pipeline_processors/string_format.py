import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
from datetime import datetime, timedelta
from dateutil.tz import tzoffset
from decimal import Decimal, InvalidOperation
import logging
import pytz
import re
import math

from boolean_processor_helper import (
    get_expression,
    check_line,
)

logging.basicConfig(
    level=logging.WARNING,
)
logger = logging.getLogger(__name__)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)
fields = parameters.get('fields', [])

def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            # Get the old fields
            datapackage_fields = resource_['schema']['fields']

            # Create a list of names and a lookup dict for the new fields
            new_field_names = [f['output_field'] for f in fields]
            new_fields_dict = {
                f['output_field']: {
                    'name': f['output_field'],
                    'type': 'string',
                } for f in fields
            }

            # Iterate through the old fields, updating where necessary to maintain order
            processed_fields = []
            for f in datapackage_fields:
                if f['name'] in new_field_names:
                    processed_fields.append(new_fields_dict[f['name']])
                    new_field_names.remove(f['name'])
                else:
                    processed_fields.append(f)
            # Add new fields that were not added through the update
            for fname in new_field_names:
                processed_fields.append(new_fields_dict[fname])

            # Add back to the datapackage
            resource_['schema']['fields'] = processed_fields
    return datapackage_


def process_resource(rows, missing_data_values):
    expression = get_expression(parameters.get('boolean_statement', None))

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_data_values)
        try:
            for field in fields:
                # Inititalize all of the parameters that are used by both python and excel input_type
                output_field = field.get('output_field', None)
                if not output_field:
                    raise Exception('output_field is required')
                input_string = field.get('input_string', None)
                if not input_string:
                    raise Exception('input_string is required')
                input_fields = field.get('input_fields', None)
                if not input_fields:
                    raise Exception('input_fields is required')

                if not line_passed:
                    if output_field in row:
                        row[output_field] = row[output_field]
                    else:
                        row[output_field] = None
                    continue
                row_values = []
                for input_field in input_fields:
                    if input_field not in row:
                        raise Exception(f'Input field {input_field} not found: {row}')
                    if row[input_field] in missing_data_values or row[input_field] is None:
                        # There is a value in missing_data_values
                        # per discussion with data managers, set entire row to None
                        row_values = None
                        break
                    row_values.append(row[input_field])
                if row_values is None:
                    if output_field in row:
                        row[output_field] = row[output_field]
                    else:
                        row[output_field] = None
                    continue

                # Do the string format
                try:
                    row[output_field] = input_string.format(*row_values)
                except ValueError:
                    raise Exception(
                        f'There was an error while formatting {input_string} to the values {row_values} at row {row_counter}'
                        + ' Make sure that the types of the fields correctly correspond to the format string'
                    )

            yield row
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


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
