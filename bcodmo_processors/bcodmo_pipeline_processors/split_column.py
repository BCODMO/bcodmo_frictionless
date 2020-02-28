import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging
import re

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
delete_input = parameters.get('delete_input', False)

def modify_datapackage(datapackage_):
    output_fields = []
    input_fields = []
    for field in fields:
        output_fields += field.get('output_fields', [])
        input_fields.append(field.get('input_field'))
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            # Get the old fields
            datapackage_fields = resource_['schema']['fields']

            # Create a list of names and a lookup dict for the new fields
            new_field_names = [f for f in output_fields]
            new_fields_dict = {
                f: {
                    'name': f,
                    'type': 'string',
                } for f in output_fields
            }

            # Iterate through the old fields, updating where necessary to maintain order
            processed_fields = []
            for f in datapackage_fields:
                if delete_input and f['name'] in input_fields and f['name'] not in output_fields:
                    continue
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
                input_field = field['input_field']
                if input_field not in row:
                    raise Exception(f'Input field {input_field} not found in row')
                row_value = row[input_field]
                output_fields = field['output_fields']

                if not line_passed:
                    for output_field in output_fields:
                        row[output_field] = None
                    continue

                if row_value in missing_data_values or row_value is None:
                    for output_field in output_fields:
                        row[output_field] = row_value
                    continue
                row_value = str(row_value)


                pattern = field['pattern']
                match = re.search(pattern, row_value)
                # Ensure there is a match
                if not match:
                    raise Exception(f'Match not found for expression \"{pattern}\" and value \"{row_value}\"')
                groups = match.groups()
                if len(groups) != len(output_fields):
                    raise Exception(
                        f'Found a different number of matches to the number of output fields: "{groups}" and "{output_fields}"'
                    )
                for index in range(len(groups)):
                    string = groups[index]
                    output_field = output_fields[index]
                    row[output_field] = string
                if delete_input and input_field not in output_fields:
                    del row[input_field]

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
