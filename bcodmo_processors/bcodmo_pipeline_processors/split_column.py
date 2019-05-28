import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging
import re

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
            datapackage_fields = resource_['schema']['fields']
            new_fields = [{
                'name': f,
                'type': 'string',
            } for f in output_fields]
            datapackage_fields += new_fields
            if delete_input:
                datapackage_fields = [
                    f for f in datapackage_fields if f['name'] not in input_fields
                ]
            resource_['schema']['fields'] = datapackage_fields

    return datapackage_


def process_resource(rows, missing_data_values):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            for field in fields:
                input_field = field['input_field']
                if input_field not in row:
                    raise Exception(f'Input field {input_field} not found in row')
                row_value = row[input_field]
                output_fields = field['output_fields']

                if row_value in missing_data_values or row_value is None:
                    for output_field in output_fields:
                        row[output_field] = row_value
                    continue


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
                if delete_input:
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
