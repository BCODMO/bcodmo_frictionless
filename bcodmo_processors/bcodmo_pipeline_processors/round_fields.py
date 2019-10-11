import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging

from boolean_processor_helper import (
    get_expression,
    check_line,
)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)
fields = parameters.get('fields', [])

def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            datapackage_fields = resource_['schema']['fields']
            for field in fields:
                # If we should cast to an integer rather than a number
                if field.get('convert_to_integer', False) and field['digits'] == 0:
                    def convert_to_integer(f):
                        if f['name'] == field['name']:
                            if f['type'] != 'number':
                                raise Exception(
                                    f'Attempting to convert a field ("{f["name"]}") that has not been cast to a number: {f["type"]}'
                                )
                            f['type'] = 'integer'
                        return f

                    datapackage_fields = list(map(convert_to_integer, datapackage_fields))

            resource_['schema']['fields'] = datapackage_fields
    return datapackage_

def process_resource(rows, missing_data_values, schema):
    dp_resources = datapackage.get('resources', [])
    expression = get_expression(parameters.get('boolean_statement', None))

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_data_values)
        if line_passed:
            try:
                for field in fields:
                    cur_schema_field = {}
                    for schema_field in schema.get('fields', []):
                        if schema_field['name'] == field['name']:
                            cur_schema_field = schema_field
                            break
                    # Check if the type in the datapackage is a number
                    if cur_schema_field['type'] == 'number' or (
                        cur_schema_field['type'] == 'integer' and field['digits'] == 0 and field.get('convert_to_integer', False)
                    ):
                        orig_val = row[field['name']]
                        if orig_val in missing_data_values or orig_val is None:
                            row[field['name']] = orig_val
                            continue
                        rounded_val = round(float(orig_val), int(field['digits']))
                        # Convert the rounded val back to the original type
                        # If the field has been cast to a number without validating,
                        # it might be a string here
                        new_val = type(orig_val)(str(rounded_val))
                        #
                        if field.get('convert_to_integer', False) and field['digits'] == 0:
                            # If we have cast to integer AND validated, we can set the type here to integer
                            if type(orig_val) is not 'string':
                                new_val = int(new_val)

                        row[field['name']] = new_val
                    else:
                        raise Exception(
                            f'Attempting to convert a field ("{field["name"]}") that has not been cast to a number'
                        )
                yield row
            except Exception as e:
                raise type(e)(
                    str(e) +
                    f' at row {row_counter}'
                ).with_traceback(sys.exc_info()[2])
        else:
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
                    schema = resource_datapackage.get(
                        'schema', {},
                    )
                    missing_data_values = schema.get(
                        'missingValues', ['']
                    )
                    break
            yield process_resource(resource, missing_data_values, schema)


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
