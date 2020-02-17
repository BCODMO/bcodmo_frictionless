import sys
import functools
import collections
import logging
import time

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew

from boolean_processor_helper import (
    NULL_VALUES,
    get_expression,
    math_expr,
    parse_boolean, parse_math
)

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
            new_field_names = [f['target'] for f in fields]
            new_fields_dict = {
                f['target']: {
                    'name': f['target'],
                    'type': f.get('type', 'string'),
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
    field_functions = []
    value_functions = []
    for index in range(len(fields)):
        field = fields[index]
        field_functions.append([])
        value_functions.append([])
        for function in field.get('functions', []):
            boolean_string = function.get('boolean', '')
            value_string = function.get('value', '')

            # Parse the field boolean string
            field_expression = get_expression(boolean_string)
            field_functions[index].append(field_expression)

            # Parse the value boolean string

            if function.get('math_operation', False):
                value_expression = get_expression(value_string, math_expr)
                value_functions[index].append(value_expression)
            else:
                value_functions.append(None)

    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            for field_index in range(len(fields)):
                field = fields[field_index]

                functions = field.get('functions', [])
                for func_index in range(len(functions)):
                    function = functions[func_index]
                    expression = field_functions[field_index][func_index]

                    expression_true = parse_boolean(row_counter, expression, row, missing_data_values)
                    if expression_true:
                        value_ = function.get('value', '')
                        if function.get('math_operation', False):
                            # Handle a mathematical equation in value
                            value_expression = value_functions[field_index][func_index]
                            new_col = parse_math(row_counter, value_expression, row, missing_data_values)
                        else:
                            new_val = value_.format(**row)
                            if new_val in NULL_VALUES:
                                new_val = None
                            new_col = new_val
                        row[field['target']] = new_col
                    elif field['target'] not in row:
                        row[field['target']] = None

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
