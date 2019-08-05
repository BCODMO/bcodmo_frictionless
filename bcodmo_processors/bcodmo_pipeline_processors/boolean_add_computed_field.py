import sys
import functools
import collections
import logging
import time

from pyparsing import ParseException

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew

from boolean_add_computed_field_helper import (
    NULL_VALUES,
    boolean_expr, math_expr,
    parse_boolean, parse_math
)

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)
fields = parameters.get('fields', [])



def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            new_fields = [
                {
                    'name': nf['target'],
                    'type': nf.get('type', 'string'),
                } for nf in fields
            ]

            def filter_old_field(field):
                for nf in fields:
                    if field['name'] == nf['target']:
                        return False
                return True
            resource_['schema']['fields'] = list(filter(
                filter_old_field,
                resource_['schema']['fields'],
            ))
            resource_['schema']['fields'] += new_fields
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
            try:
                field_functions[index].append(
                    boolean_expr.parseString(boolean_string)
                )
            except ParseException as e:
                raise type(e)(
                        'Error parsing input. Make sure all strings are surrounded by \' \' and all fields are surrounded by { }:\n'
                    + str(e)
                ).with_traceback(sys.exc_info()[2])
            if function.get('math_operation', False):
                try:
                    value_functions[index].append(
                        math_expr.parseString(value_string)
                    )
                except ParseException as e:
                    raise type(e)(
                            'Error parsing value input:\n'
                        + str(e)
                    ).with_traceback(sys.exc_info()[2])
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
