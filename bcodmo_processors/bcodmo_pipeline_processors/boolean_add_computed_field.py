import sys
import functools
import collections
import logging
import pyparsing as pp
import time
from dateutil import parser

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)
fields = parameters.get('fields', [])

NULL_VALUES = ['null', 'NULL', 'None', 'NONE']

# Set up a language logic for parsing boolean strings
operator = pp.Regex(">=|<=|!=|>|<|==").setName("operator")
date = pp.Regex("(\d+[/:\- ])+(\d+)?")
number = pp.Regex(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
variable = pp.Regex(r"\{.*?\}")
string = pp.Regex(r"'.*?'")
null = pp.Regex('|'.join(NULL_VALUES))
comparison_term = date | number | variable | string | null
condition = pp.Group(comparison_term + operator + comparison_term)
expr = pp.operatorPrecedence(
    condition,
    [
        ("AND", 2, pp.opAssoc.LEFT, ),
        ("and", 2, pp.opAssoc.LEFT, ),
        ("&&", 2, pp.opAssoc.LEFT, ),
        ("OR", 2, pp.opAssoc.LEFT, ),
        ("or", 2, pp.opAssoc.LEFT, ),
        ("||", 2, pp.opAssoc.LEFT, ),
    ],
)

def parse_pyparser_result(row_counter, res, row, missing_data_values):
    ''' Parse a result from pyparser '''
    if type(res) == bool:
        return res
    # Try to convert to float
    try:
        return float(res)
    except (ValueError, TypeError):
        pass
    # Parse string
    if type(res) == str:
        # Handle null passed in
        if res in NULL_VALUES:
            return None
        if res.startswith("'") and res.endswith("'"):
            return res[1:-1]

        try:
            return float((res.format(**row)))
        except ValueError:
            try:
                val = res.format(**row)
                # Handle val being part of missing_data_Values
                if val in missing_data_values or val is None or (val == 'None' and row[res[1:-1]] == None):
                    return None
                return parser.parse(val)
            except ValueError:
                return val

    if len(res) == 0:
        return False
    if len(res) == 1:
        return parse_pyparser_result(row_counter, res[0], row, missing_data_values)
    first_value = None
    operation = None
    try:
        for term in res:
            if not first_value:
                first_value = term
            elif not operation:
                operation = term
            else:
                first_parsed = parse_pyparser_result(row_counter, first_value, row, missing_data_values)
                second_parsed = parse_pyparser_result(row_counter, term, row, missing_data_values)
                if operation == '>':
                    first_value = first_parsed > second_parsed
                elif operation == '>=':
                    first_value = first_parsed >= second_parsed
                elif operation == '<':
                    first_value = first_parsed < second_parsed
                elif operation == '<=':
                    first_value = first_parsed <= second_parsed
                elif operation == '!=':
                    first_value = first_parsed != second_parsed
                elif operation == '==':
                    first_value = first_parsed == second_parsed
                elif operation in ['AND', 'and', '&&']:
                    first_value = first_parsed and second_parsed
                elif operation in ['OR', 'or', '||']:
                    first_value = first_parsed or second_parsed
                operation = None
    except TypeError as e:
        raise e

    return first_value



def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            new_fields = [
                {
                    'name': f['target'],
                    'type': 'string',
                } for f in fields
            ]
            resource_['schema']['fields'] += new_fields
    return datapackage_

def process_resource(rows, missing_data_values):
    field_functions = []
    for index in range(len(fields)):
        field = fields[index]
        field_functions.append([])
        for function in field.get('functions', []):
            boolean_string = function.get('boolean', '')
            try:
                field_functions[index].append(
                    expr.parseString(boolean_string)
                )
            except pp.ParseException as e:
                raise type(e)(
                        'Error parsing input. Make sure all strings are surrounded by \' \' and all fields are surrounded by { }:\n'
                    + str(e)
                ).with_traceback(sys.exc_info()[2])

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

                    value_ = function.get('value', '')
                    new_col = value_.format(**row)

                    expression_true = parse_pyparser_result(row_counter, expression, row, missing_data_values)
                    if expression_true:
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

