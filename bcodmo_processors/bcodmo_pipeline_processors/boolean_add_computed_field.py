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

# Set up a language logic for parsing boolean strings
operator = pp.Regex(">=|<=|!=|>|<|==").setName("operator")
number = pp.Regex(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
variable = pp.Regex(r"\{.*?\}")
date = pp.Regex("(\d+[/:\- ])+(\d+)?")
comparison_term = date | number | variable
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

def parse_pyparser_result(row_counter, res, row):
    ''' Parse a result from pyparser '''
    if type(res) == bool:
        return res
    # Try to convert to float
    try:
        return float(res)
    except (ValueError, TypeError):
        pass
    # Parse strings
    if type(res) == str:
        try:
            return float((res.format(**row)))
        except ValueError:
            try:
                val = res.format(**row)
                if (val == ''):
                    # TODO handle missing data value types
                    return 0
                return parser.parse(val)
            except ValueError:
                raise Exception(f'Failed to parse {res} into a valid number or date at row {row_counter}: {res.format(**row)}')

    if len(res) == 0:
        return False
    if len(res) == 1:
        return parse_pyparser_result(row_counter, res[0], row)
    first_value = None
    operation = None
    try:
        for term in res:
            if not first_value:
                first_value = term
            elif not operation:
                operation = term
            else:
                first_parsed = parse_pyparser_result(row_counter, first_value, row)
                second_parsed = parse_pyparser_result(row_counter, term, row)

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

def process_resource(rows):
    field_functions = []
    for index in range(len(fields)):
        field = fields[index]
        field_functions.append([])
        for function in field.get('functions', []):
            boolean_string = function.get('boolean', '')
            field_functions[index].append(
                expr.parseString(boolean_string)
            )
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

                    expression_true = parse_pyparser_result(row_counter, expression, row)
                    if expression_true:
                        row[field['target']] = new_col
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
            yield process_resource(resource)



spew(modify_datapackage(datapackage), process_resources(resource_iterator))

