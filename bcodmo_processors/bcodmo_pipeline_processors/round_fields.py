import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging
from decimal import Decimal

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

def remove_trailing_zeros(f):
    d = Decimal(str(f));
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()

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
                        orig_type = type(orig_val)
                        if orig_type is not Decimal:
                            orig_val = Decimal(str(origin_val))
                        digits = int(field.get('digits'))

                        if field.get('maximum_precision', False):
                            # Find the current precision and ignore this row if it's lower than digits
                            current_precision = orig_val.as_tuple().exponent * -1
                            if current_precision < digits:
                                continue


                        rounded_val = round(orig_val, digits)

                        if not field.get('preserve_trailing_zeros', False):
                            rounded_val = remove_trailing_zeros(rounded_val)

                        if field.get('convert_to_integer', False) and field['digits'] == 0:
                            rounded_val = int(new_val)

                        row[field['name']] = rounded_val
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
