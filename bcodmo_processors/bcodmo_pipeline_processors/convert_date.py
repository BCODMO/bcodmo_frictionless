import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
from datetime import datetime, timedelta
from dateutil.tz import tzoffset
import logging
import pytz
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

EXCEL_START_DATE = datetime(1899, 12, 30)

def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            datapackage_fields = resource_['schema']['fields']
            new_field_names = [f['output_field'] for f in fields]
            datapackage_fields = [
                f for f in datapackage_fields if f['name'] not in new_field_names
            ]
            new_fields = [{
                'name': f['output_field'],
                'type': 'datetime',
                'outputFormat': f['output_format']
            } for f in fields]
            datapackage_fields += new_fields
            resource_['schema']['fields'] = datapackage_fields
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
                    raise Exception('Output field name is required')
                output_format = field.get('output_format', None)
                if not output_format:
                    raise Exception('Output format is required')
                if not line_passed:
                    if output_field in row:
                        row[output_field] = row[output_field]
                    else:
                        row[output_field] = None

                elif 'input_type' not in field or field['input_type'] == 'python':
                    row_value = ''
                    input_format = ''
                    # Support multiple input fields
                    if 'inputs' in field:
                        inputs = field['inputs']
                        for input_d in inputs:
                            input_field = input_d['field']
                            if input_field not in row:
                                raise Exception(f'Input field {input_field} not found: {row}')
                            if row[input_field] in missing_data_values or row[input_field] is None:
                                # There is a value in missing_data_values
                                # per discussion with data managers, set entire row to None
                                row_value = None
                                break

                            row_value += f' {row[input_field]}'
                            input_format += f' {input_d["format"]}'

                    # Backwards compatability with a single input field
                    elif 'input_field' in field:
                        input_field = field['input_field']
                        if input_field not in row:
                            raise Exception(f'Input field {input_field} not found: {row}')
                        row_value = row[input_field]
                        if 'input_format' not in field:
                            raise Exception('If using depecrated input_field for python input_type you must pass in input_format')
                        input_format = field['input_format']
                    else:
                        raise Exception('One of input_field or inputs is required')

                    if row_value in missing_data_values or row_value is None:
                        row[output_field] = row_value
                        continue
                    row_value = str(row_value)


                    input_timezone = field.get('input_timezone', None)
                    input_timezone_utc_offset = field.get('input_timezone_utc_offset', None)
                    output_timezone = field.get('output_timezone', None)
                    output_timezone_utc_offset = field.get('output_timezone_utc_offset', None)


                    year = field.get('year', None)

                    try:
                        date_obj = datetime.strptime(row_value, input_format)
                    except ValueError as e:
                        if year and str(e) == 'day is out of range for month':
                            # Possible leap year date without year in string
                            date_obj = datetime.strptime(
                                f'{year} {row_value}',
                                f'%Y {input_format}'
                            )
                        else:
                            raise e
                    if not date_obj.tzinfo:
                        if not input_timezone:
                            if '%Z' in output_format:
                                raise Exception(
                                    f'%Z cannot be in the output format if timezone information is not inputted'
                                )
                            if output_timezone:
                                raise Exception(
                                    f'Output timezone cannot be inputted without input timezone information'
                                )
                        elif input_timezone == 'UTC' and input_timezone_utc_offset:
                            # Handle UTC offset timezones differently
                            date_obj = date_obj.replace(tzinfo=tzoffset('UTC', input_timezone_utc_offset * 60 * 60))
                        else:
                            input_timezone_obj = pytz.timezone(input_timezone)
                            date_obj = input_timezone_obj.localize(date_obj)

                    if year:
                        date_obj = date_obj.replace(year=int(year))
                    if not output_timezone:
                        output_date_obj = date_obj
                    elif output_timezone == 'UTC' and output_timezone_utc_offset:
                        # Handle UTC offset timezones differently
                        output_date_obj = date_obj.astimezone(
                            tzoffset('UTC', output_timezone_utc_offset * 60 * 60)
                        )
                    else:
                        output_timezone_obj = pytz.timezone(output_timezone)
                        output_date_obj = date_obj.astimezone(output_timezone_obj)
                    # TODO: Fix this for output_date_obj, probably as a dump_to_path parameter
                    '''
                    # Python datetime uses UTC as the timezone string, ISO requires Z
                    if output_timezone == 'UTC':
                        output_date_obj = output_date_string.replace('UTC', 'Z')
                    '''
                    row[output_field] = output_date_obj

                elif field['input_type'] == 'excel':
                    '''
                    Handle excel number datetimes

                    takes in input_field, output_field and output_format
                    but does not take any other parameters from python input_type
                    '''
                    row_value = None
                    if 'input_field' in field:
                        input_field = field['input_field']
                        if input_field not in row:
                            raise Exception(f'Input field {input_field} not found: {row}')
                    else:
                        raise Exception('input_field is required when input_type is excel')

                    row_value = row[input_field]
                    if row_value in missing_data_values or row_value is None:
                        row[output_field] = row_value
                        continue

                    try:
                        row_value = float(row_value)
                    except ValueError:
                        raise Exception(f'Row value {row_value} could not be converted to a number')
                    output_date_obj = EXCEL_START_DATE + timedelta(days=row_value)
                    row[output_field] = output_date_obj

                else:
                    raise Exception(f'Invalid input {field["input_type"]}')


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
