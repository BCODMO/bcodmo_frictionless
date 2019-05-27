from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
from datetime import datetime, timedelta
from dateutil.tz import tzoffset
import logging
import pytz
import re

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
            new_fields = [{
                'name': f['output_field'],
                'type': 'string',
            } for f in fields]
            resource_['schema']['fields'] += new_fields
    return datapackage_


def process_resource(rows, missing_data_values):
    row_counter = 0
    for row in rows:
        row_counter += 1
        for field in fields:
            # Inititalize all of the parameters that are used by both python and excel input_type
            output_field = field.get('output_field', None)
            if not output_field:
                raise Exception('Output field name is required')
            output_format = field.get('output_format', None)
            if not output_format:
                raise Exception('Output format is required')

            if 'input_type' not in field or field['input_type'] == 'python':
                row_value = None
                # Support multiple input fields
                if 'input_fields' in field:
                    input_fields = field['input_fields']
                    for input_field in input_fields:
                        if input_field not in row:
                            raise Exception(f'Input field {input_field} not found in row {row_counter}: {row}')
                    row_value = ' '.join([
                        row[input_field] for input_field in input_fields
                        if row[input_field] and row[input_field] not in missing_data_values
                    ])
                # Backwards compatability with a single input field
                elif 'input_field' in field:
                    input_field = field['input_field']
                    if input_field not in row:
                        raise Exception(f'Input field {input_field} not found in row {row_counter}: {row}')
                    row_value = row[input_field]
                else:
                    raise Exception('One of input_field or input_fields is required')

                if row_value in missing_data_values or row_value is None:
                    row[output_field] = row_value
                    continue
                row_value = str(row_value)

                input_format = field['input_format']

                input_timezone = field.get('input_timezone', None)
                input_timezone_utc_offset = field.get('input_timezone_utc_offset', None)
                output_timezone = field.get('output_timezone', None)
                output_timezone_utc_offset = field.get('output_timezone_utc_offset', None)


                year = field.get('year', None)

                date_obj = datetime.strptime(row_value, input_format)
                logger.info(f'inputtimezone: {input_timezone}, {bool(input_timezone)}')
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
                    output_date_string = date_obj.strftime(output_format)
                elif output_timezone == 'UTC' and output_timezone_utc_offset:
                    # Handle UTC offset timezones differently
                    output_date_string = date_obj.astimezone(
                        tzoffset('UTC', output_timezone_utc_offset * 60 * 60)
                    ).strftime(output_format)
                else:
                    output_timezone_obj = pytz.timezone(output_timezone)
                    output_date_string = date_obj.astimezone(output_timezone_obj).strftime(output_format)
                # Python datetime uses UTC as the timezone string, ISO requires Z
                if output_timezone == 'UTC':
                    output_date_string = output_date_string.replace('UTC', 'Z')
                row[output_field] = output_date_string

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
                        raise Exception(f'Input field {input_field} not found in row {row_counter}: {row}')
                else:
                    raise Exception('input_field is required when input_type is excel')

                row_value = row[input_field]
                if row_value in missing_data_values or row_value is None:
                    row[output_field] = row_value
                    continue

                try:
                    row_value = float(row_value)
                except ValueError:
                    raise Exception(f'Row value {row_value} at row {row_counter} could not be converted to a number')
                date_obj = EXCEL_START_DATE + timedelta(days=row_value)
                output_date_string = date_obj.strftime(output_format)
                row[output_field] = output_date_string

            else:
                raise Exception(f'Invalid input {field["input_type"]}')


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
                    missing_data_values = resource_datapackage.get(
                        'schema', {},
                    ).get(
                        'missingValues', ['']
                    )
                    break
            yield process_resource(resource, missing_data_values)


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
