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

def modify_datapackage(datapackage_):
    dp_resources = datapackage_.get('resources', [])
    for resource_ in dp_resources:
        if resources.match(resource_['name']):
            new_fields = [{
                'name': f['output_field'],
                'type': 'number',
            } for f in fields]
            resource_['schema']['fields'] += new_fields
    return datapackage_


def process_resource(rows, missing_data_values):
    for row in rows:
        for field in fields:
            input_field = field['input_field']
            if input_field not in row:
                raise Exception(f'Input field {input_field} not found in row')
            row_value = row[input_field]
            output_field = field['output_field']


            if row_value in missing_data_values or row_value is None:
                row[output_field] = row_value
                continue
            row_value = str(row_value)

            # If directional is user inputted, get it
            directional = 'directional' in field and field['directional']

            pattern = field['pattern']
            input_format = field['format']
            match = re.search(pattern, row_value)
            # Ensure there is a match
            if not match:
                raise Exception(f'Match not found for expression \"{pattern}\" and value \"{row_value}\"')

            # Get the degrees value
            try:
                degrees = float(match.group('degrees'))
            except IndexError:
                raise Exception(f'The degrees group is required in the expression \"{pattern}\"')
            except ValueError:
                raise Exception(f'Couldn\'t convert "{match.group("degrees")}" to a number: from line "{row_value}"')

            # Get the directional value
            if directional in field:
                directional = field['directional']
            else:
                try:
                    directional = match.group('directional')
                except IndexError:
                    directional = None


            # Input is degrees, minutes, seconds
            if input_format == 'degrees-minutes-seconds':

                # Get the minutes value
                try:
                    minutes = float(match.group('minutes'))
                except IndexError:
                    raise Exception(f'The minutes group is required in the expression \"{pattern}\"')
                except ValueError:
                    raise Exception(f'Couldn\'t convert "{match.group("minutes")}" to a number: from line "{row_value}"')

                # Get the seconds value
                try:
                    seconds = float(match.group('seconds'))
                except IndexError:
                    raise Exception(f'The seconds group is required in the expression \"{pattern}\"')
                except ValueError:
                    raise Exception(f'Couldn\'t convert "{match.group("seconds")}" to a number: from line "{row_value}"')

                if seconds >= 60:
                    raise Exception(f'Seconds are greater than 60: {seconds}')
                decimal_minutes = minutes + (seconds / 60)

            # Input is degrees, decimal seconds
            elif input_format == 'degrees-decimal_minutes':
                # Get the decimal_minutes value
                try:
                    decimal_minutes = float(match.group('decimal_minutes'))
                except IndexError:
                    raise Exception(f'The decimal_minutes group is required in the expression \"{pattern}\"')
                except ValueError:
                    raise Exception(f'Couldn\'t convert "{match.group("decimal_minutes")}" to a number: from line "{row_value}"')

            if decimal_minutes >= 60:
                raise Exception(f'Decimal minutes are greater than 60: {decimal_minutes}')

            if degrees < 0:
                decimal_degrees = degrees - (decimal_minutes / 60)
            else:
                # TODO: is it always true that decimal_minutes will be positive?
                decimal_degrees = degrees + (decimal_minutes / 60)

            if (directional == 'W' or directional == 'S') and decimal_degrees >= 0:
                decimal_degrees *= -1

            row[output_field] = decimal_degrees

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
