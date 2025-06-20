import sys
import logging
import re
from decimal import Decimal

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def process_resource(rows, fields, missing_values, boolean_statement=None):
    expression = get_expression(boolean_statement)
    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_values)
        try:
            for field in fields:
                input_field = field["input_field"]
                if input_field not in row:
                    raise Exception(f"Input field {input_field} not found: {row}")
                row_value = row[input_field]
                output_field = field["output_field"]
                handle_ob = field.get("handle_out_of_bounds", False)

                if not line_passed:
                    if output_field in row:
                        row[output_field] = row[output_field]
                    else:
                        row[output_field] = None
                    continue

                if row_value in missing_values or row_value is None:
                    row[output_field] = row_value
                    continue
                row_value = str(row_value)

                pattern = field["pattern"]
                input_format = field["format"]
                match = re.search(pattern, row_value)
                # Ensure there is a match
                if not match:
                    raise Exception(
                        f'Match not found for expression "{pattern}" and value "{row_value}"'
                    )

                # Get the degrees value
                try:
                    degrees = Decimal(match.group("degrees"))
                except IndexError:
                    raise Exception(
                        f'The degrees group is required in the expression "{pattern}"'
                    )
                except ValueError:
                    raise Exception(
                        f'Couldn\'t convert "{match.group("degrees")}" to a number: from line "{row_value}"'
                    )

                # Get the directional value
                if "directional" in field and field["directional"]:
                    directional = field["directional"]
                else:
                    try:
                        directional = match.group("directional")
                    except IndexError:
                        directional = None

                # Input is degrees, minutes, seconds
                if input_format == "degrees-minutes-seconds":
                    # Get the minutes value
                    try:
                        minutes = Decimal(match.group("minutes"))
                    except IndexError:
                        raise Exception(
                            f'The minutes group is required in the expression "{pattern}"'
                        )
                    except ValueError:
                        raise Exception(
                            f'Couldn\'t convert "{match.group("minutes")}" to a number: from line "{row_value}"'
                        )

                    # Get the seconds value
                    try:
                        seconds = Decimal(match.group("seconds"))
                    except IndexError:
                        raise Exception(
                            f'The seconds group is required in the expression "{pattern}"'
                        )
                    except ValueError:
                        raise Exception(
                            f'Couldn\'t convert "{match.group("seconds")}" to a number: from line "{row_value}"'
                        )

                    if seconds >= 60:
                        if handle_ob:
                            minutes += int(seconds / 60)
                            seconds = seconds % 60
                        else:
                            raise Exception(
                                f"Seconds are greater or equal to 60: {seconds}. Set the handle_out_of_bounds flag to true to handle this case"
                            )
                    decimal_minutes = minutes + (seconds / 60)

                # Input is degrees, decimal seconds
                elif input_format == "degrees-decimal_minutes":
                    # Get the decimal_minutes value
                    try:
                        decimal_minutes = Decimal(match.group("decimal_minutes"))
                    except IndexError:
                        raise Exception(
                            f'The decimal_minutes group is required in the expression "{pattern}"'
                        )
                    except ValueError:
                        raise Exception(
                            f'Couldn\'t convert "{match.group("decimal_minutes")}" to a number: from line "{row_value}"'
                        )
                else:
                    raise Exception(
                        '"format" must be one of "degrees-minutes-seconds" or "degrees-decimal_minutes"'
                    )

                if decimal_minutes >= 60:
                    if handle_ob:
                        degrees += decimal_minutes / 60
                        decimal_minutes = decimal_minutes % 60

                    else:
                        raise Exception(
                            f"Decimal minutes or minutes are greater or equal to 60 ({decimal_minutes}). Set the handle_out_of_bounds flag to true to handle this case"
                        )

                if degrees < 0:
                    decimal_degrees = degrees - (decimal_minutes / 60)
                else:
                    # TODO: is it always true that decimal_minutes will be positive?
                    decimal_degrees = degrees + (decimal_minutes / 60)

                if (directional == "W" or directional == "S") and decimal_degrees >= 0:
                    decimal_degrees *= -1

                if decimal_degrees > 180:
                    if handle_ob:
                        while decimal_degrees > 180:
                            decimal_degrees -= 360
                    else:
                        raise Exception(
                            f"Decimal degrees greater than 180 ({decimal_degrees}). Set the handle_out_of_bounds flag to true to handle this case"
                        )

                if decimal_degrees < -180:
                    if handle_ob:
                        while decimal_degrees < -180:
                            decimal_degrees += 360
                    else:
                        raise Exception(
                            f"Decimal degrees less than 180 ({decimal_degrees}). Set the handle_out_of_bounds flag to true to handle this case"
                        )
                row[output_field] = decimal_degrees

            yield row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def convert_to_decimal_degrees(fields, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                # Get the old fields
                package_fields = resource["schema"]["fields"]

                # Create a list of names and a lookup dict for the new fields
                new_field_names = [f["output_field"] for f in fields]
                new_fields_dict = {
                    f["output_field"]: {
                        "name": f["output_field"],
                        "type": "number",
                    }
                    for f in fields
                }

                # Iterate through the old fields, updating where necessary to maintain order
                processed_fields = []
                for f in package_fields:
                    if f["name"] in new_field_names:
                        processed_fields.append(new_fields_dict[f["name"]])
                        new_field_names.remove(f["name"])
                    else:
                        processed_fields.append(f)
                # Add new fields that were not added through the update
                for fname in new_field_names:
                    processed_fields.append(new_fields_dict[fname])

                # Add back to the datapackage
                resource["schema"]["fields"] = processed_fields

        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(
                    rows, fields, missing_values, boolean_statement=boolean_statement
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        convert_to_decimal_degrees(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )
