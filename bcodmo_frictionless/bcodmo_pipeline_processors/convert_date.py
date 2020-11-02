import sys
from datetime import time, date, datetime, timedelta
from dateutil.tz import tzoffset
from decimal import Decimal, InvalidOperation
import logging
import pytz
import re
import math

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow


from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


EXCEL_START_DATE = datetime(1899, 12, 30)


def is_leap(year):
    return (year % 4) == 0 and ((year % 100) != 0 or (year % 400) == 0)


def process_resource(
    rows, fields, missing_values, datapackage_fields, boolean_statement=None
):
    expression = get_expression(boolean_statement)
    row_counter = 0
    for row in rows:
        row_counter += 1
        new_row = dict((k, v) for k, v in row.items())

        line_passed = check_line(expression, row_counter, new_row, missing_values)
        try:
            for field in fields:
                # Inititalize all of the parameters that are used by both python and excel input_type
                output_field = field.get("output_field", None)
                if not output_field:
                    raise Exception("Output field name is required")
                output_format = field.get("output_format", None)
                if not output_format:
                    raise Exception("Output format is required")
                output_type = field.get("output_type", "datetime")
                if output_type not in ["datetime", "date", "time"]:
                    raise Exception("Output type must be one of datetime, date or time")

                if not line_passed:
                    if output_field in new_row:
                        new_row[output_field] = new_row[output_field]
                    else:
                        new_row[output_field] = None

                elif "input_type" not in field or field["input_type"] == "python":
                    row_value = ""
                    input_format = ""
                    # Support multiple input fields
                    if "inputs" in field:
                        inputs = field["inputs"]
                        for input_d in inputs:
                            input_field = input_d["field"]
                            if input_field not in new_row:
                                raise Exception(
                                    f"Input field {input_field} not found: {new_row}"
                                )
                            val = new_row[input_field]
                            if val in missing_values or val is None:
                                # There is a value in missing_values
                                # per discussion with data managers, set entire row to None
                                row_value = None
                                break
                            if not input_d["format"]:
                                raise Exception(
                                    f"Format for input field {input_field} is empty"
                                )
                            if type(val) in (datetime, date, time):
                                input_datapackage_field = None
                                for f in datapackage_fields:
                                    if f.name == input_field:
                                        input_datapackage_field = f
                                        break
                                if "outputFormat" in input_datapackage_field.descriptor:
                                    str_format = input_datapackage_field.descriptor[
                                        "outputFormat"
                                    ]
                                    input_string = val.strftime(str_format)
                                else:
                                    intput_string = str(val)

                            else:
                                input_string = str(val)

                            row_value += f" {input_string}"
                            input_format += f' {input_d["format"]}'

                    # Backwards compatability with a single input field
                    elif "input_field" in field:
                        input_field = field["input_field"]
                        if input_field not in new_row:
                            raise Exception(
                                f"Input field {input_field} not found: {new_row}"
                            )
                        row_value = new_row[input_field]
                        if "input_format" not in field:
                            raise Exception(
                                "If using depecrated input_field for python input_type you must pass in input_format"
                            )
                        input_format = field["input_format"]
                    else:
                        raise Exception("One of input_field or inputs is required")

                    if row_value in missing_values or row_value is None:
                        new_row[output_field] = row_value
                        continue
                    row_value = str(row_value)

                    input_timezone = field.get("input_timezone", None)
                    input_timezone_utc_offset = field.get(
                        "input_timezone_utc_offset", None
                    )
                    output_timezone = field.get("output_timezone", None)
                    output_timezone_utc_offset = field.get(
                        "output_timezone_utc_offset", None
                    )

                    year = field.get("year", None)

                    try:
                        date_obj = datetime.strptime(row_value, input_format)
                    except ValueError as e:
                        if year and str(e) == "day is out of range for month":
                            # Possible leap year date without year in string
                            date_obj = datetime.strptime(
                                f"{year} {row_value}", f"%Y {input_format}"
                            )
                        else:
                            raise e
                    if not date_obj.tzinfo:
                        if not input_timezone:
                            if "%Z" in output_format:
                                raise Exception(
                                    f"%Z cannot be in the output format if timezone information is not inputted"
                                )
                            if output_timezone:
                                raise Exception(
                                    f"Output timezone cannot be inputted without input timezone information"
                                )
                        elif input_timezone == "UTC" and input_timezone_utc_offset:
                            # Handle UTC offset timezones differently
                            date_obj = date_obj.replace(
                                tzinfo=tzoffset(
                                    "UTC", input_timezone_utc_offset * 60 * 60
                                )
                            )
                        else:
                            input_timezone_obj = pytz.timezone(input_timezone)
                            date_obj = input_timezone_obj.localize(date_obj)

                    if year:
                        date_obj = date_obj.replace(year=int(year))
                    if not output_timezone:
                        output_date_obj = date_obj
                    elif output_timezone == "UTC" and output_timezone_utc_offset:
                        # Handle UTC offset timezones differently
                        output_date_obj = date_obj.astimezone(
                            tzoffset("UTC", output_timezone_utc_offset * 60 * 60)
                        )
                    else:
                        output_timezone_obj = pytz.timezone(output_timezone)
                        output_date_obj = date_obj.astimezone(output_timezone_obj)
                    # TODO: Fix this for output_date_obj, probably as a dump_to_path parameter
                    """
                    # Python datetime uses UTC as the timezone string, ISO requires Z
                    if output_timezone == 'UTC':
                        output_date_obj = output_date_string.replace('UTC', 'Z')
                    """
                    new_row[output_field] = output_date_obj

                elif (
                    field["input_type"] == "excel"
                    or field["input_type"] == "matlab"
                    or field["input_type"] == "decimalDay"
                    or field["input_type"] == "decimalYear"
                ):
                    """
                    Handle excel number, decimal as day of year, and decimal as day and year in time datetimes

                    takes in input_field, output_field and output_format
                        - decimalDay also takes in Year
                    Does not take any other parameters from python input_type
                    """
                    row_value = None
                    if "input_field" in field:
                        input_field = field["input_field"]
                        if input_field not in new_row:
                            raise Exception(
                                f"Input field {input_field} not found: {new_row}"
                            )
                    else:
                        raise Exception(
                            "input_field is required when input_type is excel"
                        )

                    row_value = new_row[input_field]
                    if row_value in missing_values or row_value is None:
                        new_row[output_field] = row_value
                        continue

                    # Convert the row to a number
                    try:
                        if (
                            field["input_type"] == "excel"
                            or field["input_type"] == "matlab"
                        ):
                            row_value = float(row_value)
                        else:
                            row_value = Decimal(row_value)
                    except (ValueError, InvalidOperation):
                        raise Exception(
                            f"Row value {row_value} could not be converted to a number"
                        )

                    # Do the math to convert to a date
                    if field["input_type"] == "excel":
                        output_date_obj = EXCEL_START_DATE + timedelta(days=row_value)
                    elif field["input_type"] == "matlab":
                        day = datetime.fromordinal(int(row_value))
                        dayfrac = timedelta(days=row_value % 1) - timedelta(days=366)
                        output_date_obj = day + dayfrac
                    elif field["input_type"] == "decimalYear":
                        start_day = field.get("decimal_year_start_day", None)
                        if not start_day:
                            raise Exception(
                                f'"decimal_year_start_day" must be specified if input type is decimalYear'
                            )
                        try:
                            start_day = int(start_day)

                        except Exception as e:
                            raise Exception(
                                f"decimal_year_start_day {start_day} could not be converted to an integer"
                            )
                        # Handle decimalYear
                        year = int(row_value)
                        decimal_part = float(row_value) - year
                        d = timedelta(
                            days=(decimal_part * (365 + (1 if is_leap(year) else 0)))
                        ) - timedelta(days=start_day)
                        day_one = datetime(year, 1, 1)
                        output_date_obj = d + day_one
                    else:
                        year = field.get("year", None)
                        if not year:
                            raise Exception(
                                "Year is required for decimal day input type"
                            )
                        # Function to get the left and right side of a number in separate variables
                        f = lambda x: (math.floor(x), x - math.floor(x))

                        # Handle Days
                        days, x = f(row_value)
                        # Handle hours
                        hours, x = f(x * 24)
                        # Handle minutes
                        minutes, x = f(x * 60)
                        # Handle seconds
                        seconds, x = f(x * 60)
                        # Handle microseconds
                        microseconds, _ = f(x * 1000000)

                        # Have to use strptime because its the only function that takes in day of year (no month)
                        output_date_obj = datetime.strptime(
                            f"{year}:{days}:{hours}:{minutes}:{seconds}:{microseconds}",
                            "%Y:%j:%H:%M:%S:%f",
                        )

                    # Set output field
                    new_row[output_field] = output_date_obj

                else:
                    raise Exception(f'Invalid input {field["input_type"]}')
                if (
                    new_row[output_field]
                    and new_row[output_field] not in missing_values
                ):
                    if output_type == "date":
                        new_row[output_field] = new_row[output_field].date()
                    if output_type == "time":
                        new_row[output_field] = new_row[output_field].time()

            yield new_row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def convert_date(fields, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_fields = resource["schema"]["fields"]

                # Create a list of names and a lookup dict for the new fields
                new_field_names = [f["output_field"] for f in fields]
                new_fields_dict = {
                    f["output_field"]: {
                        "name": f["output_field"],
                        "type": f.get("output_type", "datetime"),
                        "outputFormat": f["output_format"],
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
                    rows,
                    fields,
                    missing_values,
                    rows.res.schema.fields,
                    boolean_statement=boolean_statement,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        convert_date(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
