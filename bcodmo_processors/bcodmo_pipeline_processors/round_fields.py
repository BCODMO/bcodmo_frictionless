import sys
import logging
from decimal import Decimal
from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from bcodmo_processors.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from bcodmo_processors.bcodmo_pipeline_processors.helper import (
    get_missing_values,
)


def remove_trailing_zeros(f):
    d = Decimal(str(f))
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def process_resource(rows, fields, missing_values, boolean_statement=None):
    expression = get_expression(boolean_statement)
    schema = rows.res.descriptor["schema"]

    row_counter = 0
    for row in rows:
        row_counter += 1

        line_passed = check_line(expression, row_counter, row, missing_values)
        if line_passed:
            try:
                for field in fields:
                    cur_schema_field = {}
                    for schema_field in schema.get("fields", []):
                        if schema_field["name"] == field["name"]:
                            cur_schema_field = schema_field
                            break
                    # Check if the type in the datapackage is a number
                    if cur_schema_field["type"] == "number" or (
                        cur_schema_field["type"] == "integer"
                        and field["digits"] == 0
                        and field.get("convert_to_integer", False)
                    ):
                        orig_val = row[field["name"]]
                        if orig_val in missing_values or orig_val is None:
                            row[field["name"]] = orig_val
                            continue
                        orig_type = type(orig_val)
                        if orig_type is not Decimal:
                            orig_val = Decimal(str(orig_val))
                        digits = int(field.get("digits"))

                        if field.get("maximum_precision", False):
                            # Find the current precision and ignore this row if it's lower than digits
                            current_precision = orig_val.as_tuple().exponent * -1
                            if current_precision < digits:
                                continue

                        rounded_val = round(orig_val, digits)

                        if not field.get("preserve_trailing_zeros", False):
                            rounded_val = remove_trailing_zeros(rounded_val)

                        if (
                            field.get("convert_to_integer", False)
                            and field["digits"] == 0
                        ):
                            rounded_val = int(rounded_val)

                        row[field["name"]] = rounded_val
                    else:
                        raise Exception(
                            f'Attempting to convert a field ("{field["name"]}") that has not been cast to a number'
                        )
                yield row
            except Exception as e:
                raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                    sys.exc_info()[2]
                )
        else:
            yield row


def round_fields(fields, resources=None, boolean_statement=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_fields = resource["schema"]["fields"]
                for field in fields:
                    if field.get("convert_to_integer", False) and field["digits"] == 0:
                        if field.get("digits") is not 0:
                            raise Exception(
                                'The "convert_to_integer" flag cannot be used when "digits" is not 0'
                            )
                        # Convert to integer
                        for package_field in package_fields:
                            if package_field["name"] == field["name"]:
                                if package_field["type"] != "number":
                                    raise Exception(
                                        f'Tried to convert a field ("{field["name"]}") that has not been cast to a number: {field["type"]}. Make sure all fields used with the round_fields processor are numbers.'
                                    )
                                package_field["type"] = "integer"

                resource["schema"]["fields"] = package_fields
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
        round_fields(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
            boolean_statement=parameters.get("boolean_statement"),
        )
    )

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)

