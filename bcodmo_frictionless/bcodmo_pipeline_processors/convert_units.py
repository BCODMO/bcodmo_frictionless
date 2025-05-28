import sys
import copy
import logging
from decimal import Decimal

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def remove_trailing_zeros(f):
    d = Decimal(str(f))
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def process_resource(
    rows,
    fields,
    missing_values,
):
    schema = rows.res.descriptor["schema"]

    row_counter = 0
    for row in rows:
        row_counter += 1

        try:
            for field in fields:
                cur_schema_field = {}
                for schema_field in schema.get("fields", []):
                    if schema_field["name"] == field["name"]:
                        cur_schema_field = schema_field
                        break
                # Check if the type in the datapackage is a number
                if cur_schema_field["type"] == "number":
                    orig_val = row[field["name"]]

                    # Handle missing values
                    if orig_val in missing_values or orig_val is None:
                        row[field["name"]] = orig_val
                        if field.get("preserve_field", False):
                            new_field_name = field.get("new_field_name", "")
                            raise Exception(
                                f'The convert_units processor is missing a new field name. The parameter "preserve_field" was set to true.'
                            )
                        row[field[new_field_name]] = orig_val
                        continue

                    # Handle typing
                    orig_type = type(orig_val)
                    if orig_type is not Decimal:
                        orig_val = Decimal(str(orig_val))

                    # Handle converting
                    conversion_function = field.get("conversion")
                    new_value = orig_val
                    # Feet to meter
                    if conversion_function == "feet_to_meter":
                        new_value *= Decimal("0.3048")
                    elif conversion_function == "fathom_to_meter":
                        new_value *= Decimal("1.8288")
                    elif conversion_function == "inch_to_cm":
                        new_value *= Decimal("2.54")
                    elif conversion_function == "mile_to_km":
                        new_value *= Decimal("1.60934")
                    else:
                        raise Exception(
                            f'The convert_units processor was passed an unknown conversion function "{conversion_function}"'
                        )

                    # Handle field naming
                    if field.get("preserve_field", False):
                        new_field_name = field.get("new_field_name", "")
                        if new_field_name == "":
                            raise Exception(
                                f'The convert_units processor is missing a new field name. The parameter "preserve_field" was set to true.'
                            )

                        row[new_field_name] = new_value
                    else:
                        row[field["name"]] = new_value

                else:
                    raise Exception(
                        f'Attempting to convert units for a field ("{field["name"]}") that has not been cast to a number'
                    )
            yield row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )


def convert_units(fields, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                field_dict = {}
                for field in fields:
                    field_dict[field["name"]] = field

                package_fields = resource["schema"]["fields"]
                new_package_fields = []
                previous_field_names = set([f["name"] for f in package_fields])
                for package_field in package_fields:
                    new_package_fields.append(package_field)
                    if package_field["name"] in field_dict:
                        field = field_dict[package_field["name"]]
                        if package_field["type"] != "number":
                            raise Exception(
                                f'Tried to convert the units of a field ("{field["name"]}") that has not been cast to a number: {field["type"]}. Make sure all fields used with the convert_units processor are numbers.'
                            )
                        if field.get("preserve_field", False):
                            # We create a new field if preserve_field is True
                            new_field_name = field["new_field_name"]
                            if (
                                new_field_name in previous_field_names
                                and new_field_name != package_field["name"]
                            ):
                                raise Exception(
                                    f'In the convert_units processor, attempted to add a new field name that already exists somewhere else: "{new_field_name}".'
                                )
                            elif new_field_name != package_field["name"]:
                                # Only create the field if it's different than the original field
                                new_package_field = copy.deepcopy(package_field)
                                new_package_field["name"] = new_field_name
                                new_package_fields.append(new_package_field)

                resource["schema"]["fields"] = new_package_fields
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(rows, fields, missing_values)
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        convert_units(
            parameters.get("fields", []),
            resources=parameters.get("resources"),
        )
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
