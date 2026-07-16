from dataflows.helpers.resource_matcher import ResourceMatcher
import logging

from dataflows import Flow

from bcodmo_frictionless.bcodmo_pipeline_processors.timing import StepTimer


def reorder_fields(fields, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_fields = resource["schema"]["fields"]
                package_field_names = [o["name"] for o in package_fields]

                new_fields_list = []
                for field in fields:
                    try:
                        field_index = package_field_names.index(field)
                    except ValueError:
                        raise Exception(
                            f"Field {field} not found in the list of fields: {package_field_names}"
                        )
                    new_fields_list.append(package_fields[field_index])

                if len(new_fields_list) != len(package_fields):
                    raise Exception(
                        f"Only {len(new_fields_list)} were passed in to the reorder_fields step, {len(package_fields)} required"
                    )
                resource["schema"]["fields"] = new_fields_list
        yield package.pkg
        # reorder_fields only reshuffles the schema field order; the row dicts
        # pass through untouched, so work= should be ~0. It is timed anyway so
        # every step in the pipeline reports a line.
        for rows in package:
            if matcher.match(rows.res.name):
                timer = StepTimer("reorder_fields", rows.res.name)
                yield timer.wrap(timer.rows(rows))
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        reorder_fields(
            parameters.get("fields", []),
            resources=parameters.get("resources", None),
        )
    )
