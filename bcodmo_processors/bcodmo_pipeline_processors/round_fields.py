from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging

parameters, datapackage, resource_iterator = ingest()

resources = ResourceMatcher(parameters.get('resources'), datapackage)
fields = parameters.get('fields', [])


def process_resource(rows, missing_data_values):
    for row in rows:
        for field in fields:
            orig_val = row[field['name']]
            if orig_val in missing_data_values or orig_val is None:
                row[field['name']] = orig_val
                continue
            rounded_val = round(float(orig_val), int(field['digits']))
            # Convert the rounded val back to the original type
            new_val = type(orig_val)(rounded_val)

            row[field['name']] = new_val
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


spew(datapackage, process_resources(resource_iterator))
