import sys
from datapackage_pipelines.wrapper import ingest, spew
from dataflows.helpers.resource_matcher import ResourceMatcher
import logging

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
            for field in fields:
                old_field_name = field['old_field']
                new_field_name = field['new_field']
                datapackage_fields = resource_['schema']['fields']
                for datapackage_field in datapackage_fields:
                    if datapackage_field['name'] == old_field_name:
                        datapackage_field['name'] = new_field_name
                resource_['schema']['fields'] = datapackage_fields

    return datapackage_

def process_resource(rows):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            for field in fields:
                old_field_name = field['old_field']
                new_field_name = field['new_field']
                if new_field_name in row:
                    raise Exception(f'New field name {new_field_name} already exists in row {row.keys()}')
                value = row[old_field_name]
                row[new_field_name] = value
                del row[old_field_name]
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
            yield process_resource(resource)


spew(modify_datapackage(datapackage), process_resources(resource_iterator))
