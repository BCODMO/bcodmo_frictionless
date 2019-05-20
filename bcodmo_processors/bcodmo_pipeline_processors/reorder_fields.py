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
        if (resources.match(resource_['name'])):
            datapackage_fields = resource_['schema']['fields']
            datapackage_field_names = [o['name'] for o in datapackage_fields]

            new_fields_list = []
            for field in fields:
                try:
                    field_index = datapackage_field_names.index(field)
                except ValueError:
                    raise Exception(f'Field {field} not found in the list of fields: {datapackage_field_names}')
                new_fields_list.append(datapackage_fields[field_index])

            if len(new_fields_list) != len(datapackage_fields):
                raise Exception(f'Only {len(new_fields_list)} were passed in, {len(datapackage_fields)} required')
            resource_['schema']['fields'] = new_fields_list
    return datapackage_

spew(modify_datapackage(datapackage), resource_iterator)
