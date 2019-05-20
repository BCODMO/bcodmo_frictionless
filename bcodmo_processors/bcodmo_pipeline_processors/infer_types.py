from datetime import datetime
from itertools import chain
import logging
import re
from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.wrapper.input_processor import ResourceIterator
import tableschema
logging.basicConfig(
    level=logging.WARNING,
)
logger = logging.getLogger(__name__)

parameters, datapackage, resource_iterator = ingest()


class ResourceWrapper(object):
    """
    A wrapper around a resource

    Provides functionality to peek inside resource and get the first
    value. That first value will still be returned when the Generator is
    iterated over so it is not lost

    The stream that resource (of type ResourceIterator) uses is stdin,
    which means you can't use the seek() function to move the stream
    pointer back to the beginning. This class adds that functionality
    """
    def __init__(self, resource):
        # Set all of resource_iterator's attributes to this object
        self.__dict__ = resource.__dict__.copy()
        self.peek_val = next(resource)
        self.resource = resource
        self.iterated = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self.iterated:
            self.iterated = True
            return self.peek_val
        return next(self.resource)

    def next(self):
        return self.__next__()

    def peek(self):
        return self.peek_val



def infer_table_types(row):
    """
    A function that goes through a row and infers data types for each cell

    Returns a dictionary with the col name as a key and the type as a string
    """
    types = {}
    for col, val in row.items():
        if val:
            # Check for number type
            try:
                float(val)
                types[col] = 'number'
                types[col] = {
                    'type': 'number',
                }
                continue
            except ValueError:
                pass

            # Check for date, datetime and time types
            try:
                datetime.strptime(val, '%Y-%m-%d')
                types[col] = {
                    'type': 'date',
                    'format': '%Y-%m-%d'
                }
                continue
            except ValueError:
                pass
            try:
                datetime.strptime(val, '%Y-%m-%dT%H:%M:%SZ')
                types[col] = {
                    'type': 'datetime',
                    'format': '%Y-%m-%dT%H:%M:%SZ',
                }
                continue
            except ValueError:
                pass
            try:
                datetime.strptime(val, '%H:%M:%S')
                types[col] = {
                    'type': 'time',
                    'format': '%H:%M:%S'
                }
                continue
            except ValueError:
                pass

        # Fall back to string last
        types[col] = {
            'type': 'string',
        }
    return types




# Here we infer the data types by peeking at the first row
# of the data.
table_types = {}
resources_list = []
for resource in resource_iterator:
    wrapped_resource = ResourceWrapper(resource)

    # Peek and get the first row values
    row = wrapped_resource.peek()

    table_types[wrapped_resource.spec['name']] = infer_table_types(row)
    resources_list.append(wrapped_resource)


resource_iterator = iter(resources_list)

resources = ResourceMatcher(parameters.get('resources'), datapackage)




def modify_datapackage(datapackage_):

    for resource in datapackage_['resources']:
        name = resource['name']
        if not resources.match(name):
            continue

        if 'schema' not in resource:
            continue

        fields = resource.setdefault('schema', {}).get('fields', [])
        for field_name, field_definition in table_types[name].items():
            if field_definition is not None:
                filtered_fields = [field for field in fields if field['name'] == field_name]
                for field in filtered_fields:
                    field.update(field_definition)
                assert len(filtered_fields) > 0, \
                    "No field found matching %r" % field_name

        resource['schema']['fields'] = fields
    return datapackage_


def process_resource(spec, rows):
    schema = spec['schema']
    jts = tableschema.Schema(schema)
    field_names = list(map(lambda f: f['name'], schema['fields']))
    count = 0
    for row in rows:
        count += 1
        flattened_row = [row.get(name) for name in field_names]
        try:
            flattened_row = jts.cast_row(flattened_row)
        except Exception:
            logging.error('Failed to cast row %r', flattened_row)
            raise
        row = dict(zip(field_names, flattened_row))
        yield row

def process_resources(resource_iterator_):
    for resource in resource_iterator_:
        spec = resource.spec
        if not resources.match(spec['name']):
            yield resource
        else:
            yield process_resource(spec, resource)


new_datapackage = modify_datapackage(datapackage)

spew(new_datapackage, process_resources(resource_iterator))

