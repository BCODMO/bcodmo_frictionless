from dataflows import Flow, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.utilities.flow_utils import spew_flow
import itertools
import os

from dataflows.helpers.resource_matcher import ResourceMatcher


def concatenator(resources, all_target_fields, field_mapping, include_source_name, source_field_name):
    for resource_ in resources:
        res_name = resource_.res.name
        path_name = resource_.res._Resource__current_descriptor['dpp:streamedFrom']
        file_name = os.path.basename(path_name)
        for row in resource_:
            processed = dict((k, '') for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                    in row.items()
                    if k in field_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            if include_source_name == 'resource':
                processed[source_field_name] = res_name
            if include_source_name == 'path':
                processed[source_field_name] = path_name
            if include_source_name == 'file':
                processed[source_field_name] = file_name
            yield processed


def concatenate(
    fields,
    target={},
    resources=None,
    include_source_name=False,
    source_field_name='source_name',
):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        # Prepare target resource
        if 'name' not in target:
            target['name'] = 'concat'
        if 'path' not in target:
            target['path'] = 'data/' + target['name'] + '.csv'
        target.update(dict(
            mediatype='text/csv',
            schema=dict(fields=[], primaryKey=[]),
        ))

        # Create mapping between source field names to target field names
        field_mapping = {}
        for target_field, source_fields in fields.items():
            if source_fields is not None:
                for source_field in source_fields:
                    if source_field in field_mapping:
                        raise RuntimeError('Duplicate appearance of %s (%r)' % (source_field, field_mapping))
                    field_mapping[source_field] = target_field

            if target_field in field_mapping:
                raise RuntimeError('Duplicate appearance of %s' % target_field)

            field_mapping[target_field] = target_field

        # Create the schema for the target resource
        needed_fields = sorted(fields.keys())
        if include_source_name and source_field_name in needed_fields:
            raise Exception(f'source_field_name "{source_field_name}" field name already exists')
        for resource in package.pkg.descriptor['resources']:
            if not matcher.match(resource['name']):
                continue

            schema = resource.get('schema', {})
            pk = schema.get('primaryKey', [])
            for field in schema.get('fields', []):
                orig_name = field['name']
                if orig_name in field_mapping:
                    name = field_mapping[orig_name]
                    if name not in needed_fields:
                        continue
                    if orig_name in pk:
                        target['schema']['primaryKey'].append(name)
                    target['schema']['fields'].append(field)
                    field['name'] = name
                    needed_fields.remove(name)
        if len(target['schema']['primaryKey']) == 0:
            del target['schema']['primaryKey']

        for name in needed_fields:
            target['schema']['fields'].append(dict(
                name=name, type='string'
            ))

        if include_source_name:
            target['schema']['fields'].append({
                'name': source_field_name,
                'type': 'string',
            })


        # Update resources in datapackage (make sure they are consecutive)
        prefix = True
        suffix = False
        num_concatenated = 0
        new_resources = []
        for resource in package.pkg.descriptor['resources']:
            name = resource['name']
            if name == target['name']:
                raise Exception(f'Name of concatenate target ({target["name"]}) cannot match an existing resource name ({name})')
            match = matcher.match(name)
            if prefix:
                if match:
                    prefix = False
                    num_concatenated += 1
                else:
                    new_resources.append(resource)
            elif suffix:
                assert not match
                new_resources.append(resource)
            else:
                if not match:
                    suffix = True
                    new_resources.append(target)
                    new_resources.append(resource)
                else:
                    num_concatenated += 1
        if not suffix:
            new_resources.append(target)

        package.pkg.descriptor['resources'] = new_resources
        yield package.pkg

        needed_fields = sorted(fields.keys())
        it = iter(package)
        for resource in it:
            if matcher.match(resource.res.name):
                resource_chain = itertools.chain(
                    [resource],
                    itertools.islice(
                        it,
                        num_concatenated - 1
                    )
                )
                yield concatenator(
                    resource_chain,
                    needed_fields,
                    field_mapping,
                    include_source_name,
                    source_field_name,
                )
            else:
                yield resource

    return func

def flow(parameters):
    return Flow(
        concatenate(
            parameters.get('fields', {}),
            parameters.get('target', {}),
            parameters.get('sources'),
            parameters.get('include_source_name', False),
            parameters.get('source_field_name', 'source_name'),
        ),
        update_resource(
            parameters.get('target', {}).get('name', 'concat'),
            **{
                PROP_STREAMING: True,
            },
        ),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)


