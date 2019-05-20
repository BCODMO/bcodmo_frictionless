from dataflows import Flow, load
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.resources import PROP_STREAMING, PROP_STREAMED_FROM

# Import custom parsers here
from parsers import FixedWidthParser
# Add custom parsers here
# Custom parsers should NOT have periods in their name
custom_parsers = {
    'bcodmo-fixedwidth': FixedWidthParser,
}


def flow(parameters, datapackage):
    _from = parameters.pop('from')
    # Grab missing_values from the parameters
    _missing_values = parameters.pop('missing_values', [''])

    num_resources = 0

    def count_resources():
        def func(package):
            global num_resources
            num_resources = len(package.pkg.resources)
            yield package.pkg
            yield from package
        return func

    # Default the name to res[1-n]
    if 'name' not in parameters:
        resource_name_num = len(datapackage['resources']) + 1
        resource_name = f'res{resource_name_num}'
        while resource_name in [res['name'] for res in datapackage['resources']]:
            resource_name_num += 1
            resource_name = f'res{resource_name_num}'
        parameters['name'] = resource_name

    _name = parameters['name']

    def mark_streaming(_from):
        def func(package):
            for i in range(num_resources, len(package.pkg.resources)):
                if package.pkg.descriptor['resources'][i]['name'] == _name:
                    package.pkg.descriptor['resources'][i]['schema']['missingValues'] = _missing_values
                    package.pkg.descriptor['resources'][i][PROP_STREAMING] = True
                    package.pkg.descriptor['resources'][i][PROP_STREAMED_FROM] = _from
            yield package.pkg
            yield from package
        return func



    return Flow(
        count_resources(),
        load(_from, custom_parsers = custom_parsers, **parameters),
        mark_streaming(_from),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.datapackage), ctx)
