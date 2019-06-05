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
    _input_seperator = parameters.pop('input_seperator', ',')
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

    def mark_streaming(_from, _name):
        def func(package):
            for i in range(num_resources, len(package.pkg.resources)):
                if package.pkg.descriptor['resources'][i]['name'] == _name:
                    package.pkg.descriptor['resources'][i]['schema']['missingValues'] = _missing_values
                    package.pkg.descriptor['resources'][i][PROP_STREAMING] = True
                    package.pkg.descriptor['resources'][i][PROP_STREAMED_FROM] = _from
            yield package.pkg
            yield from package
        return func



    params = []
    _name = parameters.pop('name', '')
    name_len = len(_name.split(','))
    from_len = len(_from.split(','))
    if _name and name_len is not from_len:
        raise Exception(
            f'The comma seperated list of names has length {name_len} and the list of urls has length {from_len}. They must be equal'
        )
    names = []
    if not _name:
        for i in range(from_len):
            resource_name_num = len(datapackage['resources']) + 1 + i
            resource_name = f'res{resource_name_num}'
            while resource_name in [res['name'] for res in datapackage['resources']] or resource_name in names:
                resource_name_num += 1
                resource_name = f'res{resource_name_num}'
            names.append(resource_name)
    else:
        names = _name.split(_input_seperator)

    # Get comma seperated file names/urls
    for i, url in enumerate(_from.split(',')):
        # Default the name to res[1-n]
        resource_name = names[i]

        params.extend([
            load(url, custom_parsers = custom_parsers, name = resource_name, **parameters),
            mark_streaming(url, resource_name),
        ])

    return Flow(
        count_resources(),
        *params,
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.datapackage), ctx)
