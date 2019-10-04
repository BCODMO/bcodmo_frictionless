import xlrd
import re
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
    _input_separator = parameters.pop('input_separator', ',')
    _remove_empty_rows = parameters.pop('remove_empty_rows', False)
    if parameters.get('format') == 'bcodmo-fixedwidth':
        # With fixed width files, we want to also send the sample_size
        # and skip_rows parameters to the parser (not just the stream)
        parameters['fixedwidth_sample_size'] = parameters.get('sample_size', 100)
        parameters['fixedwidth_skip_header'] = [v for v in parameters.get('skip_rows', []) if type(v) == str]


    if parameters.get('parse_seabird_header'):
        '''
        Handling a special case of parsing a seabird header.

        Since the parser can't set the header value itself,
        we will set header row to 1 and allow the bcodmo-fixedwidth
        parser to populate the row at 1 with what it thinks the header values should be
        '''
        parameters['headers'] = 1


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
                    package.pkg.descriptor['resources'][i][PROP_STREAMING] = True
                    package.pkg.descriptor['resources'][i][PROP_STREAMED_FROM] = _from
            yield package.pkg
            yield from package
        return func

    def remove_empty_rows(name):
        def func(package):
            yield package.pkg
            def process_resource(rows):
                for row in rows:
                    for value in row.values():
                        if value:
                            # Only yield if something in the row has a value
                            yield row
                            break

            for r in package:
                if r.res.name == name:
                    yield process_resource(r)
                else:
                    yield r
        return func

    params = []
    _name = parameters.pop('name', '')
    name_len = len(_name.split(_input_separator))
    from_len = len(_from.split(_input_separator))
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
        names = _name.split(_input_separator)

    # Get comma seperated file names/urls
    for i, url in enumerate(_from.split(_input_separator)):
        # Default the name to res[1-n]
        resource_name = names[i]

        if parameters.get('sheet_regex', False):
            '''
            Handling a regular expression sheet name
            '''
            try:
                xls = xlrd.open_workbook(url, on_demand=True)
            except FileNotFoundError:
                raise Exception(f'The file {url} was not found. Remember that sheet regular expressions only work on local paths')
            sheet_names = xls.sheet_names()
            sheet_regex = parameters.pop('sheet', '')
            for sheet_name in sheet_names:
                if re.match(sheet_regex, sheet_name):
                    new_name = re.sub('[^-a-z0-9._]', '', sheet_name.lower())
                    if len(_from.split(_input_separator)) > 1:
                        # If there are multiple urls being loaded, have the name take that into account
                        new_name = f'{resource_name}-{new_name}'
                    params.extend([
                        load(
                            url,
                            custom_parsers = custom_parsers,
                            name = new_name,
                            sheet = sheet_name,
                            **parameters,
                        ),
                        mark_streaming(url, new_name),
                    ])
                    if _remove_empty_rows:
                        params.append(remove_empty_rows(new_name))
        else:
            params.extend([
                load(url, custom_parsers = custom_parsers, name = resource_name, **parameters),
                mark_streaming(url, resource_name),
            ])
            if _remove_empty_rows:
                params.append(remove_empty_rows(resource_name))

    return Flow(
        count_resources(),
        *params,
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.datapackage), ctx)
