import xlrd
import re
import glob
import sys
from dataflows import Flow, load as standard_load
from datapackage_pipelines.utilities.resources import PROP_STREAMING, PROP_STREAMED_FROM
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

# Import custom parsers here
from bcodmo_processors.bcodmo_pipeline_processors.parsers import FixedWidthParser

# Add custom parsers here
# Custom parsers should NOT have periods in their name
custom_parsers = {
    "bcodmo-fixedwidth": FixedWidthParser,
}


def load(_from, parameters):
    _input_separator = parameters.pop("input_separator", ",")
    _remove_empty_rows = parameters.pop("remove_empty_rows", True)
    _recursion_limit = parameters.pop("recursion_limit", False)

    if _recursion_limit:
        sys.setrecursionlimit(_recursion_limit)

    if parameters.get("format") == "bcodmo-fixedwidth":
        # With fixed width files, we want to also send the sample_size
        # and skip_rows parameters to the parser (not just the stream)
        parameters["fixedwidth_sample_size"] = parameters.get("sample_size", 100)
        parameters["fixedwidth_skip_header"] = [
            v for v in parameters.get("skip_rows", []) if type(v) == str
        ]

    if parameters.get("parse_seabird_header"):
        """
        Handling a special case of parsing a seabird header.

        Since the parser can't set the header value itself,
        we will set header row to 1 and allow the bcodmo-fixedwidth
        parser to populate the row at 1 with what it thinks the header values should be
        """
        parameters["headers"] = 1

    def count_resources():
        def func(package):
            global num_resources
            num_resources = len(package.pkg.resources)
            yield package.pkg
            yield from package

        return func

    def mark_streaming(_from):
        def func(package):
            for i in range(num_resources, len(package.pkg.resources)):
                package.pkg.descriptor["resources"][i].setdefault(PROP_STREAMING, True)
                package.pkg.descriptor["resources"][i].setdefault(
                    PROP_STREAMED_FROM, _from
                )
            yield package.pkg
            yield from package

        return func

    def remove_empty_rows(name):
        def func(package):
            yield package.pkg

            def process_resource(rows, missing_data_values):
                for row in rows:
                    for value in row.values():
                        if value and value not in missing_data_values:
                            # Only yield if something in the row has a value
                            yield row
                            break

            for r in package:
                if r.res.name == name:
                    missing_data_values = r.res.descriptor.get("schema", {},).get(
                        "missingValues", [],
                    )

                    yield process_resource(r, missing_data_values)
                else:
                    yield r

        return func

    # Handle multiple source URIs
    from_list = _from.split(_input_separator)
    input_path_pattern = parameters.pop("input_path_pattern", False)
    if input_path_pattern:
        from_list = glob.glob(_from)
        if not len(from_list):
            raise Exception(f"No files found with the pattern {_from})")
    params = []
    _name = parameters.pop("name", None)
    if not _name:
        raise Exception(
            '"name" is now a required parameter. Please add at least a single name.'
        )

    name_len = len(_name.split(_input_separator))
    from_len = len(from_list)
    if name_len is not 1 and name_len is not from_len:
        raise Exception(
            f"The comma seperated list of names has length {name_len} and the list of urls has length {from_len}. Please provide only one name or an equal number of names as the from list",
        )

    # Handle the names of the resources, if multiple
    names = []
    if name_len is 1:
        if from_len > 1:
            for i in range(from_len):
                resource_name = f"{_name}-{i + 1}"
                names.append(resource_name)
        else:
            names = [_name]
    else:
        names = _name.split(_input_separator)

    # Get comma seperated file names/urls
    for i, url in enumerate(from_list):
        # Default the name to res[1-n]
        resource_name = names[i]

        if parameters.get("sheet_regex", False):
            """
            Handling a regular expression sheet name
            """
            try:
                xls = xlrd.open_workbook(url, on_demand=True)
            except FileNotFoundError:
                raise Exception(
                    f"The file {url} was not found. Remember that sheet regular expressions only work on local paths"
                )
            sheet_names = xls.sheet_names()
            sheet_regex = parameters.pop("sheet", "")
            for sheet_name in sheet_names:
                if re.match(sheet_regex, sheet_name):
                    new_name = re.sub(
                        "[^-a-z0-9._]", "", re.sub(r"\s+", "_", sheet_name.lower())
                    )
                    if len(from_list) > 1:
                        # If there are multiple urls being loaded, have the name take that into account
                        new_name = f"{resource_name}-{new_name}"
                    params.extend(
                        [
                            count_resources(),
                            standard_load(
                                url,
                                custom_parsers=custom_parsers,
                                name=new_name,
                                sheet=sheet_name,
                                **parameters,
                            ),
                            mark_streaming(url),
                        ]
                    )
                    if _remove_empty_rows:
                        params.append(remove_empty_rows(new_name))
        else:
            params.extend(
                [
                    count_resources(),
                    standard_load(
                        url,
                        custom_parsers=custom_parsers,
                        name=resource_name,
                        **parameters,
                    ),
                    mark_streaming(url),
                ]
            )
            if _remove_empty_rows:
                params.append(remove_empty_rows(resource_name))

    return Flow(*params,)


def flow(parameters):
    _from = parameters.pop("from")
    return Flow(load(_from, parameters),)


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
