import xlrd
import os
import re
import boto3
import glob
import sys
import fnmatch
from urllib.parse import unquote
from dataflows import Flow, load as standard_load
from datapackage_pipelines.utilities.resources import PROP_STREAMING, PROP_STREAMED_FROM
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

# Imports for handling s3 excel sheet regex
from six.moves.urllib.parse import urlparse
from tabulator.helpers import requote_uri


# Import custom parsers here
from bcodmo_processors.bcodmo_pipeline_processors.parsers import FixedWidthParser

# Add custom parsers here
# Custom parsers should NOT have periods in their name
custom_parsers = {
    "bcodmo-fixedwidth": FixedWidthParser,
}


def get_s3():
    load_access_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
    load_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
    load_endpoint_url = os.environ.get("LAMINAR_S3_HOST", None)

    if load_access_key and load_secret_access_key and load_endpoint_url:
        return boto3.resource(
            "s3",
            aws_access_key_id=load_access_key,
            aws_secret_access_key=load_secret_access_key,
            endpoint_url=load_endpoint_url,
        )
    if os.environ.get("TESTING") == "true":
        return boto3.resource("s3")
    raise Exception(
        "The credentials for the S3 load bucket are not set up properly on this machine"
    )


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
    if type(_from) == list:
        from_list = _from
    else:
        from_list = _from.split(_input_separator)
    if not len(from_list):
        raise Exception(
            "There are no URLs selected in the source files parameter of the load step. Select a source file from the provided list"
        )
    input_path_pattern = parameters.pop("input_path_pattern", False)
    if input_path_pattern:
        new_from_list = []
        for p in from_list:
            temp_from_list = []
            if p.startswith("s3://"):
                # Handle s3 pattern
                try:
                    bucket, path = p[5:].split("/", 1)
                except ValueError:
                    raise Exception(
                        f"Improperly formed S3 url passed to the load step: {p}"
                    )

                s3 = get_s3()
                bucket_obj = s3.Bucket(bucket)
                matches = fnmatch.filter(
                    [unquote(obj.key) for obj in bucket_obj.objects.all()], path
                )
                for match in matches:
                    temp_from_list.append(f"s3://{bucket}/{match}")

                if not len(temp_from_list):
                    raise Exception(
                        f"No objects found in S3 matching the glob pattern {p}. Are you sure the files have been properly staged?"
                    )
            else:
                # Handle local filesystem pattern
                temp_from_list = glob.glob(p)
                if not len(temp_from_list):
                    raise Exception(
                        f"No files found on the local file system with the glob pattern {p}. Are you sure you meant to use the input_path_pattern parameter?"
                    )
            new_from_list += temp_from_list
        from_list = new_from_list

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
            f"The list of names has length {name_len} and the list of urls has length {from_len}. Please provide only one name or an equal number of names as the from list",
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

        sheet_regex = parameters.pop("sheet_regex", False)
        sheet = parameters.get("sheet", "")
        sheet_range = False
        if type(sheet) == str:
            sheet_range = re.match("\d-\d", sheet)
        sheet_separator = parameters.pop("sheet_separator", None)

        if sheet_regex or sheet_range or (sheet_separator and sheet_separator in sheet):
            sheet = parameters.pop("sheet", "")
            """
            Handling a regular expression sheet name
            """
            sheets = []
            if sheet_regex:
                # Handle sheet regular expression (ignore sheet range and separator)
                try:
                    if url.startswith("s3://"):
                        s3 = get_s3()
                        import time

                        start = time.time()

                        parts = urlparse(requote_uri(url), allow_fragments=False)
                        obj = s3.Object(parts.netloc, parts.path[1:])
                        data = obj.get()["Body"].read()
                        xls = xlrd.open_workbook(file_contents=data, on_demand=True)
                        # xls = xlrd.open_workbook(io.BytesIO(data), on_demand=True)
                        elapsed = time.time() - start
                        print(f"Took {elapsed} to get the regular expression sheet")
                    else:
                        xls = xlrd.open_workbook(url, on_demand=True)
                except FileNotFoundError:
                    raise Exception(
                        f"The file {url} was not found. Remember that sheet regular expressions only work on local and s3 paths"
                    )
                sheet_names = xls.sheet_names()
                for sheet_name in sheet_names:
                    if re.match(sheet, sheet_name):
                        sheets.append(sheet_name)
            else:
                sheets_separate = sheet.split(sheet_separator)
                for s in sheets_separate:
                    if re.match("\d-\d", s):
                        try:
                            start, end = [
                                int(sheet_number) for sheet_number in s.split("-", 1)
                            ]
                            sheets += range(start, end + 1)
                        except ValueError:
                            raise Exception(
                                f"Attempted to parse a sheet range that contained a non-number: {s}"
                            )
                    else:
                        if s.isdigit():
                            sheets.append(int(s))
                        else:
                            sheets.append(s)
            if not len(sheets):
                raise Exception(
                    f"No sheets found for {url} with the inputted parameters"
                )

            # Create load processors for all of these sheets
            for sheet_name in sheets:
                new_name = re.sub(
                    "[^-a-z0-9._]", "", re.sub(r"\s+", "_", str(sheet_name).lower())
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
