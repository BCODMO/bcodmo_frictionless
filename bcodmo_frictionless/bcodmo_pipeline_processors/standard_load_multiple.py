from dataflows import load as standard_load

from billiard import Pool
from datapackage import Package
import time
from tabulator import Stream
from tabulator.helpers import extract_options
from dataflows.processors.parsers import XMLParser, ExcelXMLParser, ExtendedSQLParser
from tableschema.schema import Schema
import io
import os
from bcodmo_frictionless.bcodmo_pipeline_processors.loaders import (
    BcodmoAWS,
)


def _preload_data(load_source, mode, loader, loader_options):
    try:
        loader_obj = loader(**loader_options)
        chars = loader_obj.load(load_source, mode=mode)

        return chars.read(), loader_obj.encoding, None
    except Exception as e:
        return None, None, e


def get_mode(_format):
    return "b" if _format in ["xlsx", "xls"] else "t"


class standard_load_multiple(standard_load):
    def __init__(
        self,
        load_sources,
        names,
        sheets=None,
        **options,
    ):
        super(standard_load_multiple, self).__init__("", **options)
        self.load_sources = load_sources
        self.names = names
        self.sheets = sheets

    def _set_individual(self, i):
        load_source = self.load_sources[i]
        name = self.names[i]
        self.preloaded_chars = None

        self.load_source = load_source
        self.name = name

        if self.sheets:
            sheet = self.sheets[i]
            self.options["sheet"] = sheet

        if "sheet" in self.options and not self.options["sheet"]:
            del self.options["sheet"]

    def process_datapackage(self, dp: Package):
        results = {}

        # Only do preloaded data for bcodmo-aws loader
        if (
            self.options.get("scheme", None) == "bcodmo-aws"
            and len(self.load_sources) > 1
        ):
            procs = {}
            pool = Pool(threads=True)

            for i, load_source in enumerate(self.load_sources):
                if load_source not in procs:
                    self._set_individual(i)
                    self.options["loader_resource_name"] = self.names[i]
                    mode = get_mode(self.options.get("format"))

                    loader_options = extract_options(self.options, BcodmoAWS.options)

                    proc = pool.apply_async(
                        _preload_data,
                        (load_source, mode, BcodmoAWS, loader_options),
                    )
                    procs[load_source] = proc

            pool.close()
            pool.join()
            for load_source, proc in procs.items():
                data, encoding, err = proc.get()
                if err is not None:
                    raise err
                results[load_source] = {"data": data, "encoding": encoding}

        for i, load_source in enumerate(self.load_sources):
            # Set the proper variables for this individual resource

            self._set_individual(i)
            self.options["loader_resource_name"] = self.names[i]

            if load_source in results:
                chars_s = results[load_source]["data"]
                encoding = results[load_source]["encoding"]

                mode = get_mode(self.options.get("format"))
                if mode == "b":
                    self.preloaded_chars = io.BufferedRandom(io.BytesIO(chars_s))
                else:
                    self.preloaded_chars = io.TextIOWrapper(
                        io.BytesIO(bytes(chars_s.encode(encoding)))
                    )

            super(standard_load_multiple, self).process_datapackage(Package())

        dp.descriptor.setdefault("resources", []).extend(self.resource_descriptors)
        return dp

    def safe_process_datapackage(self, dp: Package):
        # If loading from datapackage & resource iterator:
        if isinstance(self.load_source, tuple):
            datapackage_descriptor, resource_iterator = self.load_source
            resources = datapackage_descriptor["resources"]
            resource_matcher = ResourceMatcher(self.resources, datapackage_descriptor)
            for resource_descriptor in datapackage_descriptor["resources"]:
                if resource_matcher.match(resource_descriptor["name"]):
                    self.resource_descriptors.append(resource_descriptor)
            self.iterators = (
                resource
                for resource, descriptor in zip(resource_iterator, resources)
                if resource_matcher.match(descriptor["name"])
            )

        # If load_source is string:
        else:
            # Handle Environment vars if necessary:
            if self.load_source.startswith("env://"):
                env_var = self.load_source[6:]
                self.load_source = os.environ.get(env_var)
                if self.load_source is None:
                    raise ValueError(f"Couldn't find value for env var '{env_var}'")

            # Loading from datapackage:
            if (
                os.path.basename(self.load_source) == "datapackage.json"
                or self.options.get("format") == "datapackage"
            ):
                self.load_dp = Package(self.load_source)
                resource_matcher = ResourceMatcher(self.resources, self.load_dp)
                for resource in self.load_dp.resources:
                    if resource_matcher.match(resource.name):
                        self.resource_descriptors.append(resource.descriptor)
                        self.iterators.append(resource.iter(keyed=True, cast=True))

            # Loading for any other source
            else:
                path = os.path.basename(self.load_source)
                path = os.path.splitext(path)[0]
                descriptor = dict(
                    path=self.name or path, profile="tabular-data-resource"
                )
                self.resource_descriptors.append(descriptor)
                descriptor["name"] = self.name or path
                if "encoding" in self.options:
                    descriptor["encoding"] = self.options["encoding"]
                self.options.setdefault("custom_parsers", {}).setdefault(
                    "xml", XMLParser
                )
                self.options.setdefault("custom_parsers", {}).setdefault(
                    "excel-xml", ExcelXMLParser
                )
                self.options.setdefault("custom_parsers", {}).setdefault(
                    "sql", ExtendedSQLParser
                )
                self.options.setdefault("ignore_blank_headers", True)
                self.options.setdefault("headers", 1)

                """ Change to add preloaded data """
                options = self.options
                if self.preloaded_chars is not None:
                    options["preloaded_chars"] = self.preloaded_chars
                stream: Stream = Stream(self.load_source, **options).open()
                """ Finish change to add preloaded data """

                if len(stream.headers) != len(set(stream.headers)):
                    if not self.deduplicate_headers:
                        raise ValueError(
                            "Found duplicate headers."
                            + "Use the `deduplicate_headers` flag (found headers=%r)"
                            % stream.headers
                        )
                    stream.headers = self.rename_duplicate_headers(stream.headers)
                schema = Schema().infer(
                    stream.sample,
                    headers=stream.headers,
                    confidence=1,
                    guesser_cls=self.guesser,
                )
                # restore schema field names to original headers
                for header, field in zip(stream.headers, schema["fields"]):
                    field["name"] = header
                if self.override_schema:
                    schema.update(self.override_schema)
                if self.override_fields:
                    fields = schema.get("fields", [])
                    for field in fields:
                        field.update(self.override_fields.get(field["name"], {}))
                if self.extract_missing_values:
                    missing_values = schema.get("missingValues", [])
                    if not self.extract_missing_values["values"]:
                        self.extract_missing_values["values"] = missing_values
                    schema["fields"].append(
                        {
                            "name": self.extract_missing_values["target"],
                            "type": "object",
                            "format": "default",
                            "values": self.extract_missing_values["values"],
                        }
                    )
                descriptor["schema"] = schema
                descriptor["format"] = self.options.get("format", stream.format)
                descriptor["path"] += ".{}".format(stream.format)
                self.iterators.append(stream.iter(keyed=True))
        dp.descriptor.setdefault("resources", []).extend(self.resource_descriptors)
        return dp
