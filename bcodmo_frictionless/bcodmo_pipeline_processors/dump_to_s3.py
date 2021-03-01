import os
import json
import io
import logging
import boto3
import tempfile
import hashlib
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from dataflows.processors.dumpers.dumper_base import DumperBase
from dataflows.processors.dumpers.file_formats import CSVFormat, JSONFormat

WINDOWS_LINE_ENDING = b"\r\n"
UNIX_LINE_ENDING = b"\n"

WINDOWS_LINE_ENDING_STR = "\r\n"
UNIX_LINE_ENDING_STR = "\n"


def expand_scientific_notation(flt):
    str_vals = str(flt).split("E")
    coef = float(str_vals[0])
    exp = int(str_vals[1])
    return_val = ""
    if int(exp) > 0:
        return_val += str(coef).replace(".", "")
        return_val += "".join(
            ["0" for _ in range(0, abs(exp - len(str(coef).split(".")[1])))]
        )
    elif int(exp) < 0:
        return_val += "0."
        return_val += "".join(["0" for _ in range(0, abs(exp) - 1)])
        return_val += str(coef).replace(".", "")
    return return_val


def num_to_string(num):
    value = str(num)
    if "E" in value:
        # Handle scientific notation
        return expand_scientific_notation(value)
    return value


def num_to_scientific_notation(num):
    return "{:e}".format(num)


class CustomCSVFormat(CSVFormat):
    # A custom CSVFormat that allows use to customize the serializer for decimal

    SERIALIZERS = {**CSVFormat.SERIALIZERS, **{"number": num_to_string}}

    def __init__(self, file, schema, use_titles=False, **options):
        super(CustomCSVFormat, self).__init__(
            file, schema, use_titles=use_titles, **options
        )

        for field in schema.fields:
            # support scientific notation
            if field.type in ["number"]:
                number_output_format = field.descriptor.get("numberOutputFormat", None)
                if (
                    number_output_format
                    and number_output_format == "scientificNotation"
                ):
                    field.descriptor["serializer"] = num_to_scientific_notation


class S3Dumper(DumperBase):
    def __init__(self, bucket_name, prefix, **options):
        super(S3Dumper, self).__init__(options)
        self.force_format = options.get("force_format", True)
        self.forced_format = options.get("format", "csv")
        self.temporal_format_property = options.get("temporal_format_property", None)
        self.use_titles = options.get("use_titles", False)
        self.submission_id = options.get("submission_id", False)

        self.prefix = prefix
        self.bucket_name = bucket_name
        self.save_pipeline_spec = options.get("save_pipeline_spec", False)
        self.pipeline_spec = options.get("pipeline_spec", None)
        self.data_manager = options.get("data_manager", {})

        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        host = os.environ.get("LAMINAR_S3_HOST")
        if access_key and host and secret_access_key:
            self.s3 = boto3.resource(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                endpoint_url=host,
            )
        else:
            logging.warn("Using base boto credentials for S3 Dumper")
            self.s3 = boto3.resource("s3")

    def process_datapackage(self, datapackage):
        datapackage = super(S3Dumper, self).process_datapackage(datapackage)

        self.file_formatters = {}
        # Make sure all resources are proper CSVs
        resource: Resource = None
        for i, resource in enumerate(datapackage.resources):
            if self.force_format:
                file_format = self.forced_format
            else:
                _, file_format = os.path.splitext(resource.source)
                file_format = file_format[1:]
            file_formatter = {"csv": CustomCSVFormat, "json": JSONFormat}.get(
                file_format
            )
            if file_format is not None:
                self.file_formatters[resource.name] = file_formatter
                self.file_formatters[resource.name].prepare_resource(resource)
                resource.commit()
                datapackage.descriptor["resources"][i] = resource.descriptor

        if "bcodmo:" not in datapackage.descriptor:
            datapackage.descriptor["bcodmo:"] = {}
        datapackage.descriptor["bcodmo:"]["dataManager"] = self.data_manager
        datapackage.descriptor["bcodmo:"]["submissionId"] = self.submission_id
        return datapackage

    def write_file_to_output(self, contents, path, content_type):
        if path.startswith("."):
            path = path[1:]
        if path.startswith("/"):
            path = path[1:]
        obj_name = os.path.join(self.prefix, path)
        contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

        obj = self.s3.Object(self.bucket_name, obj_name)
        obj.put(Body=contents, ContentType=content_type)

        return path, len(contents)

    def handle_datapackage(self):
        if self.save_pipeline_spec and self.pipeline_spec:
            # self from pipeline_spec inputted
            try:
                # Write the pipeline_spec to the temp file
                contents = self.pipeline_spec.encode()
                self.write_file_to_output(contents, "pipeline-spec.yaml", "text/yaml")
            except Exception as e:
                logging.warn(f"Failed to save the pipeline-spec.yaml: {str(e)}",)

        self.datapackage.descriptor["dump_path"] = self.prefix
        self.datapackage.descriptor["dump_bucket"] = self.bucket_name
        self.datapackage.commit()

        # Handle temporal_format_property
        if self.temporal_format_property:
            for resource in self.datapackage.descriptor["resources"]:
                for field in resource["schema"]["fields"]:
                    if field.get("type") in ["datetime", "date", "time"]:
                        format = field.get(self.temporal_format_property, None)
                        if format:
                            field["format"] = format
            self.datapackage.commit()

        stream = io.StringIO()
        indent = 2 if self.pretty_descriptor else None
        str_contents = json.dumps(
            self.datapackage.descriptor,
            indent=indent,
            sort_keys=True,
            ensure_ascii=False,
        )
        contents = str_contents.encode()

        DumperBase.inc_attr(
            self.datapackage.descriptor, self.datapackage_bytes, len(contents)
        )

        self.write_file_to_output(contents, "datapackage.json", "application/json")

        super(S3Dumper, self).handle_datapackage()

    def rows_processor(self, resource, writer, stream):
        for row in resource:
            writer.write_row(row)
            yield row
        writer.finalize_file()

        # Get resource descriptor
        resource_descriptor = resource.res.descriptor
        for descriptor in self.datapackage.descriptor["resources"]:
            if descriptor["name"] == resource.res.descriptor["name"]:
                resource_descriptor = descriptor

        # File Hash:
        if self.resource_hash:
            hasher = S3Dumper.hash_handler(stream)
            # Update path with hash
            if self.add_filehash_to_path:
                DumperBase.insert_hash_in_path(resource_descriptor, hasher.hexdigest())
            DumperBase.set_attr(
                resource_descriptor, self.resource_hash, hasher.hexdigest()
            )

        # Finalise
        stream.seek(0)
        _, filesize = self.write_file_to_output(
            stream.read().encode(), resource.res.source, "text/csv"
        )
        stream.close()

        # Update filesize
        DumperBase.inc_attr(
            self.datapackage.descriptor, self.datapackage_bytes, filesize
        )
        DumperBase.inc_attr(resource_descriptor, self.resource_bytes, filesize)

    def process_resource(self, resource):
        if resource.res.name in self.file_formatters:
            schema = resource.res.schema

            stream = io.StringIO()
            writer_kwargs = {"use_titles": True} if self.use_titles else {}
            writer_kwargs["temporal_format_property"] = self.temporal_format_property
            writer = self.file_formatters[resource.res.name](
                stream, schema, **writer_kwargs
            )

            return self.rows_processor(resource, writer, stream)
        else:
            return resource

    @staticmethod
    def hash_handler(tfile):
        tfile.seek(0)
        hasher = hashlib.md5()
        data = "x"
        while len(data) > 0:
            data = tfile.read(1024)
            if isinstance(data, str):
                data = data.replace(WINDOWS_LINE_ENDING_STR, UNIX_LINE_ENDING_STR)
                hasher.update(data.encode("utf8"))
            elif isinstance(data, bytes):
                data = data.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
                hasher.update(data)
        return hasher


def flow(parameters):
    return Flow(
        S3Dumper(parameters.pop("bucket_name"), parameters.pop("prefix"), **parameters)
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
