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


class S3Dumper(DumperBase):
    def __init__(self, bucket_name, prefix, **options):
        super(S3Dumper, self).__init__(options)
        self.force_format = options.get("force_format", True)
        self.forced_format = options.get("format", "csv")
        self.temporal_format_property = options.get("temporal_format_property", None)
        self.use_titles = options.get("use_titles", False)

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
            file_formatter = {"csv": CSVFormat, "json": JSONFormat}.get(file_format)
            if file_format is not None:
                self.file_formatters[resource.name] = file_formatter
                self.file_formatters[resource.name].prepare_resource(resource)
                resource.commit()
                datapackage.descriptor["resources"][i] = resource.descriptor

        if "bcodmo:" not in datapackage.descriptor:
            datapackage.descriptor["bcodmo:"] = {}
        datapackage.descriptor["bcodmo:"]["dataManager"] = self.data_manager
        return datapackage

    def write_file_to_output(self, contents, path, content_type):
        if path.startswith("."):
            path = path[1:]
        if path.startswith("/"):
            path = path[1:]
        obj_name = os.path.join(self.prefix, path)
        contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
        with open(f"/home/conrad/Documents/laminar_testing/{path}", "w") as f:
            f.write(contents.decode())

        obj = self.s3.Object(self.bucket_name, obj_name)
        obj.put(Body=contents, ContentType=content_type)

        return path

    def handle_datapackage(self):
        if self.save_pipeline_spec and self.pipeline_spec:
            # self from pipeline_spec inputted
            try:
                # Write the pipeline_spec to the temp file
                contents = self.pipeline_spec.encode()
                self.write_file_to_output(contents, "pipeline-spec.yaml", "text/yaml")
            except Exception as e:
                logging.warn(f"Failed to save the pipeline-spec.yaml: {str(e)}",)

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

        # File size:
        filesize = stream.tell()
        DumperBase.inc_attr(
            self.datapackage.descriptor, self.datapackage_bytes, filesize
        )
        DumperBase.inc_attr(resource_descriptor, self.resource_bytes, filesize)

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
        self.write_file_to_output(
            stream.read().encode(), resource.res.source, "text/csv"
        )
        stream.close()

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
                hasher.update(data.encode("utf8"))
            elif isinstance(data, bytes):
                hasher.update(data)
        return hasher


def flow(parameters):
    return Flow(
        S3Dumper(parameters.pop("bucket_name"), parameters.pop("prefix"), **parameters)
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
