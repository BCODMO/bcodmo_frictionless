import os
import boto3
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from dataflows.processors.dumpers.file_dumper import FileDumper


class S3Dumper(FileDumper):
    def __init__(self, bucket_name, prefix, **options):
        super(S3Dumper, self).__init__(options)
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.s3 = boto3.resource("s3")

    def write_file_to_output(self, filename, path):
        if path.startswith("."):
            path = path[1:]
        if path.startswith("/"):
            path = path[1:]
        obj_name = os.path.join(self.prefix, path)
        with open(filename, "rb") as f:
            contents = f.read()

            obj = self.s3.Object(self.bucket_name, obj_name)
            obj.put(Body=contents)

        return path


def flow(parameters):
    return Flow(
        S3Dumper(parameters.pop("bucket_name"), parameters.pop("prefix"), **parameters)
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
