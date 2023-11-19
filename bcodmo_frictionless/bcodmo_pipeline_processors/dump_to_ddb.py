import os
import json
import io
import time
import logging
import boto3
import tempfile
import hashlib
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from dataflows.processors.dumpers.dumper_base import DumperBase
from dataflows.processors.dumpers.file_formats import CSVFormat, JSONFormat

from .dump_to_s3 import S3Dumper, WINDOWS_LINE_ENDING, UNIX_LINE_ENDING


class DDBDumper(S3Dumper):
    def __init__(
        self,
        ddb_endpoint_url,
        ddb_results_table,
        ddb_results_last_used_table,
        cache_id,
        **options,
    ):
        super(DDBDumper, self).__init__(
            options.pop("bucket_name"), options.pop("prefix"), **options
        )
        self.ddb = boto3.client("dynamodb", endpoint_url=ddb_endpoint_url)
        self.ddb_results_table = ddb_results_table
        self.ddb_results_last_used_table = ddb_results_last_used_table
        self.cache_id = cache_id

    def update_last_used(self, key):
        ddb_last_used_response = self.ddb.put_item(
            Item={
                "CacheIdFileName": {
                    "S": key,
                },
                "LastUsed": {
                    "N": str(time.time()),
                },
            },
            TableName=self.ddb_results_last_used_table,
        )
        if (
            ddb_last_used_response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode", None
            )
            != 200
        ):
            raise Exception(
                f"Error adding the key {key} to the ddb results last used table"
            )

    def write_file_to_output(self, contents, path, content_type):
        contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

        start = time.time()
        print(f"Starting save file {time.time()}")
        key = f"{self.cache_id}-{path}"
        r = self.ddb.put_item(
            Item={
                "CacheIdFileName": {
                    "S": key,
                },
                "RowNumberHundreds": {
                    "N": str(0),
                },
                "Content": {"S": contents.decode()},
            },
            TableName=self.ddb_results_table,
        )
        print(f"Took {time.time() - start} to save the file ({path})")
        self.update_last_used(key)

        return path, len(contents)

    def rows_processor(self, resource, writer, stream):
        row_number = None
        print(f"Received at {time.time()}")
        key = f"{self.cache_id}-{resource.res.descriptor['name']}"

        try:
            start = time.time()
            row_number = 0
            buffered_rows = []
            buffered_rows_wrapper = []
            start_row_number = None

            def upload_to_ddb(key, start, rows_wrapper):
                r = self.ddb.batch_write_item(
                    RequestItems={
                        self.ddb_results_table: [
                            {
                                "PutRequest": {
                                    "Item": {
                                        "CacheIdFileName": {
                                            "S": key,
                                        },
                                        "RowNumberHundreds": {
                                            "N": str(start + i),
                                        },
                                        "Content": {
                                            "S": rows,
                                        },
                                    }
                                }
                            }
                            for i, rows in enumerate(rows_wrapper)
                        ]
                    }
                )
                if r.get("ResponseMetadata", {}).get("HTTPStatusCode", None) != 200:
                    raise Exception(
                        f"Error adding rows {hundred_count*100} to {(hundred_count+1)*100} for ETag {etag} to the ddb load table"
                    )
                return r

            for row in resource:
                # We use the writer to transform the row, but we don't actually write to it
                if start_row_number is None:
                    start_row_number = row_number
                transformed_row = list(writer._FileFormat__transform_row(row).values())
                buffered_rows.append(transformed_row)
                if len(buffered_rows) == 100:
                    buffered_rows_wrapper.append(json.dumps(buffered_rows))
                    buffered_rows = []
                    if len(buffered_rows_wrapper) == 25:
                        upload_to_ddb(
                            key, start_row_number // 100, buffered_rows_wrapper
                        )
                        buffered_rows_wrapper = []
                        start_row_number = None

                row_number += 1
                yield row
            if len(buffered_rows) != 0:
                buffered_rows_wrapper.append(json.dumps(buffered_rows))
            if len(buffered_rows_wrapper) != 0:
                upload_to_ddb(key, start_row_number // 100, buffered_rows_wrapper)

            row_number = None

            print(f"Finished streaming rows to DDB after {time.time() - start}")

            self.update_last_used(key)

            # Delete the row one above our last row, in case the last run with this cache id had more rows
            response = self.ddb.delete_item(
                Key={
                    "CacheIdFileName": {
                        "S": key,
                    },
                    "RowNumberHundreds": {
                        "N": str(start_row_number + 1),
                    },
                },
                TableName=self.ddb_results_table,
            )

            # Get resource descriptor
            resource_descriptor = resource.res.descriptor
            for descriptor in self.datapackage.descriptor["resources"]:
                if descriptor["name"] == resource.res.descriptor["name"]:
                    resource_descriptor = descriptor

            # File Hash:
            if self.resource_hash:
                nd_value = "not available"
                if self.add_filehash_to_path:
                    DumperBase.insert_hash_in_path(resource_descriptor, nd_value)
                DumperBase.set_attr(resource_descriptor, self.resource_hash, nd_value)

            # TODO actually calculate filesize
            filesize = 0
            # Update filesize
            DumperBase.inc_attr(
                self.datapackage.descriptor, self.datapackage_bytes, filesize
            )
            DumperBase.inc_attr(resource_descriptor, self.resource_bytes, filesize)
        except Exception as e:
            row_number_text = ""
            if row_number is not None:
                row_number_text = f" - occured at line # {row_number + 1}"

            if len(e.args) >= 1:
                e.args = (
                    e.args[0]
                    + f"\n\nOccured at resource {resource.res.descriptor['name']}{row_number_text}",
                ) + e.args[1:]
            raise e


def flow(parameters):
    return Flow(
        DDBDumper(
            parameters.pop("ddb_endpoint_url"),
            parameters.pop("ddb_results_table"),
            parameters.pop("ddb_results_last_used_table"),
            parameters.pop("cache_id"),
            **parameters,
        )
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
