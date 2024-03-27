import csv
import os
import json
import io
import redis
import time
import logging
import boto3
import tempfile
import hashlib
from dataflows import Flow
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from billiard import Process, Queue, Pool

from dataflows.processors.dumpers.dumper_base import DumperBase
from dataflows.processors.dumpers.file_formats import CSVFormat, JSONFormat, FileFormat
from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_progress_resource_key,
    get_redis_progress_num_parts_key,
    get_redis_progress_parts_key,
    get_redis_connection,
    REDIS_PROGRESS_SAVING_START_FLAG,
    REDIS_PROGRESS_SAVING_DONE_FLAG,
    REDIS_EXPIRES,
)

WINDOWS_LINE_ENDING = b"\r\n"
UNIX_LINE_ENDING = b"\n"

WINDOWS_LINE_ENDING_STR = "\r\n"
UNIX_LINE_ENDING_STR = "\n"


MB = 1024 * 1024


def calculate_partsize(num_parts_so_far):
    # Ensures we stay with small part size when the file is small, but increase the part size as the file gets bigger
    # This ensures up to 1TB of uplooad size
    if num_parts_so_far < 25:
        # First 25 parts of 5 MB
        return MB * 7
    if num_parts_so_far < 50:
        # Next 25 parts are 10 MB
        return MB * 10
    if num_parts_so_far < 75:
        # Next 25 parts are 25 MB
        return MB * 25
    if num_parts_so_far < 100:
        # Next 25 parts are 50 MB
        return MB * 50
    return MB * 100


def expand_scientific_notation(flt):
    was_neg = False
    if flt.startswith("-"):
        was_neg = True
        flt = flt[1:]

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
    if was_neg:
        return_val = "-" + return_val
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
    # and also not write the header if we so choose

    SERIALIZERS = {**CSVFormat.SERIALIZERS, **{"number": num_to_string}}

    def __init__(self, file, schema, write_header=True, use_titles=False, **options):
        headers = [f.name for f in schema.fields]
        if use_titles:
            titles = [f.descriptor.get("title", f.name) for f in schema.fields]
            csv_writer = CsvTitlesDictWriter(file, headers, fieldtitles=titles)
        else:
            csv_writer = csv.DictWriter(file, headers)
        # We can leave out write header

        if write_header:
            csv_writer.writeheader()

        FileFormat.__init__(self, csv_writer, schema, **options)

        # super(CustomCSVFormat, self).__init__(
        #    file, schema, use_titles=use_titles, **options
        # )

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
        self.submission_id = options.get("submission_id", None)
        self.submission_ids = options.get("submission_ids", [])
        self.cache_id = options.get("cache_id", None)
        self.delete = options.get("delete", False)
        self.limit_yield = options.get("limit_yield", None)

        self.prefix = prefix
        self.bucket_name = bucket_name
        self.save_pipeline_spec = options.get("save_pipeline_spec", False)
        self.pipeline_spec = options.get("pipeline_spec", None)
        self.data_manager = options.get("data_manager", {})
        self.procs = {}
        self.pool = Pool(threads=True)

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
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                endpoint_url=host,
            )
        else:
            logging.warn("Using base boto credentials for S3 Dumper")
            self.s3 = boto3.resource("s3")
            self.s3_client = boto3.client("s3")
        if self.delete:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_access_key,
                endpoint_url=host,
            )
            res = s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix,
            )
            if "Contents" in res:
                contents = res["Contents"]
                if len(contents) >= 10:
                    raise Exception(
                        f"Throwing an error from the dump_to_s3 processor because the number of files to be deleted was more than 10. This is a safety measure to ensure we don't accidently more files than expected."
                    )
                s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
                )

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
        if self.submission_id:
            datapackage.descriptor["bcodmo:"]["submissionId"] = self.submission_id
        datapackage.descriptor["bcodmo:"]["submissionIds"] = self.submission_ids

        return datapackage

    def handle_datapackage(self):
        etags = {}
        filesizes = {}
        self.pool.close()
        self.pool.join()
        for resource_name, d in self.procs.items():
            redis_conn = None
            progress_key = None
            if self.cache_id:
                redis_conn = get_redis_connection()
                progress_key = get_redis_progress_key(resource_name, self.cache_id)

            upload_id = d["upload_id"]
            procs = d["procs"]
            object_key = d["object_key"]
            parts = []
            filesize = 0
            is_multipart = False
            for proc in procs:
                partsize, part, err = proc.get()
                if err is not None:
                    return self._handle_exception(err, resource_name)
                parts.append(part)
                filesize += partsize

            is_multipart = parts[0]["part_number"] is not None
            if is_multipart:
                parts.sort(key=lambda p: p["part_number"])
                response = self.s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [
                            {"ETag": p["etag"], "PartNumber": p["part_number"]}
                            for p in parts
                        ]
                    },
                )
                etags[resource_name] = response["ETag"]
            else:
                etags[resource_name] = parts[0]["etag"]

            filesizes[resource_name] = filesize

            if redis_conn is not None:
                redis_conn.set(
                    progress_key, REDIS_PROGRESS_SAVING_DONE_FLAG, ex=REDIS_EXPIRES
                )

        for resource_descriptor in self.datapackage.descriptor["resources"]:
            resource_name = resource_descriptor["name"]
            filesize = filesizes.get(resource_name, 0)
            DumperBase.inc_attr(
                self.datapackage.descriptor, self.datapackage_bytes, filesize
            )
            DumperBase.inc_attr(resource_descriptor, self.resource_bytes, filesize)

            etag = etags.get(resource_name, None)
            if etag:
                if self.add_filehash_to_path:
                    DumperBase.insert_hash_in_path(resource_descriptor, etag)
                DumperBase.set_attr(
                    resource_descriptor,
                    self.resource_hash,
                    etag,
                )

        if self.save_pipeline_spec and self.pipeline_spec:
            # self from pipeline_spec inputted
            try:
                # Write the pipeline_spec to the temp file
                contents = self.pipeline_spec.encode()
                self.write_file_to_output(contents, "pipeline-spec.yaml", "text/yaml")
            except Exception as e:
                logging.warn(
                    f"Failed to save the pipeline-spec.yaml: {str(e)}",
                )

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

    def write_file_to_output(
        self,
        contents,
        path,
        content_type,
    ):
        start = time.time()
        if path.startswith("."):
            path = path[1:]
        if path.startswith("/"):
            path = path[1:]
        obj_name = os.path.join(self.prefix, path)
        contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

        start = time.time()
        obj = self.s3.Object(self.bucket_name, obj_name)
        obj.put(Body=contents, ContentType=content_type)

        print(f"Took {round(time.time() -start, 3)} to upload {path}")
        return path, len(contents)

    @staticmethod
    def write_file(contents, object_key, content_type, bucket_name):
        try:
            contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

            # We create our own client for this part, because it seems to be faster
            start = time.time()
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            host = os.environ.get("LAMINAR_S3_HOST")
            if access_key and host and secret_access_key:
                s3 = boto3.resource(
                    "s3",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    endpoint_url=host,
                )
            else:
                s3 = boto3.resource("s3")
            obj = s3.Object(bucket_name, object_key)
            response = obj.put(Body=contents, ContentType=content_type)
            print(
                f"Completed uploading file of size {round(len(contents) / (1024 * 1024), 4)}MiB after {round(time.time() - start, 3)}"
            )
            return (
                len(contents),
                {"part_number": None, "etag": response["ETag"]},
                None,
            )
        except Exception as e:
            print("ERROR", e)
            return None, None, e

    @staticmethod
    def write_part(
        contents,
        object_key,
        upload_id,
        part_number,
        bucket_name,
        resource_name,
        cache_id,
    ):
        try:
            contents = contents.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

            # We create our own client for this part, because it seems to be faster
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            host = os.environ.get("LAMINAR_S3_HOST")
            if access_key and host and secret_access_key:
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_access_key,
                    endpoint_url=host,
                )
            else:
                s3_client = boto3.client(
                    "s3",
                )

            start = time.time()
            response = s3_client.upload_part(
                Body=contents,
                Bucket=bucket_name,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=part_number,
            )
            if cache_id:
                redis_conn = get_redis_connection()
                redis_key = get_redis_progress_parts_key(resource_name, cache_id)
                redis_conn.sadd(
                    redis_key,
                    part_number,
                )
                redis_conn.expire(redis_key, REDIS_EXPIRES)

            print(
                f"Completed uploading part of size {round(len(contents) / (1024 * 1024), 4)}MiB after {round(time.time() - start, 3)}"
            )
            return (
                len(contents),
                {"part_number": part_number, "etag": response["ETag"]},
                None,
            )
        except Exception as e:
            print("ERROR", e)
            return None, None, e

    def async_write_part(
        self, stream, resource, part_number, object_key, upload_id, is_last
    ):
        # A helper function to create a Process that does a part upload
        resource_name = resource.res.descriptor["name"]
        part_number += 1
        stream.seek(0)
        s = stream.read()
        contents = s.encode()

        if len(contents):
            if is_last and part_number == 1:
                # Don't use multipart if the file size is less than 5MB
                proc = self.pool.apply_async(
                    S3Dumper.write_file,
                    (
                        contents,
                        object_key,
                        "text/csv",
                        self.bucket_name,
                    ),
                )
            else:
                if part_number == 1:
                    # Create multipart upload
                    response = self.s3_client.create_multipart_upload(
                        Bucket=self.bucket_name, Key=object_key, ContentType="text/csv"
                    )
                    upload_id = response["UploadId"]
                    self.procs[resource_name]["upload_id"] = upload_id
                proc = self.pool.apply_async(
                    S3Dumper.write_part,
                    (
                        contents,
                        object_key,
                        upload_id,
                        part_number,
                        self.bucket_name,
                        resource_name,
                        self.cache_id,
                    ),
                )

            self.procs[resource_name]["procs"].append(proc)
        stream.close()
        writer, stream = self.generate_writer(resource, write_header=False)
        return part_number, upload_id, writer, stream

    def _handle_exception(self, e, resource_name, row_number=None):
        row_number_text = ""
        if row_number is not None:
            row_number_text = f" - occured at line # {row_number + 1}"

        if len(e.args) >= 1:
            e.args = (
                e.args[0]
                + f"\n\nOccured at resource {resource_name}{row_number_text} in the dump_to_s3 processor",
            ) + e.args[1:]
        raise e

    def rows_processor(self, resource, writer, stream):
        resource_name = resource.res.descriptor["name"]

        path = resource.res.source
        if path.startswith("."):
            path = path[1:]
        if path.startswith("/"):
            path = path[1:]
        object_key = os.path.join(self.prefix, path)

        self.procs[resource_name] = {
            "upload_id": None,
            "object_key": object_key,
            "procs": [],
        }

        redis_conn = None
        progress_key = None
        if self.cache_id:
            redis_conn = get_redis_connection()
            redis_key = get_redis_progress_resource_key(self.cache_id)
            redis_conn.sadd(
                redis_key,
                resource_name,
            )
            redis_conn.expire(redis_key, REDIS_EXPIRES)

            redis_conn.delete(
                get_redis_progress_parts_key(resource_name, self.cache_id),
                get_redis_progress_num_parts_key(resource_name, self.cache_id),
            )

            progress_key = get_redis_progress_key(resource_name, self.cache_id)

        row_number = None
        upload_id = None

        writer_timer_sum = 0
        process_timer_sum = 0
        process_timer_count = 0
        process_timer = None
        redis_timer_sum = 0

        try:
            row_number = 0
            part_number = 0
            start = time.time()
            timer = time.time()

            yield_start = time.time()
            yield_total = 0
            async_total = 0
            loop_total = 0
            for row in resource:
                loop_total += time.time() - yield_start

                row_number += 1

                writer_timer = time.time()
                writer.write_row(row)
                writer_timer_sum += time.time() - writer_timer

                redis_timer = time.time()
                if redis_conn is not None and time.time() - timer > 0.75:
                    redis_conn.set(progress_key, row_number, ex=REDIS_EXPIRES)
                    timer = time.time()
                redis_timer_sum += time.time() - redis_timer

                async_start = time.time()
                if row_number % 25 == 0 and stream.tell() > calculate_partsize(
                    part_number
                ):
                    part_number, upload_id, writer, stream = self.async_write_part(
                        stream, resource, part_number, object_key, upload_id, False
                    )
                async_total += time.time() - async_start

                yield_total += time.time() - yield_start
                if (row_number + 1) % 10000 == 0:
                    # print(
                    #    f"total {yield_total}. async {async_total}. redis {redis_timer_sum}. writer {writer_timer_sum}. loop {loop_total}"
                    # )
                    yield_total = 0
                    async_total = 0
                    redis_timer_sum = 0
                    writer_timer_sum = 0
                    loop_total = 0
                if (
                    self.limit_yield is None
                    or self.limit_yield < 0
                    or row_number <= self.limit_yield
                ):
                    yield row
                yield_start = time.time()
            # Set row number values
            DumperBase.inc_attr(
                self.datapackage.descriptor, self.datapackage_rowcount, row_number
            )
            DumperBase.inc_attr(
                resource.res.descriptor, self.resource_rowcount, row_number
            )
            resource.res.commit()
            self.datapackage.commit()

            row_number = None
            writer.finalize_file()
            # Upload final part
            part_number, _, _, stream = self.async_write_part(
                stream, resource, part_number, object_key, upload_id, True
            )

            if redis_conn is not None:
                redis_conn.set(
                    get_redis_progress_num_parts_key(resource_name, self.cache_id),
                    part_number,
                    ex=REDIS_EXPIRES,
                )
                redis_conn.set(
                    progress_key, REDIS_PROGRESS_SAVING_START_FLAG, ex=REDIS_EXPIRES
                )

            stream.close()
        except Exception as e:
            return self._handle_exception(e, resource_name, row_number=row_number)

    def generate_writer(self, resource, write_header=True):
        schema = resource.res.schema
        stream = io.StringIO()
        writer_kwargs = {"use_titles": True} if self.use_titles else {}
        writer_kwargs["temporal_format_property"] = self.temporal_format_property
        writer = self.file_formatters[resource.res.name](
            stream, schema, write_header=write_header, **writer_kwargs
        )

        return writer, stream

    def process_resource(self, resource):
        if resource.res.name in self.file_formatters:
            writer, stream = self.generate_writer(resource)
            return self.rows_processor(resource, writer, stream)
        else:
            return resource

    def row_counter(self, resource, iterator):
        return iterator


def flow(parameters):
    return Flow(
        S3Dumper(parameters.pop("bucket_name"), parameters.pop("prefix"), **parameters)
    )


if __name__ == "__main__":
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
