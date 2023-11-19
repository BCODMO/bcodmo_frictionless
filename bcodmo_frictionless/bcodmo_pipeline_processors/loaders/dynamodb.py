# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import io
import time
import boto3
from tabulator.loader import Loader
from tabulator import helpers, config, exceptions
from six.moves.urllib.parse import urlparse


# Module API


class LaminarLoadDDBTextIO(io.TextIOBase):
    def __init__(
        self,
        ddb_client,
        ddb_load_table,
        etag,
    ):
        self.ddb_client = ddb_client
        self.ddb_load_table = ddb_load_table
        self.etag = etag

        self.current_line = 0
        self.current_hundreds = 0

        self._buffered_start_hundred_row = None
        self._buffered_rows = None

    def _get_rows(self, start):
        response = self.ddb_client.batch_get_item(
            RequestItems={
                self.ddb_load_table: {
                    "Keys": [
                        {
                            "S3ETag": {
                                "S": self.etag,
                            },
                            "RowNumberHundreds": {
                                "N": str(i),
                            },
                        }
                        for i in range(start, start + 25)
                    ],
                },
            },
        )
        rows = []
        for ddb_row in response.get("Responses", {}).get(self.ddb_load_table, []):
            contentString = ddb_row.get("Content", {}).get("S", None)
            if contentString is None:
                raise Exception(
                    f"Received a DDB row without content from ETag {self.etag}"
                )
            content = json.loads(contentString)
            rows.extend(content)

        self._buffered_start_hundred_row = start
        self._buffered_rows = rows

    def read(self):
        raise io.UnsupportedOperation("read not implemented in Laminar load DDB stream")

    def detach(self):
        raise io.UnsupportedOperation(
            "detach not implemented in Laminar load DDB stream"
        )

    def readline(self, size=-1):
        if size != -1:
            raise io.UnsupportedOperation(
                "readline with a size parameter is not implemented in Laminar load DDB stream"
            )

        if (
            # Initial state, fill up the buffer
            self._buffered_start_hundred_row is None
            or self._buffered_rows is None
            # Weird state
            or self.current_line - self._buffered_start_hundred_row * 100 < 0
            # We've passed the buffer
            or self.current_line - self._buffered_start_hundred_row * 100
            > len(self._buffered_rows) - 1
        ):
            # Fill the buffer with rows from DDB
            self._get_rows(self.current_line // 100)
        # Reached the end of the line
        if (
            self.current_line - self._buffered_start_hundred_row * 100
            > len(self._buffered_rows) - 1
        ):
            return ""
        row = self._buffered_rows[
            self.current_line - self._buffered_start_hundred_row * 100
        ]
        self.current_line += 1
        return row

    def seek(self, offset, whence=0):
        if whence != 0:
            raise io.UnsupportedOperation(
                "seek with a whence parameter is not implemented in Laminar load DDB stream"
            )
        self._get_rows(offset // 100)

    def tell(self):
        return self.current_line


class DynamoDBLoader(Loader):
    """Loader to load source from dynamodb."""

    # Public

    options = [
        "s3_endpoint_url",
        "ddb_endpoint_url",
        "ddb_load_table",
        "ddb_load_last_used_table",
    ]

    def __init__(
        self,
        bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
        s3_endpoint_url=None,
        ddb_endpoint_url=None,
        ddb_load_table=None,
        ddb_load_last_used_table=None,
    ):
        self.__bytes_sample_size = bytes_sample_size
        self.__s3_endpoint_url = (
            s3_endpoint_url
            or os.environ.get("S3_ENDPOINT_URL")
            or config.S3_DEFAULT_ENDPOINT_URL
        )
        self.__ddb_endpoint_url = ddb_endpoint_url or os.environ.get("DDB_ENDPOINT")
        self.__s3_client = boto3.client("s3", endpoint_url=self.__s3_endpoint_url)
        self.__ddb_client = boto3.client(
            "dynamodb", endpoint_url=self.__ddb_endpoint_url
        )
        self.__ddb_load_table = ddb_load_table
        self.__ddb_load_last_used_table = ddb_load_last_used_table
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode="t", encoding=None):

        parts = urlparse(source, allow_fragments=False)
        s3_response = self.__s3_client.head_object(
            Bucket=parts.netloc, Key=parts.path[1:]
        )
        etag = s3_response.get("ETag", None)
        print(etag)

        # Update the last used table
        ddb_last_used_response = self.__ddb_client.put_item(
            Item={
                "S3ETag": {
                    "S": etag,
                },
                "LastUsed": {
                    "N": str(time.time()),
                },
            },
            TableName=self.__ddb_load_last_used_table,
        )
        if (
            ddb_last_used_response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode", None
            )
            != 200
        ):
            raise Exception(f"Error adding ETag {etag} to the ddb load last used table")

        ddb_response = self.__ddb_client.get_item(
            Key={
                "S3ETag": {
                    "S": etag,
                },
                "RowNumberHundreds": {
                    "N": "0",
                },
            },
            TableName=self.__ddb_load_table,
        )
        if "Item" not in ddb_response:
            print("Not found in DDB. Loading into DDB")
            response = self.__s3_client.get_object(
                Bucket=parts.netloc, Key=parts.path[1:]
            )

            bytes = io.BufferedRandom(io.BytesIO())
            contents = response["Body"].read()
            bytes.write(contents)
            bytes.seek(0)

            if self.__bytes_sample_size:
                sample = bytes.read(self.__bytes_sample_size)
                bytes.seek(0)
                encoding = helpers.detect_encoding(sample, encoding)

            # Prepare chars
            chars = io.TextIOWrapper(bytes, encoding)

            count = 0
            hundred_count = 0
            content = []

            def upload_to_ddb(content, hundred_count):
                r = self.__ddb_client.put_item(
                    Item={
                        "S3ETag": {
                            "S": etag,
                        },
                        "RowNumberHundreds": {
                            "N": str(hundred_count),
                        },
                        "Content": {"S": json.dumps(content)},
                    },
                    TableName=self.__ddb_load_table,
                )
                if r.get("ResponseMetadata", {}).get("HTTPStatusCode", None) != 200:
                    raise Exception(
                        f"Error adding rows {hundred_count*100} to {(hundred_count+1)*100} for ETag {etag} to the ddb load table"
                    )
                return r

            for line in chars.readlines():
                # TODO return the s3 wrapper if something fails here, including too large of a row sent to DDB because of a wide dataset
                content.append(line)
                if (count + 1) % 100 == 0:
                    upload_to_ddb(content, hundred_count)

                    hundred_count += 1
                    content = []
                count += 1
            if len(content) > 0:
                upload_to_ddb(content, hundred_count)

        else:
            print("Found in DDB. Continue on")

        chars = LaminarLoadDDBTextIO(self.__ddb_client, self.__ddb_load_table, etag)
        return chars

        exit()

        # Support only bytes
        if hasattr(source, "encoding"):
            message = "Only byte streams are supported."
            raise exceptions.SourceError(message)

        # Prepare bytes
        bytes = source
        if self.__stats:
            bytes = helpers.BytesStatsWrapper(bytes, self.__stats)

        # Return bytes
        if mode == "b":
            return bytes

        # Detect encoding
        if self.__bytes_sample_size:
            sample = bytes.read(self.__bytes_sample_size)
            bytes.seek(0)
            encoding = helpers.detect_encoding(sample, encoding)

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars
