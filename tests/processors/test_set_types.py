import logging

for name in ["boto", "urllib3", "s3transfer", "boto3", "botocore", "nose", "requests"]:
    logging.getLogger(name).setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
import pytest
import boto3
import os
from dataflows import Flow
from dataflows.base import exceptions as dataflow_exceptions
from decimal import Decimal
from moto import mock_aws
from tabulator.exceptions import IOError as TabulatorIOError
import logging
import io
import csv
import hashlib
from moto.server import ThreadedMotoServer

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"


@mock_aws
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_set_types():
    os.environ["LAMINAR_S3_HOST"] = ""
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    conn.create_bucket(Bucket="testing_dump_bucket")
    conn.upload_file("data/test.csv", "testing_bucket", "test.csv")

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test.csv",
                "name": "res",
                "format": "csv",
                # "infer_strategy": "strings",
                # "cast_strategy": "strings",
            }
        ),
        set_types({"types": {"col4": {"type": "date", "format": "%m/%d/%y"}}}),
        dump_to_s3(
            {
                "prefix": "test",
                "force-format": True,
                "format": "csv",
                "save_pipeline_spec": True,
                "temporal_format_property": "outputFormat",
                "bucket_name": "testing_dump_bucket",
                "data_manager": "test",
            }
        ),
    ]

    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1
    assert datapackage.descriptor["count_of_rows"] == 4

    assert (
        datapackage.descriptor["resources"][0]["schema"]["fields"][3]["type"] == "date"
    )
