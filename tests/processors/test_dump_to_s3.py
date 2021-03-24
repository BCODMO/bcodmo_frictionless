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
from moto import mock_s3
from tabulator.exceptions import IOError as TabulatorIOError
import logging

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_dump_s3():
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
            }
        ),
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
    body = (
        conn.get_object(Bucket="testing_dump_bucket", Key="test/res.csv")["Body"]
        .read()
        .decode("utf-8")
    )

    assert len(body)
    assert len(datapackage.resources) == 1


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_dump_scientific_notation():
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    conn.create_bucket(Bucket="testing_dump_bucket")
    conn.upload_file(
        "data/test_scientific_notation.xlsx",
        "testing_bucket",
        "test_scientific_notation.xlsx",
    )

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test_scientific_notation.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 1,
                "preserve_formatting": True,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        ),
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
    body = (
        conn.get_object(Bucket="testing_dump_bucket", Key="test/res.csv")["Body"]
        .read()
        .decode("utf-8")
    )

    assert len(body)
    assert body == "scientific_notation\n4.273E-07\n"
    assert len(datapackage.resources) == 1

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test_scientific_notation.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 1,
                "preserve_formatting": True,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        ),
        set_types({"types": {"scientific_notation": {"type": "number"}}}),
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
    body = (
        conn.get_object(Bucket="testing_dump_bucket", Key="test/res.csv")["Body"]
        .read()
        .decode("utf-8")
    )

    assert len(body)
    assert body == "scientific_notation\n0.0000004273\n"
    assert len(datapackage.resources) == 1

    # Now set type to scientific notation
    flows = [
        load(
            {
                "from": "s3://testing_bucket/test_scientific_notation.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 1,
                "preserve_formatting": True,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        ),
        set_types(
            {
                "types": {
                    "scientific_notation": {
                        "type": "number",
                        "numberOutputFormat": "scientificNotation",
                    }
                }
            }
        ),
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
    body = (
        conn.get_object(Bucket="testing_dump_bucket", Key="test/res.csv")["Body"]
        .read()
        .decode("utf-8")
    )

    assert len(body)
    assert body == "scientific_notation\n4.273e-7\n"
    assert len(datapackage.resources) == 1


data_1 = [
    {"col1": "-1.42E-14"},
]


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_dump_scientific_notation_negative():
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_dump_bucket")
    flows = [
        data_1,
        set_types(
            {
                "types": {
                    "col1": {
                        "type": "number",
                    },
                }
            }
        ),
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
    body = (
        conn.get_object(Bucket="testing_dump_bucket", Key="test/res_1.csv")["Body"]
        .read()
        .decode("utf-8")
    )

    assert len(body)
    assert body == "col1\n-0.0000000000000142\n"
