import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal
import datetime
import dateutil

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)

# datetime
data_1 = [
    {"col1": "12/31/1995 11:24:31"},
]

# multiple
data_2 = [
    {"col1": "12", "col2": "31", "col3": "1995"},
]

# timezone
data_3 = [{"col1": "12/31/1999 23:59:59"}]

# decimalDay
data_4 = [{"col1": "74.324"}]

# excel
data_5 = [{"col1": "43510.32"}]

# test order
data_6 = [{"fake1": "data", "col1": "12/31/1995", "fake2": "data"}]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_datetime():
    flows = [
        data_1,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y %H:%M:%S"}],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage.resources[0].schema.fields[1].type == "datetime"
    assert (
        datapackage.resources[0].schema.fields[1].descriptor["outputFormat"]
        == "%Y-%m-%dT%H:%M:%SZ"
    )

    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("12/31/1995 11:24:31")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_date():
    flows = [
        data_1,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y 11:24:31"}],
                        "output_field": "date_field",
                        "output_format": "%Y-%m-%d",
                        "output_type": "date",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage.resources[0].schema.fields[1].type == "date"
    assert (
        datapackage.resources[0].schema.fields[1].descriptor["outputFormat"]
        == "%Y-%m-%d"
    )
    assert rows[0][0]["date_field"] == dateutil.parser.parse("12/31/1995").date()


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_time():
    flows = [
        data_1,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "12/31/1995 %H:%M:%S"}],
                        "output_field": "time_field",
                        "output_format": "%H:%M:%S",
                        "output_type": "time",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage.resources[0].schema.fields[1].type == "time"
    assert (
        datapackage.resources[0].schema.fields[1].descriptor["outputFormat"]
        == "%H:%M:%S"
    )
    assert (
        rows[0][0]["time_field"] == dateutil.parser.parse("12/31/1995 11:24:31").time()
    )


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_multiple():
    flows = [
        data_2,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [
                            {"field": "col1", "format": "%m"},
                            {"field": "col2", "format": "%d"},
                            {"field": "col3", "format": "%Y"},
                        ],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("12/31/1995")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_timezone():
    flows = [
        data_3,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y %H:%M:%S"}],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                        "input_timezone": "EST",
                        "output_timezone": "UTC",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("1/1/2000 4:59:59 UTC")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_year():
    flows = [
        data_2,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [
                            {"field": "col1", "format": "%m"},
                            {"field": "col2", "format": "%d"},
                        ],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                        "year": "1995",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("12/31/1995")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_decimal_day():
    flows = [
        data_4,
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "decimalDay",
                        "input_field": "col1",
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                        "year": "1995",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("3/15/1995 7:46:33.6")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_excel():
    flows = [
        data_5,
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "excel",
                        "input_field": "col1",
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("2/14/2019 7:40:48")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_order():
    # Ensure that the order of fields remain the same when a field is overwritten
    flows = [
        data_6,
    ]
    rows, datapackage, _ = Flow(*flows).results()
    prev_field_names = [f.name for f in datapackage.resources[0].schema.fields]

    flows = [
        data_6,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y"}],
                        "output_field": "col1",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    after_field_names = [f.name for f in datapackage.resources[0].schema.fields]

    assert prev_field_names == after_field_names
