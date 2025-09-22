import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal
import datetime
import dateutil

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

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

# decimalYear
data_7 = [{"col1": "2019.938396"}]

# matlab
data_8 = [{"col1": "737117.593762207"}]

# Bug with set types
data_9 = [{"col1": "2018-02-17", "col2": "05:54:44"}]

# preserve_metadata test
data_10 = [{"col1": "12/31/1995 11:24:31"}]


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
def test_convert_date_decimal_year():
    flows = [
        data_7,
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "decimalYear",
                        "input_field": "col1",
                        "decimal_year_start_day": "1",
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    print(rows[0])
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse(
        "12/08/2019 12:20:56.256"
    )

    flows = [
        data_7,
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "decimalYear",
                        "input_field": "col1",
                        "decimal_year_start_day": "0",
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    print(rows[0])
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse(
        "12/09/2019 12:20:56.256"
    )


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


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_matlab():
    flows = [
        data_8,
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "matlab",
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
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse(
        "2018-02-26-26 14:15:01.054681"
    )


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_set_types_bug():
    # Fixing a bug that came up when setting the time type
    flows = [
        data_9,
        set_types(
            {
                "types": {
                    "col1": {
                        "type": "date",
                        "format": "%Y-%m-%d",
                        "outputFormat": "%Y-%m-%d",
                    },
                    "col2": {
                        "type": "time",
                        "format": "%H:%M:%S",
                    },
                }
            }
        ),
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [
                            {"field": "col1", "format": "%Y-%m-%d"},
                            {"field": "col2", "format": "%H:%M:%S"},
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
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("2018-02-17T05:54:44")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_string():
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
                        "output_type": "string",
                        "year": "1995",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("12/31/1995").strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    assert datapackage.resources[0].schema.fields[2].type == "string"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_preserve_metadata():
    # Test that metadata is preserved when preserve_metadata is True
    flows = [
        data_10,
        update_fields(
            {
                "fields": {
                    "col1": {
                        "bcodmo:": {
                            "units": "datetime",
                            "description": "Original datetime field with metadata",
                        }
                    }
                }
            }
        ),
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y %H:%M:%S"}],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    # Check that the new field has the metadata from the original field
    datetime_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "datetime_field":
            datetime_field = field
            break

    assert datetime_field is not None
    assert "bcodmo:" in datetime_field.descriptor
    assert datetime_field.descriptor["bcodmo:"]["units"] == "datetime"
    assert (
        datetime_field.descriptor["bcodmo:"]["description"]
        == "Original datetime field with metadata"
    )


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_preserve_metadata_false():
    # Test that metadata is NOT preserved when preserve_metadata is False or omitted
    flows = [
        data_10,
        update_fields(
            {
                "fields": {
                    "col1": {
                        "bcodmo:": {
                            "units": "datetime",
                            "description": "Original datetime field with metadata",
                        }
                    }
                }
            }
        ),
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y %H:%M:%S"}],
                        "output_field": "datetime_field",
                        "output_format": "%Y-%m-%dT%H:%M:%SZ",
                        "output_type": "datetime",
                        "preserve_metadata": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    # Check that the new field does NOT have metadata
    datetime_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "datetime_field":
            datetime_field = field
            break

    assert datetime_field is not None
    assert "bcodmo:" not in datetime_field.descriptor


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date_preserve_metadata_input_field():
    # Test preserve_metadata with single input_field (backwards compatibility)
    flows = [
        data_4,  # decimalDay data
        update_fields(
            {
                "fields": {
                    "col1": {
                        "bcodmo:": {
                            "units": "decimal_day",
                            "description": "Original decimal day field",
                        }
                    }
                }
            }
        ),
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
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    # Check that the new field has the metadata from the original field
    datetime_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "datetime_field":
            datetime_field = field
            break

    assert datetime_field is not None
    assert "bcodmo:" in datetime_field.descriptor
    assert datetime_field.descriptor["bcodmo:"]["units"] == "decimal_day"
    assert (
        datetime_field.descriptor["bcodmo:"]["description"]
        == "Original decimal day field"
    )


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_utc_time_convert():
    flows = [
        load(
            {
                "from": "data/test_utc_time_conversion.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 1,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
                "headers": 10,
                "preserve_formatting": True,
            }
        ),
        convert_date(
            {
                "fields": [
                    {
                        "input_type": "python",
                        "inputs": [
                            {
                                "field": "Time (HST)",
                                "format": "%H:%M",
                            }
                        ],
                        "input_timezone": "HST",
                        "output_field": "Time_UTC",
                        "output_format": "%H:%M",
                        "output_type": "time",
                        "output_timezone": "UTC",
                        "year": "2000",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert rows[0][0]["Time (HST)"] == "10:26"
    assert rows[0][0]["Time_UTC"] == datetime.time(20, 26)
