import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal
import datetime
import dateutil

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data_1 = [
    {"latitude": "14° 24.0' N"},
]
data_1_directional = [
    {"latitude": "12° 1' S"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_to_decimal_degrees_ddm():
    flows = [
        data_1,
        convert_to_decimal_degrees(
            {
                "fields": [
                    {
                        "format": "degrees-decimal_minutes",
                        "input_field": "latitude",
                        "output_field": "latitude",
                        "pattern": "(?P<degrees>\d+)° *(?P<decimal_minutes>\d+\.*\d*)' *(?P<directional>\w).*",
                        "directional": "",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["latitude"] == Decimal("14.4")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_to_decimal_degrees_ddm():
    flows = [
        data_1_directional,
        convert_to_decimal_degrees(
            {
                "fields": [
                    {
                        "format": "degrees-decimal_minutes",
                        "input_field": "latitude",
                        "output_field": "latitude",
                        "pattern": "(?P<degrees>\d+)° (?P<decimal_minutes>\d+\.*\d*)' (?P<directional>\w).*",
                        "directional": "",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["latitude"] == Decimal("-12.01666666666666666666666667")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_to_decimal_degrees_preserve_metadata():
    # Test that metadata is preserved when preserve_metadata=True
    flows = [
        data_1,
        update_fields({
            "fields": {
                "latitude": {
                    "bcodmo:": {
                        "units": "degrees_decimal_minutes",
                        "description": "Latitude in DDM format"
                    }
                }
            }
        }),
        convert_to_decimal_degrees(
            {
                "fields": [
                    {
                        "format": "degrees-decimal_minutes",
                        "input_field": "latitude",
                        "output_field": "latitude_dd",
                        "pattern": "(?P<degrees>\d+)° (?P<decimal_minutes>\d+\.*\d*)' (?P<directional>\w).*",
                        "directional": "",
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field has the metadata from the original field
    latitude_dd_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "latitude_dd":
            latitude_dd_field = field
            break
    
    assert latitude_dd_field is not None
    assert "bcodmo:" in latitude_dd_field.descriptor
    assert latitude_dd_field.descriptor["bcodmo:"]["units"] == "degrees_decimal_minutes"
    assert latitude_dd_field.descriptor["bcodmo:"]["description"] == "Latitude in DDM format"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_to_decimal_degrees_preserve_metadata_false():
    # Test that metadata is NOT preserved when preserve_metadata=False
    flows = [
        data_1,
        update_fields({
            "fields": {
                "latitude": {
                    "bcodmo:": {
                        "units": "degrees_decimal_minutes",
                        "description": "Latitude in DDM format"
                    }
                }
            }
        }),
        convert_to_decimal_degrees(
            {
                "fields": [
                    {
                        "format": "degrees-decimal_minutes",
                        "input_field": "latitude",
                        "output_field": "latitude_dd",
                        "pattern": "(?P<degrees>\d+)° (?P<decimal_minutes>\d+\.*\d*)' (?P<directional>\w).*",
                        "directional": "",
                        "preserve_metadata": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field does NOT have metadata
    latitude_dd_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "latitude_dd":
            latitude_dd_field = field
            break
    
    assert latitude_dd_field is not None
    assert "bcodmo:" not in latitude_dd_field.descriptor


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_to_decimal_degrees_nonexistent_field():
    flows = [
        data_1,
        convert_to_decimal_degrees(
            {
                "fields": [
                    {
                        "format": "degrees-decimal_minutes",
                        "input_field": "nonexistent",
                        "output_field": "latitude_dd",
                        "pattern": "(?P<degrees>\\d+).\\s(?P<decimal_minutes>[\\d.]+).\\s(?P<directional>\\w)",
                    }
                ]
            }
        ),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
