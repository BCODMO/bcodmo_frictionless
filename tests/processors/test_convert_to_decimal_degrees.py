import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal
import datetime
import dateutil

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)

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
