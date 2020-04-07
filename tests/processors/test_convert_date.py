import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal
import datetime
import dateutil

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)

data = [
    {"col1": "12/31/1995"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_date():
    flows = [
        data,
        convert_date(
            {
                "fields": [
                    {
                        "inputs": [{"field": "col1", "format": "%m/%d/%Y"}],
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

    assert rows[0][0]["datetime_field"] == dateutil.parser.parse("12/31/1995")
