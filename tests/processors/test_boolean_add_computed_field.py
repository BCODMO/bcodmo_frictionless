import pytest
import os
from dataflows import Flow
from decimal import Decimal
import dateutil.parser

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "abc", "col2": "2012-05-23T16:24:09Z"},
    {"col1": "heresabc", "col2": "2012-05-24T16:24:09Z"},
    {"col1": "nothere", "col2": "2012-05-25T16:24:09Z"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_boolean_add_computed_field():
    flows = [
        data,
        boolean_add_computed_field(
            {
                "fields": [
                    {
                        "target": "new_col1",
                        "type": "string",
                        "functions": [
                            {"boolean": "{col1} == 'heresabc'", "value": "{col1}"}
                        ],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["new_col1"] == None
    assert rows[0][1]["new_col1"] == "heresabc"
    assert rows[0][2]["new_col1"] == None


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_boolean_add_computed_field_datetime():
    flows = [
        data,
        boolean_add_computed_field(
            {
                "fields": [
                    {
                        "target": "new_col2",
                        "type": "datetime",
                        "functions": [
                            {"boolean": "{col1} == 'heresabc'", "value": "{col2}"}
                        ],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["new_col2"] == None
    assert rows[0][1]["new_col2"] == dateutil.parser.parse("2012-05-24T16:24:09Z")
    assert rows[0][2]["new_col2"] == None


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_boolean_add_computed_field_always_run():
    flows = [
        data,
        boolean_add_computed_field(
            {
                "fields": [
                    {
                        "target": "new_col1",
                        "type": "string",
                        "functions": [{"always_run": True, "value": "{col1}"}],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["new_col1"] == "abc"
    assert rows[0][1]["new_col1"] == "heresabc"
    assert rows[0][2]["new_col1"] == "nothere"
