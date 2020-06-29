import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "hello world"},
    {"col1": "foo bar"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column():
    flows = [
        data,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "hello world"
    assert rows[0][0]["f1"] == "hello"
    assert rows[0][0]["f2"] == "world"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column_delete_input():
    flows = [
        data,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                    }
                ],
                "delete_input": True,
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "col1" not in rows[0][0]
    assert "col1" not in [f.name for f in datapackage.resources[0].schema.fields]
