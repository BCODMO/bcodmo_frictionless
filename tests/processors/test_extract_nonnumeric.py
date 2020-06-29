import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "42"},
    {"col1": "15.2"},
    {"col1": "this is a string"},
    {"col1": "341 and a string"},
    {"col1": "12"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_extract_nonnumeric():
    flows = [
        data,
        extract_nonnumeric({"fields": ["col1"], "suffix": "_comment"}),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "42"
    assert rows[0][0]["col1_comment"] == None
    assert rows[0][2]["col1"] == None
    assert rows[0][2]["col1_comment"] == "this is a string"
    assert rows[0][3]["col1"] == None
    assert rows[0][3]["col1_comment"] == "341 and a string"

    fields = datapackage.resources[0].schema.fields
    assert fields[0].name == "col1"
    assert fields[1].name == "col1_comment"
