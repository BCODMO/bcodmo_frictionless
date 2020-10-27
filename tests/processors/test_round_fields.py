import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": 4.323},
    {"col1": 362.6271},
    {"col1": 9132.62},
    {"col1": 310.6},
    {"col1": 310.9999},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_round_fields():
    flows = [
        data,
        round_fields({"fields": [{"name": "col1", "digits": 2,}]}),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == Decimal("4.32")
    assert rows[0][1]["col1"] == Decimal("362.63")
    assert rows[0][2]["col1"] == Decimal("9132.62")
    assert str(rows[0][3]["col1"]) == "310.6"
    assert str(rows[0][4]["col1"]) == "311"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_round_fields_maxiumum_precision():
    flows = [
        data,
        round_fields(
            {"fields": [{"name": "col1", "digits": 3, "maximum_precision": True}]}
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == Decimal("4.323")
    assert rows[0][1]["col1"] == Decimal("362.627")
    assert str(rows[0][2]["col1"]) == "9132.62"
    assert str(rows[0][3]["col1"]) == "310.6"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_round_fields_convert_to_integer():
    flows = [
        data,
        round_fields(
            {"fields": [{"name": "col1", "digits": 0, "convert_to_integer": True}]}
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == 4
    assert datapackage.resources[0].schema.fields[0].type == "integer"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_preserve_trailing_zeros():
    flows = [
        data,
        round_fields(
            {"fields": [{"name": "col1", "digits": 3, "preserve_trailing_zeros": True}]}
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert str(rows[0][3]["col1"]) == "310.600"
    assert str(rows[0][4]["col1"]) == "311.000"
