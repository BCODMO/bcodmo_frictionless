import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": 4.323, "col2": 1},
    {"col1": 362.6271, "col2": 1},
    {"col1": 9132.62, "col2": 1},
    {"col1": 310.6, "col2": 1},
    {"col1": 310.9999, "col2": 1},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "ft_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1_test",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert "col1_test" in rows[0][0]
    assert rows[0][0]["col1"] == Decimal("4.323")
    assert rows[0][0]["col1_test"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_no_preserve():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "ft_to_meter",
                        "preserve_field": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert rows[0][0]["col1"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_replace_same():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "ft_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert "col1" in rows[0][0]
    assert len(rows[0][0]) == 2
    assert rows[0][0]["col1"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_replace_existing():
    try:
        flows = [
            data,
            convert_units(
                {
                    "fields": [
                        {
                            "name": "col1",
                            "conversion": "ft_to_meter",
                            "preserve_field": True,
                            "new_field_name": "col2",
                        }
                    ]
                }
            ),
        ]
        rows, datapackage, _ = Flow(*flows).results()
        assert "Shouldn't get here" == True
    except:
        pass
