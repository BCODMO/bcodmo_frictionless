import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "hello", "col2": "world"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_rename_fields_regex():
    flows = [
        data,
        rename_fields_regex(
            {
                "fields": ["col1", "col2"],
                "pattern": {"find": "col(\\d)", "replace": "test\\1",},
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    f = datapackage.resources[0].schema.fields
    assert f[0].name == "test1"
    assert f[1].name == "test2"
