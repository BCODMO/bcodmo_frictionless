import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"hello": "world", "foo": "bar"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_add_schema_metadata():
    flows = [
        data,
        data,
        add_schema_metadata({"test_metadata": "test", "resources": ["res_1"]}),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert "test_metadata" in datapackage.resources[0].schema.descriptor
    assert datapackage.resources[0].schema.descriptor["test_metadata"] == "test"

    assert "test_metadata" not in datapackage.resources[1].schema.descriptor
