import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"hello": "world", "foo": "bar"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_update_fields():
    flows = [data, update_fields({"fields": {"hello": {"test_metadata": "testing"}}})]
    rows, datapackage, _ = Flow(*flows).results()
    fields = datapackage.resources[0].schema.fields

    assert "test_metadata" in fields[0].descriptor
    assert fields[0].descriptor["test_metadata"] == "testing"

    assert "test_metadata" not in fields[1].descriptor
