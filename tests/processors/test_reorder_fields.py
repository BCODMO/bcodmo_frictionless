import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"hello": "world", "foo": "bar", "col": "test"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_reorder_fields():
    flows = [data, reorder_fields({"fields": ["col", "foo", "hello"]})]
    rows, datapackage, _ = Flow(*flows).results()
    fields = datapackage.resources[0].schema.fields
    assert [f.name for f in fields] == ["col", "foo", "hello"]
