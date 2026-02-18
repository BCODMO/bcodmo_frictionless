import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "hello", "col2": "world"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_rename_resource_nonexistent():
    flows = [
        data,
        rename_resource(
            {"old_resource": "nonexistent", "new_resource": "new_name"}
        ),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
