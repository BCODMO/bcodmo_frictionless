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
def test_concatenate_nonexistent_resource():
    flows = [
        data,
        concatenate(
            {
                "sources": ["nonexistent"],
                "fields": {"col1": []},
                "target": {"name": "output"},
            }
        ),
    ]
    with pytest.raises(Exception, match="did not match any resources"):
        Flow(*flows).results()
