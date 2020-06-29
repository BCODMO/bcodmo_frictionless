import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"hello": "world"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_remove_resources():
    flows = [data, remove_resources({"resources": ["res_1"]})]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 0
