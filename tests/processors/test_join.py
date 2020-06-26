import pytest
import os
from dataflows import Flow, join
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)

data1 = [
    {"col1": 1},
    {"col1": 2},
    {"col1": 3},
]
data2 = [
    {"col2": 1},
    {"col2": 2},
    {"col2": 3},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_join():
    flows = [
        data2,
        data1,
        join(
            "res_1",
            "{#}",
            "res_2",
            "{#}",
            fields={"col2": {"name": "col2"}},
            source_delete=True,
            mode="half-outer",
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    print(rows)
    assert rows == [
        [{"col1": 1, "col2": 1}, {"col1": 2, "col2": 2}, {"col1": 3, "col2": 3}]
    ]
