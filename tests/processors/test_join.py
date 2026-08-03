import pytest
import os
from dataflows import Flow, join
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

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
            {
                "source": {
                    "name": "res_1",
                    "key": "{#}",
                    "delete": True,
                },
                "target": {
                    "name": "res_2",
                    "key": "{#}",
                },
                "fields": {"col2": {"name": "col2"}},
                "mode": "half-outer",
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    print(rows)
    assert rows == [
        [{"col1": 1, "col2": 1}, {"col1": 2, "col2": 2}, {"col1": 3, "col2": 3}]
    ]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_join_mode_default_is_full_outer():
    # mode defaults to "full-outer" (aligned with the laminar_web frontend default):
    # unmatched source rows are kept in the output. Under "half-outer" only the
    # target rows would remain.
    source_data = [{"sk": 1, "v": "a"}, {"sk": 2, "v": "b"}, {"sk": 4, "v": "d"}]
    target_data = [{"tk": 1}, {"tk": 2}, {"tk": 3}]
    flows = [
        source_data,
        target_data,
        join(
            {
                "source": {"name": "res_1", "key": "{sk}", "delete": True},
                "target": {"name": "res_2", "key": "{tk}"},
                "fields": {"v": {"name": "v"}},
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    # full-outer keeps the 3 target rows plus the unmatched source row (sk=4).
    assert len(rows[0]) == 4


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_join_aggregate_default_is_first():
    # An omitted per-field aggregate defaults to "first" (aligned with the frontend
    # default), keeping the first matching source value. "any" would keep the last.
    source_data = [{"sk": 1, "v": "first_val"}, {"sk": 1, "v": "second_val"}]
    target_data = [{"tk": 1}]
    flows = [
        source_data,
        target_data,
        join(
            {
                "source": {"name": "res_1", "key": "{sk}", "delete": True},
                "target": {"name": "res_2", "key": "{tk}"},
                "fields": {"v": {"name": "v"}},
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["v"] == "first_val"
