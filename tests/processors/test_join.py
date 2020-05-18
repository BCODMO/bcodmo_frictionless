import pytest
import os
from dataflows import Flow, join
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)


data1 = [
    {"station_id": 123, "person": "John"},
    {"station_id": 456, "person": "Mary"},
    {"station_id": 456, "person": "Jane"},
    {"station_id": 789, "person": "Bob"},
]
data2 = [
    {"measurement": 50, "station_id": 123},
    {"measurement": 55, "station_id": 123},
    {"measurement": 49, "station_id": 456},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_join():
    flows = [
        data1,
        data2,
        join(
            source_name="res_1",
            source_key=["station_id"],
            target_name="res_2",
            target_key=["station_id"],
            fields={"person": {"name": "person", "aggregate": "any",},},
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1
    assert len(rows[0][0].keys()) == 3


@pytest.mark.skipif(TEST_DEV, reason="test development")
@pytest.mark.skip(TEST_DEV, reason="not implemented")
def test_join_row_number():
    flows = [
        data2,
        data1,
        join(
            source_name="res_1",
            source_key=["#"],
            target_name="res_2",
            target_key=["#"],
            fields={
                "measurement": {"name": "measurement", "aggregate": "any",},
                "wrong_station_id": {"name": "station_id"},
            },
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1
    assert len(rows[0]) == 4
    assert len(rows[0][0].keys()) == 4
