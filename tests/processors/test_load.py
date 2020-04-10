import pytest
import os
from dataflows import Flow
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *

TEST_DEV = os.environ.get("TEST_DEV", False)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_csv():
    flows = [load({"from": "data/test.csv", "name": "res", "format": "csv"})]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1
    assert rows[0][0] == {
        "col1": "abc",
        "col2": 1,
        "col3": Decimal("1.532"),
        "col4": "12/29/19",
    }
    assert rows[0][1] == {
        "col1": "abc",
        "col2": 2,
        "col3": Decimal("35.131"),
        "col4": "12/30/19",
    }
    assert rows[0][2] == {
        "col1": "def",
        "col2": 1,
        "col3": Decimal("53.1"),
        "col4": "12/31/19",
    }


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx():
    flows = [
        load({"from": "data/test.xlsx", "name": "res", "format": "xlsx", "sheet": 2})
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1
    assert rows[0][0] == {
        "col1": "abc",
        "col2": 1,
        "col3": Decimal("1.532"),
        "col4": "12/29/19",
    }


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_sheet_regex():
    flows = [
        load(
            {
                "from": "data/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": "test\d",
                "sheet_regex": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 4
    assert datapackage.resources[0].name == "test2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_multiple():
    flows = [
        load(
            {
                "from": "data/test.csv,data/test.csv,data/test.csv",
                "name": "res",
                "format": "csv",
                "input_separator": ",",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 3


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_path_pattern():
    flows = [
        load(
            {
                "from": "data/*.csv",
                "name": "res",
                "format": "csv",
                "input_path_pattern": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_fixed_width():
    pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_seabird():
    pass