import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_concatenate():
    flows = [
        load({"from": "data/test.csv", "name": "res_1", "format": "csv"}),
        load({"from": "data/test.csv", "name": "res_2", "format": "csv"}),
        concatenate(
            {
                "sources": ["res_1", "res_2"],
                "fields": {"col1": [], "col2": [], "col3": [], "col4": []},
                "include_source_names": [
                    {"type": "resource", "column_name": "source_resource"},
                    {"type": "path", "column_name": "source_path"},
                    {"type": "file", "column_name": "source_file"},
                ],
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1
    assert len(datapackage.resources[0].schema.fields) == 7

    assert len(rows[0]) == 8
    assert rows[0][0]["source_resource"] == "res_1"
    assert rows[0][0]["source_path"] == "data/test.csv"
    assert rows[0][0]["source_file"] == "test.csv"
