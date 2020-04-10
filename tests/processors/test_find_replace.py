import pytest
import os
from dataflows import Flow
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False)

data = [
    {"col1": "abc"},
    {"col1": "heresabc"},
    {"col1": "nothere"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_find_replace():
    flows = [
        data,
        find_replace(
            {
                "fields": [
                    {"name": "col1", "patterns": [{"find": "abc", "replace": "test"}],}
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "test"
    assert rows[0][1]["col1"] == "herestest"
    assert rows[0][2]["col1"] == "nothere"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_find_replace_boolean():
    flows = [
        data,
        find_replace(
            {
                "fields": [
                    {"name": "col1", "patterns": [{"find": "abc", "replace": "test"}],}
                ],
                "boolean_statement": "{col1} != 'abc'",
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "abc"
    assert rows[0][1]["col1"] == "herestest"
    assert rows[0][2]["col1"] == "nothere"