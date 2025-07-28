import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "hello world"},
    {"col1": "foo bar"},
]

data2 = [
    {"col1": "hello     world"},
    {"col1": "foo bar"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column():
    flows = [
        data,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "hello world"
    assert rows[0][0]["f1"] == "hello"
    assert rows[0][0]["f2"] == "world"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column_delete_input():
    flows = [
        data,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                    }
                ],
                "delete_input": True,
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "col1" not in rows[0][0]
    assert "col1" not in [f.name for f in datapackage.resources[0].schema.fields]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_basename():
    flows = [
        data,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "delimiter": " ",
                        "output_fields": ["f1", "f2"],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "hello world"
    assert rows[0][0]["f1"] == "hello"
    assert rows[0][0]["f2"] == "world"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_delimiter_regex():
    flows = [
        data2,
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "delimiter": "\s+",
                        "output_fields": ["f1", "f2"],
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["f1"] == "hello"
    assert rows[0][0]["f2"] == "world"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column_preserve_metadata():
    # Test that metadata is preserved when preserve_metadata=True
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "string",
                        "description": "Original combined field"
                    }
                }
            }
        }),
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that both new fields have the metadata from the original field
    f1_field = None
    f2_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "f1":
            f1_field = field
        elif field.name == "f2":
            f2_field = field
    
    assert f1_field is not None
    assert "bcodmo:" in f1_field.descriptor
    assert f1_field.descriptor["bcodmo:"]["units"] == "string"
    assert f1_field.descriptor["bcodmo:"]["description"] == "Original combined field"
    
    assert f2_field is not None
    assert "bcodmo:" in f2_field.descriptor
    assert f2_field.descriptor["bcodmo:"]["units"] == "string"
    assert f2_field.descriptor["bcodmo:"]["description"] == "Original combined field"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_split_column_preserve_metadata_false():
    # Test that metadata is NOT preserved when preserve_metadata=False
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "string",
                        "description": "Original combined field"
                    }
                }
            }
        }),
        split_column(
            {
                "fields": [
                    {
                        "input_field": "col1",
                        "pattern": "(.*) (.*)",
                        "output_fields": ["f1", "f2"],
                        "preserve_metadata": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new fields do NOT have metadata
    f1_field = None
    f2_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "f1":
            f1_field = field
        elif field.name == "f2":
            f2_field = field
    
    assert f1_field is not None
    assert "bcodmo:" not in f1_field.descriptor
    
    assert f2_field is not None
    assert "bcodmo:" not in f2_field.descriptor
