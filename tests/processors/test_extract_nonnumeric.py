import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "42"},
    {"col1": "15.2"},
    {"col1": "this is a string"},
    {"col1": "341 and a string"},
    {"col1": "12"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_extract_nonnumeric():
    flows = [
        data,
        extract_nonnumeric({"fields": ["col1"], "suffix": "_comment"}),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "42"
    assert rows[0][0]["col1_comment"] == None
    assert rows[0][2]["col1"] == None
    assert rows[0][2]["col1_comment"] == "this is a string"
    assert rows[0][3]["col1"] == None
    assert rows[0][3]["col1_comment"] == "341 and a string"

    fields = datapackage.resources[0].schema.fields
    assert fields[0].name == "col1"
    assert fields[1].name == "col1_comment"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_extract_nonnumeric_preserve_metadata():
    # Test that metadata is preserved when preserve_metadata=True
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "mixed",
                        "description": "Numeric field with occasional text comments"
                    }
                }
            }
        }),
        extract_nonnumeric({
            "fields": ["col1"], 
            "suffix": "_comment", 
            "preserve_metadata": True
        }),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field has the metadata from the original field
    comment_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "col1_comment":
            comment_field = field
            break
    
    assert comment_field is not None
    assert "bcodmo:" in comment_field.descriptor
    assert comment_field.descriptor["bcodmo:"]["units"] == "mixed"
    assert comment_field.descriptor["bcodmo:"]["description"] == "Numeric field with occasional text comments"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_extract_nonnumeric_preserve_metadata_false():
    # Test that metadata is NOT preserved when preserve_metadata=False
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "mixed",
                        "description": "Numeric field with occasional text comments"
                    }
                }
            }
        }),
        extract_nonnumeric({
            "fields": ["col1"], 
            "suffix": "_comment", 
            "preserve_metadata": False
        }),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field does NOT have metadata
    comment_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "col1_comment":
            comment_field = field
            break
    
    assert comment_field is not None
    assert "bcodmo:" not in comment_field.descriptor


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_extract_nonnumeric_nonexistent_field():
    flows = [
        data,
        extract_nonnumeric({"fields": ["nonexistent"]}),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
