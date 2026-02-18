import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": 4.323, "col2": 1},
    {"col1": 362.6271, "col2": 1},
    {"col1": 9132.62, "col2": 1},
    {"col1": 310.6, "col2": 1},
    {"col1": 310.9999, "col2": 1},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1_test",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert "col1_test" in rows[0][0]
    assert rows[0][0]["col1"] == Decimal("4.323")
    assert rows[0][0]["col1_test"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_no_preserve():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert rows[0][0]["col1"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_replace_same():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1",
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert "col1" in rows[0][0]
    assert len(rows[0][0]) == 2
    assert rows[0][0]["col1"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_replace_existing():
    try:
        flows = [
            data,
            convert_units(
                {
                    "fields": [
                        {
                            "name": "col1",
                            "conversion": "feet_to_meter",
                            "preserve_field": True,
                            "new_field_name": "col2",
                        }
                    ]
                }
            ),
        ]
        rows, datapackage, _ = Flow(*flows).results()
        assert "Shouldn't get here" == True
    except:
        pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_preserve_metadata_new_field():
    # Test that metadata is preserved when preserve_metadata=True and preserve_field=True (creates new field)
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "feet",
                        "description": "Original measurement in feet"
                    }
                }
            }
        }),
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1_meters",
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field has the metadata from the original field
    meters_field = None
    original_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "col1_meters":
            meters_field = field
        elif field.name == "col1":
            original_field = field
    
    assert meters_field is not None
    assert original_field is not None
    assert "bcodmo:" in meters_field.descriptor
    assert meters_field.descriptor["bcodmo:"]["units"] == "feet"
    assert meters_field.descriptor["bcodmo:"]["description"] == "Original measurement in feet"
    # Original field should also still have metadata
    assert "bcodmo:" in original_field.descriptor


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_preserve_metadata_false_new_field():
    # Test that metadata is NOT preserved when preserve_metadata=False and preserve_field=True
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "feet",
                        "description": "Original measurement in feet"
                    }
                }
            }
        }),
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": True,
                        "new_field_name": "col1_meters",
                        "preserve_metadata": False,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the new field does NOT have metadata
    meters_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "col1_meters":
            meters_field = field
            break
    
    assert meters_field is not None
    assert "bcodmo:" not in meters_field.descriptor


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_preserve_metadata_modify_field():
    # Test that metadata is preserved when preserve_metadata=True and preserve_field=False (modifies existing field)
    flows = [
        data,
        update_fields({
            "fields": {
                "col1": {
                    "bcodmo:": {
                        "units": "feet",
                        "description": "Measurement in feet, converted to meters"
                    }
                }
            }
        }),
        convert_units(
            {
                "fields": [
                    {
                        "name": "col1",
                        "conversion": "feet_to_meter",
                        "preserve_field": False,
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that the modified field still has metadata
    col1_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "col1":
            col1_field = field
            break
    
    assert col1_field is not None
    assert "bcodmo:" in col1_field.descriptor
    assert col1_field.descriptor["bcodmo:"]["units"] == "feet"
    assert col1_field.descriptor["bcodmo:"]["description"] == "Measurement in feet, converted to meters"
    # Check that conversion worked
    assert rows[0][0]["col1"] == Decimal("1.3176504")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_preserve_metadata_multiple_conversions():
    # Test preserve_metadata with multiple conversion types
    flows = [
        [{"depth_ft": 100.0, "distance_miles": 2.5}],
        update_fields({
            "fields": {
                "depth_ft": {
                    "bcodmo:": {
                        "units": "feet",
                        "description": "Water depth in feet"
                    }
                },
                "distance_miles": {
                    "bcodmo:": {
                        "units": "miles", 
                        "description": "Distance traveled in miles"
                    }
                }
            }
        }),
        convert_units(
            {
                "fields": [
                    {
                        "name": "depth_ft",
                        "conversion": "feet_to_meter",
                        "preserve_field": True,
                        "new_field_name": "depth_m",
                        "preserve_metadata": True,
                    },
                    {
                        "name": "distance_miles",
                        "conversion": "mile_to_km",
                        "preserve_field": True,
                        "new_field_name": "distance_km",
                        "preserve_metadata": True,
                    }
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    
    # Check that both new fields have metadata from their original fields
    depth_m_field = None
    distance_km_field = None
    for field in datapackage.resources[0].schema.fields:
        if field.name == "depth_m":
            depth_m_field = field
        elif field.name == "distance_km":
            distance_km_field = field
    
    assert depth_m_field is not None
    assert "bcodmo:" in depth_m_field.descriptor
    assert depth_m_field.descriptor["bcodmo:"]["units"] == "feet"
    assert depth_m_field.descriptor["bcodmo:"]["description"] == "Water depth in feet"
    
    assert distance_km_field is not None
    assert "bcodmo:" in distance_km_field.descriptor
    assert distance_km_field.descriptor["bcodmo:"]["units"] == "miles"
    assert distance_km_field.descriptor["bcodmo:"]["description"] == "Distance traveled in miles"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_convert_units_nonexistent_field():
    flows = [
        data,
        convert_units(
            {
                "fields": [
                    {
                        "name": "nonexistent",
                        "conversion": "feet_to_meter",
                    }
                ]
            }
        ),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
