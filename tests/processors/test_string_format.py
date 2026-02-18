import pytest
import os
from dataflows import Flow, set_type
from decimal import Decimal

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "hello", "col2": "world"},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_string_format_nonexistent_field():
    flows = [
        data,
        string_format(
            {
                "fields": [
                    {
                        "output_field": "combined",
                        "input_string": "{} {}",
                        "input_fields": ["nonexistent", "col2"],
                    }
                ]
            }
        ),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
