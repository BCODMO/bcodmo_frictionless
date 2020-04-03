import logging
import pytest
from dataflows import Flow
from decimal import Decimal

from bcodmo_processors.bcodmo_pipeline_processors import (
    load,
    find_replace,
    round_fields,
    remove_resources,
    rename_fields,
    rename_fields_regex,
    concatenate,
    reorder_fields,
    update_fields,
    add_schema_metadata,
    convert_date,
    boolean_add_computed_field,
    boolean_filter_rows,
    convert_to_decimal_degrees,
    split_column,
    dump_to_path,
    rename_resource,
    string_format,
)

TEST_DEV = False


class TestBcodmoPipelineProcessors:
    def setup_class(self):
        self.load_basic = {
            "from": "data/test.csv",
            "format": "csv",
            "name": "res",
        }

    @pytest.mark.skipif(TEST_DEV, reason="test development")
    def test_load(self):
        flows = [load(self.load_basic.copy())]
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
    def test_find_replace(self):
        flows = [
            load(self.load_basic.copy()),
            find_replace(
                {
                    "fields": [
                        {
                            "name": "col1",
                            "patterns": [{"find": "abc", "replace": "test"}],
                        }
                    ]
                }
            ),
        ]
        rows, datapackage, _ = Flow(*flows).results()
        assert rows[0][0]["col1"] == "test"
        assert rows[0][2]["col1"] == "def"
