import pytest
import os
from dataflows import Flow
from decimal import Decimal
import dateutil.parser

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"

data = [
    {"col1": "abc", "col2": "2012-05-23T16:24:09Z", "col3": 5},
    {"col1": "heresabc", "col2": "2012-05-24T16:24:09Z", "col3": 6},
    {"col1": "nothere", "col2": "2012-05-25T16:24:09Z", "col3": 8},
]


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells():
    flows = [
        data,
        edit_cells(
            {"edited": [{"row": 1, "cells": [{"field": "col1", "value": "hello"}]}]}
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "hello"
    assert rows[0][1]["col1"] == "heresabc"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_multiple_rows_and_cells():
    flows = [
        data,
        edit_cells(
            {
                "edited": [
                    {
                        "row": 3,
                        "cells": [
                            {"field": "col1", "value": "row3col1"},
                            {"field": "col2", "value": "row3col2"},
                        ],
                    },
                    {"row": 1, "cells": [{"field": "col1", "value": "row1col1"}]},
                ]
            }
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "row1col1"
    assert rows[0][2]["col1"] == "row3col1"
    assert rows[0][2]["col2"] == "row3col2"
    # Untouched row is unchanged
    assert rows[0][1]["col1"] == "heresabc"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_legacy_dict_migration():
    # The legacy dict shape (keyed by row number) is still accepted and migrated
    # to the list shape internally.
    flows = [
        data,
        edit_cells({"edited": {1: [{"field": "col1", "value": "hello"}]}}),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col1"] == "hello"
    assert rows[0][1]["col1"] == "heresabc"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_list_unused():
    flows = [
        data,
        edit_cells(
            {"edited": [{"row": 5, "cells": [{"field": "col1", "value": "hello"}]}]}
        ),
    ]
    try:
        rows, datapackage, _ = Flow(*flows).results()
        # We shouldn't get here, it should error
        assert False == True
    except:
        pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_number_fail():
    flows = [
        data,
        set_types(
            {
                "types": {
                    "col3": {
                        "type": "number",
                    },
                }
            }
        ),
        edit_cells({"edited": [{"row": 3, "cells": [{"field": "col3", "value": "hello"}]}]}),
    ]
    try:
        rows, datapackage, _ = Flow(*flows).results()
        # We shouldn't get here, it should error
        assert False == True
    except:
        pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_number():
    flows = [
        data,
        set_types(
            {
                "types": {
                    "col3": {
                        "type": "number",
                    },
                }
            }
        ),
        edit_cells({"edited": [{"row": 3, "cells": [{"field": "col3", "value": "3"}]}]}),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][2]["col3"] == 3


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_date_fail():
    flows = [
        data,
        set_types(
            {
                "types": {
                    "col2": {
                        "type": "datetime",
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                    },
                }
            }
        ),
        edit_cells({"edited": [{"row": 1, "cells": [{"field": "col2", "value": "hello"}]}]}),
    ]
    try:
        rows, datapackage, _ = Flow(*flows).results()
        # We shouldn't get here, it should error
        assert False == True
    except:
        pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_date():
    flows = [
        data,
        set_types(
            {
                "types": {
                    "col2": {
                        "type": "datetime",
                        "format": "%Y-%m-%dT%H:%M:%SZ",
                    },
                }
            }
        ),
        edit_cells(
            {"edited": [{"row": 1, "cells": [{"field": "col2", "value": "2014-02-13T10:24:09Z"}]}]}
        ),
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][0]["col2"] == dateutil.parser.parse("2014-02-13T10:24:09")


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_unused():
    flows = [
        data,
        edit_cells({"edited": [{"row": 5, "cells": [{"field": "col1", "value": "hello"}]}]}),
    ]
    try:
        rows, datapackage, _ = Flow(*flows).results()
        # We shouldn't get here, it should error
        assert False == True
    except:
        pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_edit_cells_nonexistent_field():
    flows = [
        data,
        edit_cells({"edited": [{"row": 1, "cells": [{"field": "nonexistent", "value": "hello"}]}]}),
    ]
    with pytest.raises(Exception, match="not found"):
        Flow(*flows).results()
