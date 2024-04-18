import pytest
import boto3
import os
from dataflows import Flow
from dataflows.base import exceptions as dataflow_exceptions
from decimal import Decimal
from moto import mock_s3
from tabulator.exceptions import IOError as TabulatorIOError
import logging

from bcodmo_frictionless.bcodmo_pipeline_processors import *


TEST_DEV = os.environ.get("TEST_DEV", False) == "true"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_csv():
    flows = [
        load(
            {
                "from": "data/test.csv",
                "name": "res",
                "format": "csv",
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1
    assert rows[0][0] == {
        "col1": "abc",
        "col2": "1",
        "col3": "1.532",
        "col4": "12/29/19",
    }
    assert rows[0][1] == {
        "col1": "abc",
        "col2": "2",
        "col3": "35.131",
        "col4": "12/30/19",
    }
    assert rows[0][2] == {
        "col1": "def",
        "col2": "1",
        "col3": "53.1",
        "col4": "12/31/19",
    }


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx():
    flows = [
        load(
            {
                "from": "data/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 2,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1
    assert rows[0][0] == {
        "col1": "abc",
        "col2": "1",
        "col3": "1.532",
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
                "sheet": r"test\d",
                "sheet_regex": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 4
    assert datapackage.resources[0].name == "test2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_sheet_regex_multiple():
    flows = [
        load(
            {
                "from": ["data/test.xlsx", "data/test.xlsx"],
                "name": "res",
                "format": "xlsx",
                "sheet": r"test\d",
                "sheet_regex": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 8
    assert datapackage.resources[0].name == "res-1-test2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_sheet_range():
    flows = [
        load(
            {
                "from": "data/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": "1-3",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 3
    assert datapackage.resources[0].name == "1"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_sheet_multiple():
    flows = [
        load(
            {
                "from": "data/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": "test2,test3",
                "sheet_separator": ",",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 2
    assert datapackage.resources[0].name == "test2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_sheet_multiple_range():
    flows = [
        load(
            {
                "from": "data/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": "1-3,test4",
                "sheet_separator": ",",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 4
    assert datapackage.resources[0].name == "1"
    assert datapackage.resources[3].name == "test4"


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
    assert len(datapackage.resources) == 3


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_fixed_width():
    pass


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_seabird():
    flows = [
        load(
            {
                "from": "data/seabird_load.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert [f.name for f in datapackage.resources[0].schema.fields] == [
        "prDM",
        "t090C",
        "t190C",
        "c0S/m",
        "c1S/m",
        "sbeox0V",
        "flECO-AFL",
        "turbWETntu0",
        "sal00 (1)",
        "spar",
        "par",
        "cpar",
        "depSM",
        "sal00 (2)",
        "sal11",
        "sbeox0ML/L",
        "svCM",
        "sigma-é00",
        "sigma-é11",
        "flag",
    ]
    print(rows[0][0])
    assert rows[0][0] == {
        "prDM": "3.000",
        "t090C": "8.2738",
        "t190C": "8.2746",
        "c0S/m": "3.493425",
        "c1S/m": "3.493550",
        "sbeox0V": "2.7741",
        "flECO-AFL": "1.1540",
        "turbWETntu0": "0.4288",
        "sal00 (1)": "33.3666",
        "spar": "-9.990e-29",
        "par": "1.0538e+00",
        "cpar": "4.6952e+01",
        "depSM": "2.977",
        "sal00 (2)": "33.3666",
        "sal11": "33.3672",
        "sbeox0ML/L": "6.8136",
        "svCM": "1481.54",
        "sigma-é00": "25.9506",
        "sigma-é11": "25.9509",
        "flag": "0.0000e+00",
    }


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_seabird_infer_bug():
    flows = [
        load(
            {
                "from": "data/seabird_load_infer_bug.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert rows[0][107]["t090C"] == "10.0697"


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_s3():
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    flows = [
        load(
            {
                "from": "s3://testing_bucket/test.csv",
                "name": "res",
                "format": "csv",
            }
        )
    ]
    try:
        rows, datapackage, _ = Flow(*flows).results()
        # ensure that it does indeed through an exception
        assert False
    except TabulatorIOError:
        pass
    except dataflow_exceptions.SourceLoadError:
        pass
    except dataflow_exceptions.ProcessorError as e:
        assert type(e.cause) in [TabulatorIOError, dataflow_exceptions.SourceLoadError]

    # add the file
    conn.upload_file("data/test.csv", "testing_bucket", "test.csv")

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test.csv",
                "name": "res",
                "format": "csv",
                "cache_id": "123",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 1


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_s3_path():
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    # add the file
    conn.upload_file("data/test.csv", "testing_bucket", "test1.csv")
    conn.upload_file("data/test.csv", "testing_bucket", "test2.csv")
    conn.upload_file("data/test.csv", "testing_bucket", "test3.csv")

    flows = [
        load(
            {
                "from": "s3://testing_bucket/*.csv",
                "name": "res",
                "format": "csv",
                "input_path_pattern": True,
                "cache_id": "123",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 3


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_s3_path_xlsx_regex():
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    # add the file
    conn.upload_file("data/test.xlsx", "testing_bucket", "test.xlsx")

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": r"test\d",
                "sheet_regex": True,
                "cache_id": "123",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 4
    assert datapackage.resources[0].name == "test2"
    assert rows[0][0]["col5"] == "abc"
    assert rows[1][0]["col2"] == "1"
    assert rows[2][2]["col7"] == "53.1"
    assert rows[3][1]["col6"] == "2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_list():
    flows = [
        load(
            {
                "from": ["data/test.csv", "data/test.csv"],
                "name": "res",
                "format": "csv",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 2


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_use_filename():
    flows = [load({"from": ["data/test.csv"], "use_filename": True, "format": "csv"})]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "test"
    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_use_filename_multiple():
    flows = [
        load(
            {
                "from": ["data/test.csv", "data/test.csv"],
                "use_filename": True,
                "format": "csv",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 2
    assert datapackage.resources[0].name == "test"
    assert datapackage.resources[1].name == "test_2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_capture_skipped_rows():
    flows = [
        load(
            {
                "from": "data/seabird_load.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
                "seabird_capture_skipped_rows": [
                    {"column_name": "test1", "regex": "# start_time = (.*)"}
                ],
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "test1" in rows[0][0]
    assert rows[0][0]["test1"] == "Apr 27 2018 01:53:55 [NMEA time, header]"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_capture_skipped_rows_multiple_matches():
    flows = [
        load(
            {
                "from": "data/seabird_load.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
                "seabird_capture_skipped_rows": [
                    {"column_name": "test1", "regex": r"\*\* (.*)"}
                ],
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "test1" in rows[0][0]
    assert rows[0][0]["test1"] == "Testing match multiple;another match;again"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_capture_skipped_rows_multiple_matches_no_join():
    flows = [
        load(
            {
                "from": "data/seabird_load.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
                "seabird_capture_skipped_rows": [
                    {"column_name": "test1", "regex": r"\*\* (.*)"}
                ],
                "seabird_capture_skipped_rows_join": False,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "test1" not in rows[0][0]
    assert rows[0][0]["test1 (1)"] == "Testing match multiple"
    assert rows[0][0]["test1 (2)"] == "another match"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_capture_skipped_rows_multiple_matches_separator():
    flows = [
        load(
            {
                "from": "data/seabird_load.cnv",
                "name": "res",
                "format": "bcodmo-fixedwidth",
                "infer": True,
                "parse_seabird_header": True,
                "deduplicate_headers": True,
                "skip_rows": ["#", "*"],
                "seabird_capture_skipped_rows": [
                    {"column_name": "test1", "regex": r"\*\* (.*)"}
                ],
                "seabird_capture_skipped_rows_join_string": ":",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "test1" in rows[0][0]
    assert rows[0][0]["test1"] == "Testing match multiple:another match:again"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_xlsx_scientific_notation():
    flows = [
        load(
            {
                "from": "data/test_scientific_notation.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": 1,
                "preserve_formatting": True,
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert len(rows) == 1
    assert str(rows[0][0]["scientific_notation"]) == "4.273E-07"


@mock_s3
@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_s3_path_xlsx_regex_object_spaces():
    # create bucket and put objects
    conn = boto3.client("s3")
    conn.create_bucket(Bucket="testing_bucket")
    # add the file
    conn.upload_file("data/test.xlsx", "testing_bucket", "test with spaces.xlsx")

    flows = [
        load(
            {
                "from": "s3://testing_bucket/test with spaces.xlsx",
                "name": "res",
                "format": "xlsx",
                "sheet": r"test\d",
                "sheet_regex": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert len(datapackage.resources) == 4
    assert datapackage.resources[0].name == "test2"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_regex_csv():
    flows = [
        load(
            {
                "from": "data/test_regex.csv",
                "name": "res",
                "format": "bcodmo-regex-csv",
                "skip_rows": ["#", "*"],
                "delimiter": r"\s+",
                "infer_strategy": "strings",
                "cast_strategy": "strings",
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()

    assert len(datapackage.resources[0].schema.fields) == 4

    assert len(rows) == 1
    assert rows[0][0] == {
        "col1": "abc",
        "col2": "1",
        "col3": "1.532",
        "col4": "12/29/19",
    }
    assert rows[0][1] == {
        "col1": "abc",
        "col2": "2",
        "col3": "35.131",
        "col4": "12/30/19",
    }
    assert rows[0][2] == {
        "col1": "def",
        "col2": "1",
        "col3": "53.1",
        "col4": "12/31/19",
    }


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_regex_csv_capture_skipped_rows():
    flows = [
        load(
            {
                "from": "data/test_regex.csv",
                "name": "res",
                "format": "bcodmo-regex-csv",
                "delimiter": r"\s+",
                "skip_rows": ["#", "*"],
                "capture_skipped_rows": [
                    {"column_name": "test1", "regex": r"\*\* (.*)"}
                ],
                "capture_skipped_rows_join": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "test1" in rows[0][0]
    assert len(rows[0][0]) == 5
    assert rows[0][0]["test1"] == "Testing multiple;another match multiple;again"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_regex_csv_capture_skipped_rows_column_name_bug():
    flows = [
        load(
            {
                "from": ["data/test_regex2.txt"],
                "name": "res",
                "format": "bcodmo-regex-csv",
                "delimiter": r"\s+",
                "skip_rows": [{"value": r"\*{6}.*", "type": "regex"}],
                "capture_skipped_rows": [
                    {"column_name": "EXPOCODE", "regex": r"EXPOCODE\s+(.*)\s+WHP.*"}
                ],
                "headers": [4, 5],
                "ignore_blank_headers": False,
                "capture_skipped_rows_join": False,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert "EXPOCODE" in rows[0][0]
    assert "QUALT1" in rows[0][0]
    assert len(rows[0][0]) == 7
    assert rows[0][0]["EXPOCODE"] == "316N145_10"


@pytest.mark.skipif(TEST_DEV, reason="test development")
def test_load_csv_multiline_header():
    flows = [
        load(
            {
                "from": "data/test_multiline_header.csv",
                "name": "res",
                "format": "csv",
                "headers": [1, 2],
                "multiline_headers_joiner": ";",
                "infer_strategy": "strings",
                "cast_strategy": "strings",
                "multiline_headers_duplicates": True,
            }
        )
    ]
    rows, datapackage, _ = Flow(*flows).results()
    assert datapackage
    assert len(datapackage.resources) == 1
    assert datapackage.resources[0].name == "res"
    assert len(datapackage.resources[0].schema.fields) == 5
    assert datapackage.resources[0].schema.fields[0].name == "1;5"
    assert datapackage.resources[0].schema.fields[2].name == "3;3"
