"""
Differential tests for ``bcodmo_pipeline_processors.load`` -- the source that
seeds the DuckDB engine.

``load`` is a 0->N source, so unlike the transform processors there is no input
``data``: the load step IS step 0. Both lanes run the SAME live ``load.flow``
(the dataflows lane via the harness's generic ``bcodmo_pipeline_processors.``
branch, the DuckDB lane by driving ``load.flow`` over ``datastream()`` and
streaming each resource into ``ingest_iter``). What the differential actually
proves on the DuckDB side is that the ingest->dump round-trip preserves load's
exact all-VARCHAR bytes and all-``string`` schema, per resource and byte-for-byte.

Because both lanes execute the real bcodmo ``load``, every source format /
parser / loader is exercised end to end: plain CSV, Excel (sheet index / regex /
range / separator), the bcodmo-fixedwidth parser (seabird), the bcodmo-regex-csv
parser, multi-file naming, ``use_filename``, ``input_path_pattern`` globbing, and
S3 (via moto, including the bcodmo-aws streaming loader triggered by ``cache_id``).

Glob/multi-file fixtures use IDENTICAL files (``tests/data_load/{a,b,c}.csv``) so
resource ordering can never make the comparison flaky: whichever file becomes
res-1/res-2/res-3, the rows compare equal.
"""

import os
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
)

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
DATA = os.path.join(_REPO, "data")            # shared fixtures at repo root
DATA_LOCAL = os.path.join(_HERE, "data_load")  # identical CSVs for glob/multi
BUCKET = "testing_bucket"


def _load(**params):
    return [{"run": "bcodmo_pipeline_processors.load", "parameters": params}]


def _check(steps):
    """Assert both lanes agree on the resource set, typed rows+schema, and
    byte-identical CSV. Returns the duckdb-side {name: (rows, schema)} map."""
    _, dk = assert_equivalent_multi({}, {}, steps)
    assert_csv_identical_multi({}, {}, steps)
    return dk


@pytest.fixture
def s3():
    """A mocked S3 with an ``upload(local, key)`` helper and a preconfigured
    bucket. Improves on the copy-pasted ``@mock_aws`` + ``create_bucket`` +
    ``upload_file`` boilerplate: one fixture wraps the whole test (both
    differential lanes read the same mock), and endpoint env vars are pinned so
    the bcodmo-aws streaming loader resolves deterministically."""
    prev = {k: os.environ.get(k) for k in ("LAMINAR_S3_HOST", "S3_ENDPOINT_URL")}
    os.environ["LAMINAR_S3_HOST"] = ""
    os.environ.pop("S3_ENDPOINT_URL", None)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield SimpleNamespace(
            client=client,
            bucket=BUCKET,
            upload=lambda local, key: client.upload_file(local, BUCKET, key),
        )
    for k, v in prev.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


# -- local files -----------------------------------------------------------

def test_load_csv():
    dk = _check(_load(**{"from": os.path.join(DATA, "test.csv"), "name": "res", "format": "csv"}))
    assert list(dk) == ["res"]
    assert [f["name"] for f in dk["res"][1]] == ["col1", "col2", "col3", "col4"]
    assert all(f["type"] == "string" for f in dk["res"][1])
    assert len(dk["res"][0]) == 4


def test_load_xlsx_sheet_index():
    _check(_load(**{"from": os.path.join(DATA, "test.xlsx"), "name": "res", "format": "xlsx", "sheet": 2}))


def test_load_xlsx_sheet_regex():
    dk = _check(_load(**{
        "from": os.path.join(DATA, "test.xlsx"),
        "name": "res", "format": "xlsx", "sheet": r"test\d", "sheet_regex": True,
    }))
    assert len(dk) == 4  # one resource per matching sheet


def test_load_xlsx_sheet_range():
    dk = _check(_load(**{"from": os.path.join(DATA, "test.xlsx"), "name": "res", "format": "xlsx", "sheet": "1-3"}))
    assert len(dk) == 3


def test_load_xlsx_sheet_multiple_separator():
    dk = _check(_load(**{
        "from": os.path.join(DATA, "test.xlsx"),
        "name": "res", "format": "xlsx", "sheet": "test2,test3", "sheet_separator": ",",
    }))
    assert len(dk) == 2


def test_load_fixedwidth_seabird():
    dk = _check(_load(**{
        "from": os.path.join(DATA, "seabird_load.cnv"),
        "name": "res", "format": "bcodmo-fixedwidth", "infer": True,
        "parse_seabird_header": True, "deduplicate_headers": True,
        "skip_rows": ["#", "*"],
    }))
    # The bcodmo-fixedwidth parser inferred a wide seabird schema.
    assert [f["name"] for f in dk["res"][1]][:3] == ["prDM", "t090C", "t190C"]


def test_load_regex_csv():
    dk = _check(_load(**{
        "from": os.path.join(DATA, "test_regex.csv"),
        "name": "res", "format": "bcodmo-regex-csv",
        "skip_rows": ["#", "*"], "delimiter": r"\s+",
    }))
    assert len(dk["res"][1]) == 4


def test_load_multiple_files_comma():
    a = os.path.join(DATA_LOCAL, "a.csv")
    b = os.path.join(DATA_LOCAL, "b.csv")
    dk = _check(_load(**{"from": f"{a},{b}", "name": "res", "format": "csv", "input_separator": ","}))
    assert len(dk) == 2  # res-1, res-2


def test_load_list_from():
    dk = _check(_load(**{
        "from": [os.path.join(DATA_LOCAL, "a.csv"), os.path.join(DATA_LOCAL, "b.csv")],
        "name": "res", "format": "csv",
    }))
    assert len(dk) == 2


def test_load_use_filename():
    dk = _check(_load(**{"from": [os.path.join(DATA_LOCAL, "a.csv")], "use_filename": True, "format": "csv"}))
    assert list(dk) == ["a"]  # resource named from the filename


def test_load_input_path_pattern():
    # Identical files -> resource ordering is irrelevant to the comparison.
    dk = _check(_load(**{
        "from": os.path.join(DATA_LOCAL, "*.csv"),
        "name": "res", "format": "csv", "input_path_pattern": True,
    }))
    assert len(dk) == 3


def test_load_then_set_types_integration():
    # load seeds the engine, then a transform runs on the loaded resource: proves
    # load integrates into the normal step loop, not just as a standalone source.
    steps = _load(**{"from": os.path.join(DATA, "test.csv"), "name": "res", "format": "csv"})
    steps.append({
        "run": "bcodmo_pipeline_processors.set_types",
        "parameters": {"types": {"col2": {"type": "integer"}, "col3": {"type": "number"}}},
    })
    dk = _check(steps)
    types = {f["name"]: f["type"] for f in dk["res"][1]}
    assert types["col2"] == "integer" and types["col3"] == "number"


# -- S3 via moto -----------------------------------------------------------

def test_load_s3_csv(s3):
    s3.upload(os.path.join(DATA, "test.csv"), "test.csv")
    dk = _check(_load(**{"from": f"s3://{BUCKET}/test.csv", "name": "res", "format": "csv"}))
    assert len(dk["res"][0]) == 4


def test_load_s3_input_path_pattern(s3):
    for key in ("1/a.csv", "1/b.csv", "1/c.csv"):
        s3.upload(os.path.join(DATA_LOCAL, os.path.basename(key)), key)
    dk = _check(_load(**{
        "from": f"s3://{BUCKET}/1/*.csv",
        "name": "res", "format": "csv", "input_path_pattern": True,
    }))
    assert len(dk) == 3


def test_load_s3_xlsx_sheet_regex(s3):
    s3.upload(os.path.join(DATA, "test.xlsx"), "test.xlsx")
    dk = _check(_load(**{
        "from": f"s3://{BUCKET}/test.xlsx",
        "name": "res", "format": "xlsx", "sheet": r"test\d", "sheet_regex": True,
    }))
    assert len(dk) == 4


def test_load_s3_cache_id_bcodmo_aws_loader(s3):
    # cache_id switches the scheme to bcodmo-aws -> the BcodmoAWS streaming loader.
    s3.upload(os.path.join(DATA, "test.csv"), "test.csv")
    dk = _check(_load(**{
        "from": f"s3://{BUCKET}/test.csv",
        "name": "res", "format": "csv", "cache_id": "123",
    }))
    assert len(dk["res"][0]) == 4
