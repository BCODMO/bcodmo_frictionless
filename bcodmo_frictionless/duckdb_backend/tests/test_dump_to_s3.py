"""
Differential tests for ``bcodmo_pipeline_processors.dump_to_s3`` -- the terminal
sink.

A dump has no comparable rows/schema output: its result is the set of S3 objects
it writes. So the gate here reads back EVERY uploaded object (per-resource CSVs,
``datapackage.json``, ``pipeline-spec.yaml``, unique-lat-lon files) from both lanes
and asserts they are byte-identical. Because both lanes drive the SAME live
``S3Dumper``, identity is by construction; the test proves the DuckDB lane feeds it
equivalent descriptors + typed rows (the ingest->dump round-trip).

The async multipart uploader (billiard ``Pool``) only lands objects in a REAL S3
endpoint, not in-process ``@mock_aws`` patching -- so these run against a
``ThreadedMotoServer`` with ``LAMINAR_S3_HOST`` pointed at it (the same setup the
top-level ``test_dump_to_s3`` uses for byte checks). Loads read a LOCAL fixture so
only the dump touches S3.
"""

import copy
import os

import boto3
import pytest
from dataflows import Flow
from moto.server import ThreadedMotoServer

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.engine import Engine
from bcodmo_frictionless.duckdb_backend.equivalence.differential import _dataflows_step

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
DATA = os.path.join(_REPO, "data")
SRC_CSV = os.path.join(DATA, "test.csv")
DUMP_BUCKET = "testing_dump_bucket"
PREFIX = "out"


@pytest.fixture
def s3_server():
    """A real (threaded) moto S3 endpoint so the dumper's async billiard uploads
    actually land and can be read back. Pins ``LAMINAR_S3_HOST`` + credentials so
    both the live ``S3Dumper`` (which builds its own clients from those env vars)
    and our read-back client hit the same server."""
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    keys = ("LAMINAR_S3_HOST", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
    prev = {k: os.environ.get(k) for k in keys}
    os.environ["LAMINAR_S3_HOST"] = endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    client = boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id="testing", aws_secret_access_key="testing",
    )
    client.create_bucket(Bucket=DUMP_BUCKET)
    try:
        yield client
    finally:
        server.stop()
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _snapshot(client):
    objs = client.list_objects_v2(Bucket=DUMP_BUCKET).get("Contents", [])
    return {
        o["Key"]: client.get_object(Bucket=DUMP_BUCKET, Key=o["Key"])["Body"].read()
        for o in objs
    }


def _wipe(client):
    objs = client.list_objects_v2(Bucket=DUMP_BUCKET).get("Contents", [])
    if objs:
        client.delete_objects(
            Bucket=DUMP_BUCKET,
            Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
        )


def _run_dataflows(steps):
    # Side-effects only (the dump writes to S3); ``.process()`` avoids the
    # row/schema extraction that a terminal dump -- which can ADD row-less
    # resources like pipeline-spec.yaml to the descriptor -- would trip over.
    # Steps are deep-copied because load/dump flow() pops its params.
    Flow(*[_dataflows_step(copy.deepcopy(s)) for s in steps]).process()


def _run_duckdb(steps):
    Engine().run(copy.deepcopy(steps))


def _assert_dump_identical(client, steps):
    """Run ``steps`` (ending in a dump) through both lanes against the shared
    moto server and assert the uploaded S3 objects are byte-identical."""
    _wipe(client)  # guarantee a clean bucket regardless of server reuse
    _run_dataflows(steps)
    df = _snapshot(client)
    _wipe(client)

    _run_duckdb(steps)
    dk = _snapshot(client)

    assert set(df) == set(dk), f"object keys differ: dataflows={sorted(df)} duckdb={sorted(dk)}"
    for key in sorted(df):
        assert df[key] == dk[key], (
            f"object {key!r} differs: dataflows={df[key][:300]!r} duckdb={dk[key][:300]!r}"
        )
    return df


def _load(**params):
    return {"run": "bcodmo_pipeline_processors.load", "parameters": params}


def _dump(**params):
    base = {
        "prefix": PREFIX, "force-format": True, "format": "csv",
        "temporal_format_property": "outputFormat",
        "bucket_name": DUMP_BUCKET, "data_manager": "dm",
    }
    base.update(params)
    return {"run": "bcodmo_pipeline_processors.dump_to_s3", "parameters": base}


def test_dump_csv(s3_server):
    art = _assert_dump_identical(s3_server, [
        _load(**{"from": SRC_CSV, "name": "res", "format": "csv"}),
        _dump(),
    ])
    assert f"{PREFIX}/res.csv" in art          # the CSV really landed on S3
    assert f"{PREFIX}/datapackage.json" in art


def test_dump_after_set_types(s3_server):
    # A schema change must flow into both the CSV cast and the datapackage.json.
    _assert_dump_identical(s3_server, [
        _load(**{"from": SRC_CSV, "name": "res", "format": "csv"}),
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"types": {"col2": {"type": "integer"}, "col3": {"type": "number"}}}},
        _dump(),
    ])


def test_dump_after_update_package(s3_server):
    # update_package's top-level metadata must appear in datapackage.json identically.
    _assert_dump_identical(s3_server, [
        _load(**{"from": SRC_CSV, "name": "res", "format": "csv"}),
        {"run": "update_package",
         "parameters": {"title": "My Package", "name": "pkg-name", "custom_key": "v"}},
        _dump(),
    ])


def test_dump_after_update_resource(s3_server):
    # update_resource's resource-level metadata (incl. a rename) must appear in
    # datapackage.json and drive the CSV object key identically in both lanes.
    art = _assert_dump_identical(s3_server, [
        _load(**{"from": SRC_CSV, "name": "res", "format": "csv"}),
        {"run": "update_resource",
         "parameters": {"resources": "res",
                        "metadata": {"name": "renamed", "title": "My Resource",
                                     "bcodmo:extra": "rv"}}},
        _dump(),
    ])
    # The rename changes the resource's datapackage NAME (not its file path, which
    # load fixed to res.csv); the merged metadata appears in datapackage.json.
    assert f"{PREFIX}/res.csv" in art
    dp = art[f"{PREFIX}/datapackage.json"]
    assert b'"renamed"' in dp and b'"My Resource"' in dp and b'"bcodmo:extra"' in dp


def test_dump_multiple_resources(s3_server):
    a = os.path.join(_HERE, "data_load", "a.csv")
    b = os.path.join(_HERE, "data_load", "b.csv")
    art = _assert_dump_identical(s3_server, [
        _load(**{"from": f"{a},{b}", "name": "res", "format": "csv", "input_separator": ","}),
        _dump(),
    ])
    assert f"{PREFIX}/res-1.csv" in art and f"{PREFIX}/res-2.csv" in art


def test_dump_save_pipeline_spec(s3_server):
    spec = "processors:\n  - run: bcodmo_pipeline_processors.load\n"
    art = _assert_dump_identical(s3_server, [
        _load(**{"from": SRC_CSV, "name": "res", "format": "csv"}),
        _dump(save_pipeline_spec=True, pipeline_spec=spec),
    ])
    assert f"{PREFIX}/pipeline-spec.yaml" in art
    assert art[f"{PREFIX}/pipeline-spec.yaml"] == spec.encode("utf-8")


# NOTE: ``use_titles=True`` is NOT tested because it is a latent prod bug --
# ``S3Dumper``/``CustomCSVFormat`` reference an undefined ``CsvTitlesDictWriter``
# and raise ``NameError`` in BOTH lanes (same dead-code class as join's
# ``load_lazy_json`` / standard_flows' ``_set_type``). The UI leaves use_titles
# unset (see dump.py), so the byte-exact path is the one exercised above.
