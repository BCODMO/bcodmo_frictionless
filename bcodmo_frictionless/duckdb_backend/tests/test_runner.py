"""
Tests for the pipeline runner -- the engine-side half of the laminar_server router
seam (``duckdb_backend.runner``). Covers the support gate, end-to-end execution of
a load -> transform -> dump pipeline (against a real moto endpoint, since the dump's
billiard upload needs one), the streamed UI sample, and step-error attribution.
"""

import os

import boto3
import pytest
from moto.server import ThreadedMotoServer

from bcodmo_frictionless.duckdb_backend import runner

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))
DATA = os.path.join(_REPO, "data")
SRC_CSV = os.path.join(DATA, "test.csv")
BUCKET = "results_bucket"


# -- support gate (pure) ---------------------------------------------------

def test_is_supported_accepts_known_steps():
    steps = [
        {"run": "bcodmo_pipeline_processors.load", "parameters": {}},
        {"run": "bcodmo_pipeline_processors.set_types", "parameters": {}},
        {"run": "join", "parameters": {}},
    ]
    assert runner.is_supported(steps) is True


def test_is_supported_rejects_unknown_run():
    assert runner.is_supported([{"run": "totally_made_up", "parameters": {}}]) is False


def test_is_supported_rejects_checkpoint():
    # checkpoint is a dataflows/EFS feature the engine doesn't implement.
    assert runner.is_supported(
        [{"run": "join", "parameters": {}, "checkpoint": True}]
    ) is False


def test_is_supported_rejects_non_dict():
    assert runner.is_supported(["not a step"]) is False


def test_unsupported_reasons_empty_when_supported():
    steps = [
        {"run": "bcodmo_pipeline_processors.load", "parameters": {}},
        {"run": "sort", "parameters": {}},
    ]
    assert runner.unsupported_reasons(steps) == []


def test_unsupported_reasons_names_offending_steps():
    # Two disqualifying steps -> two reasons, each naming the 1-based step number.
    steps = [
        {"run": "sort", "parameters": {}},          # ok
        {"run": "totally_made_up", "parameters": {}},  # unknown run
        {"run": "join", "parameters": {}, "checkpoint": True},  # dataflows-only
    ]
    reasons = runner.unsupported_reasons(steps)
    assert len(reasons) == 2
    assert "step 2 (totally_made_up)" in reasons[0]
    assert "step 3 (join)" in reasons[1] and "checkpoint" in reasons[1]


# -- execution -------------------------------------------------------------

@pytest.fixture
def s3_server():
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
    client.create_bucket(Bucket=BUCKET)
    try:
        yield client
    finally:
        server.stop()
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_execute_runs_load_transform_dump(s3_server, tmp_path):
    steps = [
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": SRC_CSV, "name": "res", "format": "csv"}},
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"types": {"col2": {"type": "integer"}}}},
        {"run": "bcodmo_pipeline_processors.dump_to_s3",
         "parameters": {
             "prefix": "cache123", "force-format": True, "format": "csv",
             "temporal_format_property": "outputFormat", "bucket_name": BUCKET,
             "data_manager": {"name": "", "orcid": ""},
         }},
    ]
    eng = runner.execute(steps, memory_limit="256MB", temp_directory=str(tmp_path))

    # engine ran the transform: col2 is now integer-typed
    types = {f["name"]: f["type"] for f in eng.resources["res"].schema}
    assert types["col2"] == "integer"

    # streamed sample (typed rows)
    sample = list(runner.iter_sample(eng, "res", 25))
    assert len(sample) == 4
    assert sample[0]["col2"] == 1  # cast to int

    # UI sample: [header, *rows] per resource, Decimal->float coercion applied
    ui = runner.build_sample(eng)
    assert len(ui) == 1  # one resource
    assert ui[0][0] == ["col1", "col2", "col3", "col4"]  # header
    assert ui[0][1] == ["abc", 1, "1.532", "12/29/19"]  # first row, col2 int
    assert len(ui[0]) == 1 + 4  # header + 4 rows

    # dump artifacts landed in the results bucket
    keys = {o["Key"] for o in s3_server.list_objects_v2(Bucket=BUCKET).get("Contents", [])}
    assert "cache123/datapackage.json" in keys
    assert "cache123/res.csv" in keys


def test_execute_attributes_step_error(tmp_path):
    steps = [
        {"run": "bcodmo_pipeline_processors.load",
         "parameters": {"from": SRC_CSV, "name": "res", "format": "csv"}},
        # set_types on a field that doesn't exist -> raises inside step index 1
        {"run": "bcodmo_pipeline_processors.set_types",
         "parameters": {"types": {"nonexistent_field": {"type": "integer"}}}},
    ]
    with pytest.raises(runner.StepError) as ei:
        runner.execute(steps, temp_directory=str(tmp_path))
    assert ei.value.index == 1
    assert ei.value.run == "bcodmo_pipeline_processors.set_types"
