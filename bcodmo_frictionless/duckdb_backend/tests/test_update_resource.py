"""
Differential tests for update_resource (STANDARD dataflows processor -- bare name).

update_resource merges ``metadata`` into matched resources' descriptors. Most
effects (title / bcodmo: keys) surface only in a dumped datapackage.json (covered
in test_dump_to_s3), but a ``name`` in the metadata RENAMES the resource, which the
row/schema differential observes directly. Metadata-only merges must be row/schema
no-ops in both lanes.
"""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent_multi,
    assert_csv_identical_multi,
)

_SCHEMAS = {
    "source": [{"name": "id", "type": "string"}, {"name": "v", "type": "string"}],
    "target": [{"name": "id", "type": "string"}],
}
_RESOURCES = {
    "source": [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}],
    "target": [{"id": "9"}],
}


def test_update_resource_rename():
    steps = [{"run": "update_resource",
              "parameters": {"resources": "source", "metadata": {"name": "renamed"}}}]
    _, dk = assert_equivalent_multi(_RESOURCES, _SCHEMAS, steps)
    assert set(dk) == {"renamed", "target"}
    assert_csv_identical_multi(_RESOURCES, _SCHEMAS, steps)


def test_update_resource_metadata_is_row_schema_noop():
    # Attaching resource metadata must not touch rows/fields in either lane.
    steps = [{"run": "update_resource",
              "parameters": {"resources": "source",
                             "metadata": {"title": "My Source", "bcodmo:x": "y"}}}]
    _, dk = assert_equivalent_multi(_RESOURCES, _SCHEMAS, steps)
    assert set(dk) == {"source", "target"}
    assert_csv_identical_multi(_RESOURCES, _SCHEMAS, steps)


def test_update_resource_rename_all_via_none():
    # resources=None matches all; renaming all collapses them (last wins), mirroring
    # the descriptor collision the live primitive would produce -- just assert both
    # lanes agree on the resulting resource set.
    steps = [{"run": "update_resource",
              "parameters": {"resources": None, "metadata": {"title": "T"}}}]
    df, dk = assert_equivalent_multi(_RESOURCES, _SCHEMAS, steps)
    assert set(df) == set(dk) == {"source", "target"}
