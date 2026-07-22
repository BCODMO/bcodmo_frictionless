"""
End-to-end differential harness (test layer C): run the SAME pipeline spec
through both lanes and compare typed output.

  * dataflows lane  -> ``Flow(data, *steps).results()`` (the live processors)
  * DuckDB lane     -> ``Engine.ingest_rows -> run(steps) -> typed_rows`` (deferred
                       cast materialized at output via the shared casting module)

Equal typed rows + equal final schema == the two engines agree end to end,
including the deferred-cast model (set_types casts at dump-time in the DuckDB lane
but inline in the dataflows lane, yet the output values match).

Both lanes start from the same all-string source, mirroring bcodmo load's
``cast_strategy=strings`` (and the all-VARCHAR ingest).
"""

import copy
import importlib

from dataflows import Flow, delete_fields as _df_delete_fields, update_resource

from ..engine import Engine
from . import harness


def _dataflows_step(step):
    run = step["run"]
    params = step.get("parameters", {})
    if run == "delete_fields":
        return _df_delete_fields(params.get("fields", []), regex=params.get("regex", True))
    if run == "join":
        # Bare/standard run-name: the dataflows reference is the STANDARD
        # ``dataflows.join`` primitive, mirroring ``standard_flows.join`` (minus
        # the load_lazy_json no-op on in-memory rows and the dpp:streaming prop,
        # neither of which affects compared fields/rows). source_delete defaults
        # to False, exactly as standard_flows.join passes it.
        from dataflows import join as _df_join

        src = params["source"]
        tgt = params["target"]
        return _df_join(
            src["name"],
            src["key"],
            tgt["name"],
            tgt["key"],
            copy.deepcopy(params.get("fields", {})),
            params.get("full", None),
            params.get("mode", "half-outer"),
            src.get("delete", False),
        )
    if run == "sort":
        # standard_flows.sort wraps load_lazy_json (no-op on in-memory rows).
        from dataflows import sort_rows as _df_sort_rows

        return _df_sort_rows(
            params["sort-by"],
            resources=params.get("resources"),
            reverse=params.get("reverse") or False,
        )
    if run == "unpivot":
        from dataflows import unpivot as _df_unpivot

        return _df_unpivot(
            params.get("unpivot"),
            params.get("extraKeyFields"),
            params.get("extraValueField"),
            resources=params.get("resources"),
        )
    if run == "duplicate":
        # standard_flows.duplicate wraps load_lazy_json (no-op on in-memory rows).
        from dataflows import duplicate as _df_duplicate

        return _df_duplicate(
            params.get("source"),
            params.get("target-name"),
            params.get("target-path"),
            params.get("batch_size", 1000),
            params.get("duplicate_to_end", False),
        )
    if run == "add_computed_field":
        from dataflows import add_computed_field as _df_add_computed_field

        return _df_add_computed_field(
            params.get("fields", []), resources=params.get("resources")
        )
    if run == "update_package":
        # standard_flows.update_package = Flow(update_package(**parameters)); the
        # live primitive drops the 'resources' key and merges the rest package-level.
        from dataflows import update_package as _df_update_package

        return _df_update_package(**copy.deepcopy(params))
    if run == "update_resource":
        # standard_flows.update_resource = update_resource(params['resources'],
        # **params['metadata']).
        from dataflows import update_resource as _df_update_resource

        return _df_update_resource(
            params.get("resources"), **copy.deepcopy(params.get("metadata", {}) or {})
        )
    if run == "set_types":
        # Bare/standard set_types. Faithful mirror of standard_flows.set_types
        # (built directly from the dataflows `set_type` primitive it wraps, since
        # standard_flows itself can't be imported in the test env -- it pulls in
        # AWS Secrets Manager on import). Per name -> options: cast the field;
        # options None -> delete it. Note the delete branch mirrors standard_flows
        # exactly: it passes `resources` and uses delete_fields' default regex
        # (True), NOT the caller's `regex` flag.
        from dataflows import Flow, validate, set_type as _df_set_type

        regex = params.get("regex", True)
        resources = params.get("resources")
        if "types" not in params:
            return Flow(validate())
        substeps = []
        for name, options in params["types"].items():
            if options is not None:
                substeps.append(_df_set_type(name, resources=resources, regex=regex, **options))
            else:
                substeps.append(_df_delete_fields([name], resources=resources))
        return Flow(*substeps)
    if run.startswith("bcodmo_pipeline_processors."):
        mod = importlib.import_module(
            "bcodmo_frictionless.bcodmo_pipeline_processors." + run.split(".", 1)[1]
        )
        return mod.flow(params)
    raise KeyError(f"differential harness has no dataflows mapping for run={run!r}")


def run_dataflows(data, steps):
    """Returns (rows, schema_fields) for the single resource.

    Steps are deep-copied so a processor whose ``flow`` mutates its parameters
    (e.g. ``update_fields`` pops ``fields``; ``edit_cells`` drains ``edited`` via
    ``.pop()``) cannot leak that mutation into the caller's ``steps`` or the other
    lane -- the two lanes MUST each run from pristine params."""
    steps = copy.deepcopy(steps)
    rows, dp, _ = Flow(data, *[_dataflows_step(s) for s in steps]).results()
    schema = dp.descriptor["resources"][0]["schema"]["fields"]
    return rows[0], schema


def run_duckdb(data, string_schema, steps):
    """Returns (typed_rows, schema_fields) for the single resource. Steps are
    deep-copied for the same reason as ``run_dataflows`` (param isolation)."""
    import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)

    steps = copy.deepcopy(steps)
    eng = Engine()
    eng.ingest_rows("res", data, string_schema)
    eng.run(steps)
    name = next(iter(eng.resources))
    return eng.typed_rows(name), eng.resources[name].schema


def assert_equivalent(data, string_schema, steps):
    """Assert the two lanes produce equal typed rows + equal final field
    names/types. Raises AssertionError with a precise diff on mismatch."""
    df_rows, df_schema = run_dataflows(data, steps)
    dk_rows, dk_schema = run_duckdb(data, string_schema, steps)

    df_names = [f["name"] for f in df_schema]
    dk_names = [f["name"] for f in dk_schema]
    assert df_names == dk_names, f"field order/names differ: dataflows={df_names} duckdb={dk_names}"

    df_types = {f["name"]: f.get("type") for f in df_schema}
    dk_types = {f["name"]: f.get("type") for f in dk_schema}
    assert df_types == dk_types, f"field types differ: dataflows={df_types} duckdb={dk_types}"

    assert len(df_rows) == len(dk_rows), (
        f"row count differs: dataflows={len(df_rows)} duckdb={len(dk_rows)}"
    )

    for name in df_names:
        d = harness.column(df_rows, name)
        k = harness.column(dk_rows, name)
        diffs = harness.diff_columns(d, k)
        assert diffs == [], f"column {name!r} differs (idx, dataflows, duckdb): {diffs[:5]}"
    return df_rows, dk_rows


def assert_csv_identical(data, string_schema, steps):
    """Stronger than ``assert_equivalent``: serialize BOTH lanes' output through
    the production ``CustomCSVFormat`` and assert byte-identical CSV. This catches
    representation differences that typed-value comparison rounds away (e.g.
    ``Decimal('1.50')`` vs ``Decimal('1.5')`` -> different bytes)."""
    from ..dump import to_csv_bytes

    df_rows, df_schema = run_dataflows(data, steps)
    dk_rows, dk_schema = run_duckdb(data, string_schema, steps)

    df_bytes = to_csv_bytes(df_schema, df_rows)
    dk_bytes = to_csv_bytes(dk_schema, dk_rows)
    if df_bytes != dk_bytes:
        df_lines = df_bytes.decode().splitlines()
        dk_lines = dk_bytes.decode().splitlines()
        first = next(
            (i for i, (a, b) in enumerate(zip(df_lines, dk_lines)) if a != b), None
        )
        detail = (
            f"line {first}: dataflows={df_lines[first]!r} duckdb={dk_lines[first]!r}"
            if first is not None
            else f"line count: dataflows={len(df_lines)} duckdb={len(dk_lines)}"
        )
        raise AssertionError(f"CSV bytes differ; {detail}")
    return df_bytes


# --------------------------------------------------------------------------
# Multi-resource harness (structural processors: remove_resources,
# rename_resource, concatenate, join). These reference resources BY NAME, so
# both lanes must ingest named resources.
#
#   resources: an ordered dict {resource_name: [row dicts]} of all-string data.
#   string_schemas: {resource_name: [field dicts]} the all-VARCHAR ingest schema.
#
# Returns a dict {resource_name: (rows, schema_fields)} from each lane.
# --------------------------------------------------------------------------

def run_dataflows_multi(resources, steps):
    steps = copy.deepcopy(steps)
    flow_args = []
    # dataflows names raw in-memory resources res_1, res_2, ... in order; rename
    # each to the caller's chosen name so by-name processors resolve correctly.
    for i, (name, data) in enumerate(resources.items()):
        flow_args.append(list(data))
        flow_args.append(update_resource(f"res_{i + 1}", name=name))
    flow_args += [_dataflows_step(s) for s in steps]
    rows, dp, _ = Flow(*flow_args).results()
    out = {}
    for idx, res in enumerate(dp.descriptor["resources"]):
        out[res["name"]] = (rows[idx], res["schema"]["fields"])
    return out


def run_duckdb_multi(resources, string_schemas, steps):
    import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)

    steps = copy.deepcopy(steps)
    eng = Engine()
    for name, data in resources.items():
        eng.ingest_rows(name, list(data), string_schemas[name])
    eng.run(steps)
    return {
        name: (eng.typed_rows(name), eng.resources[name].schema)
        for name in eng.resources
    }


def assert_equivalent_multi(resources, string_schemas, steps):
    """Assert BOTH lanes produce the same set of resources, each with equal field
    names/types and equal typed rows (order-sensitive within a resource)."""
    df = run_dataflows_multi(resources, steps)
    dk = run_duckdb_multi(resources, string_schemas, steps)

    assert set(df) == set(dk), (
        f"resource set differs: dataflows={sorted(df)} duckdb={sorted(dk)}"
    )
    for name in df:
        df_rows, df_schema = df[name]
        dk_rows, dk_schema = dk[name]
        df_names = [f["name"] for f in df_schema]
        dk_names = [f["name"] for f in dk_schema]
        assert df_names == dk_names, (
            f"[{name}] field names differ: dataflows={df_names} duckdb={dk_names}"
        )
        df_types = {f["name"]: f.get("type") for f in df_schema}
        dk_types = {f["name"]: f.get("type") for f in dk_schema}
        assert df_types == dk_types, (
            f"[{name}] field types differ: dataflows={df_types} duckdb={dk_types}"
        )
        assert len(df_rows) == len(dk_rows), (
            f"[{name}] row count differs: dataflows={len(df_rows)} duckdb={len(dk_rows)}"
        )
        for fname in df_names:
            d = harness.column(df_rows, fname)
            k = harness.column(dk_rows, fname)
            diffs = harness.diff_columns(d, k)
            assert diffs == [], f"[{name}] column {fname!r} differs: {diffs[:5]}"
    return df, dk


def assert_csv_identical_multi(resources, string_schemas, steps):
    """Byte-identical CSV per resource across both lanes."""
    from ..dump import to_csv_bytes

    df = run_dataflows_multi(resources, steps)
    dk = run_duckdb_multi(resources, string_schemas, steps)
    assert set(df) == set(dk), (
        f"resource set differs: dataflows={sorted(df)} duckdb={sorted(dk)}"
    )
    out = {}
    for name in df:
        df_rows, df_schema = df[name]
        dk_rows, dk_schema = dk[name]
        df_bytes = to_csv_bytes(df_schema, df_rows)
        dk_bytes = to_csv_bytes(dk_schema, dk_rows)
        if df_bytes != dk_bytes:
            df_lines = df_bytes.decode().splitlines()
            dk_lines = dk_bytes.decode().splitlines()
            first = next(
                (i for i, (a, b) in enumerate(zip(df_lines, dk_lines)) if a != b), None
            )
            detail = (
                f"line {first}: dataflows={df_lines[first]!r} duckdb={dk_lines[first]!r}"
                if first is not None
                else f"line count: dataflows={len(df_lines)} duckdb={len(dk_lines)}"
            )
            raise AssertionError(f"[{name}] CSV bytes differ; {detail}")
        out[name] = df_bytes
    return out
