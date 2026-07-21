"""
End-to-end differential test (layer C): a realistic multi-step pipeline run
through the dataflows lane and the DuckDB Engine must produce identical typed
output + schema.

Exercises together: set_types (deferred cast) -> boolean_filter_rows (numeric DSL
on cast values, evaluated as TRY_CAST in SQL vs Decimal in dataflows) ->
boolean_add_computed_field (regex + math-value DSL) -> delete_fields (EXCLUDE) ->
rename_fields (RENAME) -> reorder_fields (schema projection).

NOTE (latent live bug): the live bcodmo ``boolean_add_computed_field.process_rows``
mis-indexes ``value_functions`` when a NON-math field precedes a MATH field in the
SAME step (``value_functions.append(None)`` should be
``value_functions[index].append(None)``) -> crashes with "'NoneType' has no
attribute 'append'". Real pipelines use one field per step so it never fires; the
DuckDB ``to_sql`` handles multi-field correctly. We keep one computed field per
step here to match real usage (and avoid tripping the live bug).
"""

from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

# all-string source (as after an all-VARCHAR ingest / bcodmo load)
STRING_SCHEMA = [
    {"name": "Cruise_num", "type": "string"},
    {"name": "depth", "type": "string"},
    {"name": "lon", "type": "string"},
    {"name": "note", "type": "string"},
]
DATA = [
    {"Cruise_num": "10001", "depth": "10.5", "lon": "64.5", "note": "a"},
    {"Cruise_num": "20002", "depth": "-3.25", "lon": "0", "note": "b"},
    {"Cruise_num": "30003", "depth": "0", "lon": "180", "note": "c"},
    {"Cruise_num": "10004", "depth": "5", "lon": "-12.5", "note": "d"},
    {"Cruise_num": "20005", "depth": "-1", "lon": "90", "note": "e"},
]

STEPS = [
    {"run": "bcodmo_pipeline_processors.set_types",
     "parameters": {"regex": False, "types": {
         "depth": {"type": "number"},
         "lon": {"type": "number"},
     }}},
    # numeric filter on a cast field: SQL TRY_CAST vs dataflows Decimal
    {"run": "bcodmo_pipeline_processors.boolean_filter_rows",
     "parameters": {"boolean_statement": "{depth} >= 0", "missing_values": []}},
    # regex classification (one field per step == real-world usage; the live
    # multi-field code path has a latent index bug, see module docstring)
    {"run": "bcodmo_pipeline_processors.boolean_add_computed_field",
     "parameters": {"missing_values": [], "fields": [
         {"target": "Cruise_type", "type": "string", "functions": [
             {"boolean": "{Cruise_num} == re'^1.*'", "value": "Core", "math_operation": False},
             {"boolean": "{Cruise_num} == re'^2.*'", "value": "Bloom", "math_operation": False},
         ]},
     ]}},
    # math-value computed field (separate step)
    {"run": "bcodmo_pipeline_processors.boolean_add_computed_field",
     "parameters": {"missing_values": [], "fields": [
         {"target": "lon_west", "type": "number", "functions": [
             {"boolean": "{lon} >= 0", "value": "{lon}*(-1)", "math_operation": True},
         ]},
     ]}},
    {"run": "delete_fields",
     "parameters": {"regex": False, "fields": ["note"]}},
    {"run": "bcodmo_pipeline_processors.rename_fields",
     "parameters": {"fields": [{"old_field": "depth", "new_field": "depth_m"}]}},
    {"run": "bcodmo_pipeline_processors.reorder_fields",
     "parameters": {"fields": ["Cruise_num", "Cruise_type", "depth_m", "lon", "lon_west"]}},
]


def test_multistep_pipeline_matches_dataflows():
    df_rows, dk_rows = assert_equivalent(DATA, STRING_SCHEMA, STEPS)
    # sanity: the filter dropped the 2 negative-depth rows
    assert len(df_rows) == 3, [r.get("depth_m") for r in df_rows]


def test_multistep_pipeline_byte_identical_csv():
    csv_bytes = assert_csv_identical(DATA, STRING_SCHEMA, STEPS)
    # sanity: header + 3 data rows
    assert csv_bytes.decode().count("\r\n") == 4, csv_bytes


if __name__ == "__main__":
    test_multistep_pipeline_matches_dataflows()
    print("differential: pipeline  dataflows == duckdb (typed rows + schema)  ✓")
    test_multistep_pipeline_byte_identical_csv()
    print("differential: pipeline  dataflows == duckdb (byte-identical CSV)  ✓")
