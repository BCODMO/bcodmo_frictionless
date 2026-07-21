"""
Differential test (layer C) for convert_to_decimal_degrees.

The coordinate ``input_field`` is a STRING (e.g. ``"14° 24.0' N"``) that the live
row logic regex-parses -- so there is NO ``set_types`` -> number step before this
processor; the OUTPUT field is a ``number``. The conversion is ``Decimal``
coordinate arithmetic (minutes/seconds division, directional sign), not byte-exact
in SQL, so there is no ``to_sql``: the DuckDB lane runs the live
``process_resource`` as a UDF. ``assert_csv_identical`` proves both lanes emit
byte-identical CSV.

Covers: degrees-decimal_minutes (replace-in-place and new-field), the directional
sign (N/E positive, S/W negative), degrees-minutes-seconds, a fixed ``directional``
override, ``handle_out_of_bounds`` wrapping, an empty/missing input row, and
``boolean_statement`` gating.
"""

from bcodmo_frictionless.duckdb_backend.processors import (  # noqa: F401  (register)
    convert_to_decimal_degrees,
)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
)

DDM_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "lat", "type": "string"},
    {"name": "lon", "type": "string"},
]
DDM_DATA = [
    {"id": "1", "lat": "14° 24.0' N", "lon": "70° 30.0' W"},
    {"id": "2", "lat": "12° 1' S", "lon": "120° 15.5' E"},
    {"id": "3", "lat": "", "lon": ""},
]

DDM_PATTERN = r"(?P<degrees>\d+)° *(?P<decimal_minutes>\d+\.*\d*)' *(?P<directional>\w).*"

DMS_SCHEMA = [
    {"name": "id", "type": "string"},
    {"name": "lat", "type": "string"},
]
DMS_DATA = [
    {"id": "1", "lat": "14° 24' 30\" N"},
    {"id": "2", "lat": "12° 1' 15\" S"},
    {"id": "3", "lat": ""},
]
DMS_PATTERN = (
    r"(?P<degrees>\d+)° *(?P<minutes>\d+)' *(?P<seconds>\d+\.*\d*)\" *(?P<directional>\w).*"
)


def test_ddm_new_fields():
    # Two coordinates parsed into NEW number fields; S/W go negative.
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {"fields": [
            {"format": "degrees-decimal_minutes", "input_field": "lat",
             "output_field": "lat_dd", "pattern": DDM_PATTERN, "directional": ""},
            {"format": "degrees-decimal_minutes", "input_field": "lon",
             "output_field": "lon_dd", "pattern": DDM_PATTERN, "directional": ""},
        ]}},
    ]
    assert_equivalent(DDM_DATA, DDM_SCHEMA, steps)
    assert_csv_identical(DDM_DATA, DDM_SCHEMA, steps)


def test_ddm_replace_in_place():
    # output_field == input_field: the string column is replaced by a number
    # column of the same name, in place (order preserved).
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {"fields": [
            {"format": "degrees-decimal_minutes", "input_field": "lat",
             "output_field": "lat", "pattern": DDM_PATTERN, "directional": ""},
        ]}},
    ]
    assert_equivalent(DDM_DATA, DDM_SCHEMA, steps)
    assert_csv_identical(DDM_DATA, DDM_SCHEMA, steps)


def test_ddm_fixed_directional():
    # A fixed ``directional`` overrides the matched group -> forces the sign.
    pattern = r"(?P<degrees>\d+)° *(?P<decimal_minutes>\d+\.*\d*)' *(?P<directional>\w).*"
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {"fields": [
            {"format": "degrees-decimal_minutes", "input_field": "lat",
             "output_field": "lat_dd", "pattern": pattern, "directional": "S"},
        ]}},
    ]
    assert_equivalent(DDM_DATA, DDM_SCHEMA, steps)
    assert_csv_identical(DDM_DATA, DDM_SCHEMA, steps)


def test_dms():
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {"fields": [
            {"format": "degrees-minutes-seconds", "input_field": "lat",
             "output_field": "lat_dd", "pattern": DMS_PATTERN, "directional": ""},
        ]}},
    ]
    assert_equivalent(DMS_DATA, DMS_SCHEMA, steps)
    assert_csv_identical(DMS_DATA, DMS_SCHEMA, steps)


def test_dms_handle_out_of_bounds():
    # seconds >= 60 wrapped into minutes when handle_out_of_bounds is set.
    data = [{"id": "1", "lat": "14° 24' 75\" N"}]
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {"fields": [
            {"format": "degrees-minutes-seconds", "input_field": "lat",
             "output_field": "lat_dd", "pattern": DMS_PATTERN, "directional": "",
             "handle_out_of_bounds": True},
        ]}},
    ]
    assert_equivalent(data, DMS_SCHEMA, steps)
    assert_csv_identical(data, DMS_SCHEMA, steps)


def test_boolean_statement_gating():
    # Only rows where {id} == '1' are converted; others get None.
    steps = [
        {"run": "bcodmo_pipeline_processors.convert_to_decimal_degrees", "parameters": {
            "boolean_statement": "{id} == '1'",
            "fields": [
                {"format": "degrees-decimal_minutes", "input_field": "lat",
                 "output_field": "lat_dd", "pattern": DDM_PATTERN, "directional": ""},
            ]}},
    ]
    assert_equivalent(DDM_DATA, DDM_SCHEMA, steps)
    assert_csv_identical(DDM_DATA, DDM_SCHEMA, steps)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name}  ✓")
