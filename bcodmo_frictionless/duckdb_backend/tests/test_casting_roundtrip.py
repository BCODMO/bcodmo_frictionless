"""
Round-trip test for the UDF format-out boundary.

A UDF processor outputs TYPED values (datetime/date/time/Decimal). The DuckDB lane
stores VARCHAR, so those must serialize to strings that re-cast to the identical
typed value at the dump/next-step boundary. This proves:

    cast_rows(format_out_rows(typed_rows, fields), fields) == typed_rows

for temporal fields with explicit strftime formats (the convert_date / set_types
case) plus number/integer/string, and that the final CSV serialization is
byte-stable through the round-trip.
"""

import datetime
from decimal import Decimal

from bcodmo_frictionless.duckdb_backend.casting import cast_rows, format_out_rows
from bcodmo_frictionless.duckdb_backend.dump import to_csv_bytes

FIELDS = [
    {"name": "dt", "type": "datetime", "format": "%Y-%m-%dT%H:%M:%S", "outputFormat": "%Y-%m-%dT%H:%M:%S"},
    {"name": "d", "type": "date", "format": "%m/%d/%Y", "outputFormat": "%m/%d/%Y"},
    {"name": "t", "type": "time", "format": "%H:%M:%S", "outputFormat": "%H:%M:%S"},
    {"name": "depth", "type": "number"},
    {"name": "n", "type": "integer"},
    {"name": "label", "type": "string"},
]
TYPED_ROWS = [
    {"dt": datetime.datetime(2020, 1, 15, 3, 4, 5), "d": datetime.date(2020, 1, 15),
     "t": datetime.time(3, 4, 5), "depth": Decimal("10.5"), "n": 3, "label": "a"},
    {"dt": datetime.datetime(1999, 12, 31, 23, 59, 59), "d": datetime.date(1999, 6, 1),
     "t": datetime.time(23, 59, 59), "depth": Decimal("-180"), "n": 0, "label": "b"},
    {"dt": None, "d": None, "t": None, "depth": None, "n": None, "label": None},
]


def test_format_out_is_inverse_of_cast():
    # format-out (typed -> VARCHAR-safe strings)
    serialized = format_out_rows([dict(r) for r in TYPED_ROWS], FIELDS)
    # temporals are now strings in the field's format
    assert serialized[0]["dt"] == "2020-01-15T03:04:05"
    assert serialized[0]["d"] == "01/15/2020"
    assert serialized[0]["t"] == "03:04:05"

    # cast-in (VARCHAR -> typed) must reproduce the ORIGINAL typed values
    recast = list(cast_rows([dict(r) for r in serialized], FIELDS))
    for i, (orig, got) in enumerate(zip(TYPED_ROWS, recast)):
        for k in ("dt", "d", "t", "depth", "n", "label"):
            assert got.get(k) == orig.get(k), f"row {i} col {k}: {got.get(k)!r} != {orig.get(k)!r}"


def test_csv_bytes_stable_through_roundtrip():
    # Serializing the original typed rows vs the round-tripped rows -> same bytes.
    direct = to_csv_bytes(FIELDS, [dict(r) for r in TYPED_ROWS])
    roundtripped = to_csv_bytes(
        FIELDS, list(cast_rows(format_out_rows([dict(r) for r in TYPED_ROWS], FIELDS), FIELDS))
    )
    assert direct == roundtripped, (direct, roundtripped)


if __name__ == "__main__":
    test_format_out_is_inverse_of_cast()
    test_csv_bytes_stable_through_roundtrip()
    print("format_out_rows is the exact inverse of cast_rows (temporal + numeric)  ✓")
