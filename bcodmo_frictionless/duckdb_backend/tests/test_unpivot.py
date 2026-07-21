"""Differential tests for unpivot (bare/standard run-name; wide -> long reshape)."""

import bcodmo_frictionless.duckdb_backend  # noqa: F401  (register processors)
from bcodmo_frictionless.duckdb_backend.equivalence.differential import (
    assert_equivalent,
    assert_csv_identical,
    run_duckdb,
)

STRING_SCHEMA = [
    {"name": "country", "type": "string"},
    {"name": "y2020", "type": "string"},
    {"name": "y2021", "type": "string"},
]


def _steps():
    return [{
        "run": "unpivot",
        "parameters": {
            "unpivot": [{"name": "y([0-9]+)", "keys": {"year": "\\1"}}],
            "extraKeyFields": [{"name": "year", "type": "string"}],
            "extraValueField": {"name": "value", "type": "string"},
        },
    }]


def test_wide_to_long():
    data = [
        {"country": "US", "y2020": "10", "y2021": "20"},
        {"country": "CA", "y2020": "5", "y2021": "6"},
    ]
    steps = _steps()
    df_rows, dk_rows = assert_equivalent(data, STRING_SCHEMA, steps)
    _, dk_schema = run_duckdb(data, STRING_SCHEMA, steps)
    assert [f["name"] for f in dk_schema] == ["country", "year", "value"]
    assert len(dk_rows) == 4  # 2 rows x 2 unpivoted columns
    # regex substitution derives the year value from the column name
    assert {(r["country"], r["year"], r["value"]) for r in dk_rows} == {
        ("US", "2020", "10"), ("US", "2021", "20"),
        ("CA", "2020", "5"), ("CA", "2021", "6"),
    }
    assert_csv_identical(data, STRING_SCHEMA, steps)


if __name__ == "__main__":
    test_wide_to_long()
    print("unpivot: dataflows == duckdb  ✓")
