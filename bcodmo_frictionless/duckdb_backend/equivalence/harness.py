"""Differential comparison helpers (test layer A / B)."""

from decimal import Decimal


def norm(v):
    """Normalize a cell for cross-engine comparison (Decimal/float/None/str)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, Decimal)):
        return round(float(v), 9)
    return v


def column(rows, key):
    return [norm(r.get(key)) for r in rows]


def diff_columns(expected, actual):
    """Return list of (index, expected, actual) mismatches."""
    return [(i, e, a) for i, (e, a) in enumerate(zip(expected, actual)) if e != a]
