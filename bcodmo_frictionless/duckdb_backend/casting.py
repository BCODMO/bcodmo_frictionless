"""
The single casting source.

The DuckDB lane stores all data as VARCHAR (see PLAN.md §storage) and defers
type casting to two boundaries: the UDF fallback (so a processor's
``process_rows`` sees the SAME typed values the dataflows lane feeds it) and the
dump (so output bytes match). Both delegate here.

We do NOT reimplement casting. ``cast_rows`` runs the frictionless field cast via
dataflows' own ``schema_validator`` -- byte-for-byte the same code the live
``set_types`` uses -- so the DuckDB lane can never diverge from the dataflows lane
on casting semantics (datetime formats, decimalChar, booleans, missingValues, ...).
"""

import datetime as _dt

from dataflows import schema_validator


_TEMPORAL = ("datetime", "date", "time")


def _storage_format(field):
    """The strftime format that governs a temporal field's VARCHAR round-trip in
    the DuckDB lane (format-out writes it, cast-in parses it).

    A field created by ``convert_date`` (and similar temporal-producing
    processors) carries only ``outputFormat`` -- no cast ``format`` -- because
    the dataflows lane keeps a live ``datetime`` object in memory and never
    reparses. The DuckDB lane stores VARCHAR, so it MUST round-trip through some
    explicit format. We use ``format`` when present (set_types-style fields, whose
    input parse format may legitimately differ from ``outputFormat``), else
    ``outputFormat``. Returns None if neither is an explicit strftime string."""
    fmt = field.get("format")
    if fmt and "%" in fmt:
        return fmt
    of = field.get("outputFormat")
    if of and "%" in of:
        return of
    return None


def _cast_fields(fields):
    """Field descriptors for the frictionless cast, with the DuckDB storage-format
    invariant applied: a temporal field lacking an explicit cast ``format`` but
    carrying an ``outputFormat`` gets ``format = outputFormat`` so ``schema_validator``
    parses the VARCHAR that ``format_out_rows`` wrote. See ``_storage_format``."""
    out = []
    for f in fields:
        f = dict(f)
        if f.get("type") in _TEMPORAL and not (f.get("format") and "%" in f.get("format", "")):
            sf = _storage_format(f)
            if sf:
                f["format"] = sf
        out.append(f)
    return out


def cast_rows(rows, fields, resource_name="_cast"):
    """Cast each row's string cells to the frictionless types declared in
    ``fields`` (a list of field descriptors). Yields the same rows with typed
    values. This IS ``schema_validator`` -- the dataflows casting path (with the
    temporal storage-format invariant applied, see ``_cast_fields``)."""
    descriptor = {"name": resource_name, "schema": {"fields": _cast_fields(fields)}}
    return schema_validator(descriptor, rows)


def _format_out_value(value, field):
    """Serialize one typed value to the VARCHAR form that ``cast_rows`` (via the
    field's cast ``format``) parses back to the identical value.

    Inverse of the frictionless cast: for temporals with an explicit strftime
    ``format`` (contains ``%``) we ``strftime`` with that format, so
    ``cast_value(strftime(v)) == v``. Non-temporal / no-format values pass through
    unchanged (``str()`` at storage time round-trips number/integer/string)."""
    if value is None:
        return None
    ftype = field.get("type")
    if ftype in _TEMPORAL and isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        fmt = _storage_format(field)
        if fmt:
            return value.strftime(fmt)
        # No explicit strftime format ("default"/"any") anywhere: fall back to
        # isoformat. NOTE: frictionless's strict "default" temporal cast may not
        # round-trip this; real temporal processors set an explicit output format,
        # so this branch is a best-effort edge case (see PLAN.md §4.2).
        return value.isoformat()
    return value


def format_out_iter(rows, fields):
    """Lazy generator form of ``format_out_rows`` -- yields one VARCHAR-safe row at
    a time so a large structural output (join/sort/...) can be streamed straight
    into ``Engine.reingest_stream`` without materializing. See ``format_out_rows``."""
    by_name = {f["name"]: f for f in fields}
    for row in rows:
        yield {
            k: (_format_out_value(v, by_name[k]) if k in by_name else v)
            for k, v in row.items()
        }


def format_out_rows(rows, fields):
    """Serialize the typed values a ``process_rows`` produced back to VARCHAR-safe
    forms that re-cast identically at the dump/next-step boundary. The inverse of
    ``cast_rows`` for the DuckDB lane's VARCHAR storage. Only fields present in
    ``fields`` are touched; other keys (e.g. ``__rownum__``) pass through. Eager
    wrapper over ``format_out_iter``."""
    return list(format_out_iter(rows, fields))
