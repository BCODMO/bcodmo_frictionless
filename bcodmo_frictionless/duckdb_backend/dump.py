"""
Byte-exact CSV egress.

The DuckDB lane processes fast (VARCHAR, vectorized) and then hands its ORDERED,
CAST rows to the SAME serializer production uses -- bcodmo's ``CustomCSVFormat``
(the ``dump_to_s3`` writer: ``num_to_string`` for numbers, scientific-notation
support, temporal ``outputFormat``). Reusing the real class means the CSV bytes
cannot diverge from the dataflows lane's dump; there is nothing to keep in sync.

Production invokes ``dump_to_s3`` with ``temporal_format_property="outputFormat"``
and ``use_titles`` unset (see laminar_web Step/constants.ts) -- the defaults here.

This is byte-for-byte the ``CustomCSVFormat`` output. (``dump_to_path`` layers a
CRLF->LF normalization on top; ``dump_to_s3`` -- the UI's "Dump final" -- does
not, so this matches the S3 artifact.)
"""

import io

from tableschema import Schema

from bcodmo_frictionless.bcodmo_pipeline_processors.dump_to_s3 import CustomCSVFormat


def to_csv_bytes(fields, rows, temporal_format_property="outputFormat", use_titles=False):
    """Serialize ``rows`` (already cast + ordered) under ``fields`` to CSV bytes,
    exactly as the production ``dump_to_s3`` CSV writer would."""
    schema = Schema({"fields": [dict(f) for f in fields]})
    buf = io.StringIO()
    fmt = CustomCSVFormat(
        buf, schema,
        write_header=True,
        use_titles=use_titles,
        temporal_format_property=temporal_format_property,
    )
    for row in rows:
        fmt.write_row(row)
    return buf.getvalue().encode("utf-8")
