"""
DuckDB execution backend for bcodmo pipelines.

See PLAN.md. Core idea: each processor's logic is authored ONCE (the
``Processor`` contract: ``update_schema`` + ``process_rows`` [+ optional
``to_sql``]); the dataflows engine and this DuckDB engine are thin adapters over
it. Any ``to_sql`` fast path is a provably-verified mirror of ``process_rows``,
enforced by the differential/fuzz tests in ``equivalence/`` and ``tests/``.

Phase 0 scope: the contract, the DSL->SQL compiler, a minimal engine (native
``to_sql`` path + UDF-default ``process_rows`` path), one reference processor,
and the equivalence harness. Not yet wired into laminar_server.
"""

from .processor import Processor, register, REGISTRY  # noqa: F401

# Import processor modules so they self-register.
from .processors import add_computed_field  # noqa: F401
from .processors import add_schema_metadata  # noqa: F401
from .processors import boolean_add_computed_field  # noqa: F401
from .processors import boolean_filter_rows  # noqa: F401
from .processors import concatenate  # noqa: F401
from .processors import convert_date  # noqa: F401
from .processors import convert_to_decimal_degrees  # noqa: F401
from .processors import convert_units  # noqa: F401
from .processors import delete_fields  # noqa: F401
from .processors import duplicate  # noqa: F401
from .processors import dump_to_s3  # noqa: F401
from .processors import edit_cells  # noqa: F401
from .processors import extract_nonnumeric  # noqa: F401
from .processors import find_replace  # noqa: F401
from .processors import join  # noqa: F401
from .processors import load  # noqa: F401
from .processors import remove_resources  # noqa: F401
from .processors import rename_fields  # noqa: F401
from .processors import rename_fields_regex  # noqa: F401
from .processors import rename_resource  # noqa: F401
from .processors import reorder_fields  # noqa: F401
from .processors import round_fields  # noqa: F401
from .processors import set_types  # noqa: F401
from .processors import set_types_standard  # noqa: F401
from .processors import sort  # noqa: F401
from .processors import split_column  # noqa: F401
from .processors import string_format  # noqa: F401
from .processors import unpivot  # noqa: F401
from .processors import update_fields  # noqa: F401
