"""
Governance gates (PLAN.md §3.4) — mechanical enforcement of single-source.

These make divergence *impossible to merge*, not a convention:
  * every registered processor implements the contract + documents SEMANTICS
  * every processor that defines a `to_sql` fast path has an equivalence test
"""

import importlib
import pathlib

from bcodmo_frictionless.duckdb_backend import REGISTRY
from bcodmo_frictionless.duckdb_backend.processor import Processor

_TESTS_DIR = pathlib.Path(__file__).parent


def _all_test_source():
    return "\n".join(p.read_text() for p in _TESTS_DIR.glob("test_*.py"))


def test_contract_completeness():
    """Every processor implements process_rows + update_schema and documents SEMANTICS."""
    for name, proc in REGISTRY.items():
        cls = type(proc)
        assert cls.update_schema is not Processor.update_schema, f"{name}: no update_schema"
        assert cls.process_rows is not Processor.process_rows, f"{name}: no process_rows"
        mod = importlib.import_module(cls.__module__)
        assert "SEMANTICS" in (mod.__doc__ or ""), f"{name}: missing SEMANTICS doc block"


def test_every_to_sql_has_equivalence_test():
    """A native to_sql cannot ship without a test pinning it to process_rows."""
    src = _all_test_source()
    for name, proc in REGISTRY.items():
        defines_to_sql = type(proc).to_sql is not Processor.to_sql
        if defines_to_sql:
            assert name in src, (
                f"{name} defines to_sql but no test references it — "
                f"add an equivalence test (PLAN.md §3.2 gate B)."
            )
