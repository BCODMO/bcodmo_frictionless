"""Equivalence harness: the machinery that makes single-source non-negotiable.

Provides the differential comparison (dataflows/source-of-truth vs DuckDB) and
the DSL fuzzer. Wired into ``tests/`` as CI gates (PLAN.md §3, §11).
"""
