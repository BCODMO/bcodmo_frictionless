"""
DSL fuzzer (test layer B for the DSL family).

Generates random valid boolean expressions of the Vessel shape
(``{x} OP N`` combined with and/or) and asserts the interpreter
(``parse_boolean``) and the compiler (``dsl_sql.compile_bool``) agree for random
inputs -- i.e. the ``to_sql`` fast path can never silently diverge from the
source of truth.
"""

import random

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    check_line,
)
from ..dsl_sql import compile_bool

_OPS = [">=", "<=", ">", "<", "==", "!="]
_COMB = ["and", "or"]


def random_expression(rng):
    n = rng.randint(1, 3)
    parts = []
    for k in range(n):
        op = rng.choice(_OPS)
        val = rng.randint(0, 20)
        parts.append(f"{{x}} {op} {val}")
        if k < n - 1:
            parts.append(rng.choice(_COMB))
    return " ".join(parts)


def interp(expr, x):
    return bool(check_line(get_expression(expr), 1, {"x": x}, []))


def sql(con, expr, x):
    s = compile_bool(get_expression(expr))
    return bool(con.execute(f"SELECT {s} FROM (SELECT {x} AS x)").fetchone()[0])


def run(con, iterations=2000, seed=1234):
    """Return list of (expr, x, interp, sql) mismatches (empty == pass)."""
    rng = random.Random(seed)
    mismatches = []
    for _ in range(iterations):
        expr = random_expression(rng)
        x = rng.randint(-2, 22)
        i, s = interp(expr, x), sql(con, expr, x)
        if i != s:
            mismatches.append((expr, x, i, s))
    return mismatches
