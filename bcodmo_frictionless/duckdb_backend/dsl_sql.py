"""
DSL -> SQL compiler for the bcodmo boolean/math DSL.

Reuses the EXACT pyparsing grammar the interpreter uses
(``get_expression`` / ``boolean_expr`` / ``math_expr`` from
``boolean_processor_helper``), walks the parse tree, and emits a DuckDB SQL
expression that evaluates per-row to the same result as ``parse_boolean`` /
``parse_math`` -- but vectorized.

This is the shared source for the DSL family: the interpreter and this compiler
both derive from the one grammar, so the ``to_sql`` fast path for
``boolean_filter_rows`` / ``boolean_add_computed_field`` is not an independent
implementation. The ``equivalence/fuzz_dsl`` + ``tests`` gate keeps them aligned.

Coverage (Phase 0): numeric/string/regex comparisons, ``and``/``or``, math
``+ - * / ^``, ``{field}`` vars, last-match-wins, None-safety, ``re.match``
anchoring. TODO before full production (PLAN.md §3): missingValues->NULL,
``{placeholder}`` in non-math values, date literals, ROW_NUMBER window.
"""

from bcodmo_frictionless.bcodmo_pipeline_processors.boolean_processor_helper import (
    get_expression,
    math_expr,
)

COMPARISON = {">", ">=", "<", "<=", "==", "!="}
AND_TOKENS = {"AND", "and", "&&"}
OR_TOKENS = {"OR", "or", "||"}


def _sql_str(s):
    return "'" + str(s).replace("'", "''") + "'"


def _is_var(tok):
    return isinstance(tok, str) and tok.startswith("{") and tok.endswith("}")


def _is_regex(tok):
    return isinstance(tok, str) and tok.startswith("re'") and tok.endswith("'")


def _is_string(tok):
    return isinstance(tok, str) and tok.startswith("'") and tok.endswith("'")


def _col(tok):
    return '"' + tok[1:-1] + '"'


def _looks_numeric(tok):
    if not isinstance(tok, str) or _is_var(tok) or _is_regex(tok) or _is_string(tok):
        return False
    try:
        float(tok)
        return True
    except ValueError:
        return False


def _term_sql(tok, numeric):
    if _is_var(tok):
        return f"TRY_CAST({_col(tok)} AS DOUBLE)" if numeric else _col(tok)
    if _is_string(tok):
        return _sql_str(tok[1:-1])
    if tok in ("null", "NULL", "None", "NONE"):
        return "NULL"
    return str(tok)  # numeric / date literal


def _compile_comparison(lhs, op, rhs):
    if _is_regex(lhs) or _is_regex(rhs):
        re_tok, var_tok = (lhs, rhs) if _is_regex(lhs) else (rhs, lhs)
        pattern = re_tok[3:-1]
        # Python re.match anchors at start (not end). Emulate with '^(?:...)',
        # unless the pattern already provides its own leading '^' anchor.
        anchored = pattern if pattern.startswith("^") else "^(?:" + pattern + ")"
        col = _term_sql(var_tok, numeric=False)
        match = f"COALESCE(regexp_matches({col}, {_sql_str(anchored)}), false)"
        return match if op == "==" else f"(NOT {match})"

    numeric = _looks_numeric(lhs) or _looks_numeric(rhs)
    sql_op = {"==": "=", "!=": "<>"}.get(op, op)
    return f"({_term_sql(lhs, numeric)} {sql_op} {_term_sql(rhs, numeric)})"


def compile_bool(res):
    """Parsed boolean expression tree -> SQL boolean expression."""
    if isinstance(res, str):
        return res
    if len(res) == 1:
        return compile_bool(res[0])
    if len(res) == 3 and isinstance(res[1], str) and res[1] in COMPARISON:
        return _compile_comparison(res[0], res[1], res[2])
    sql = "(" + compile_bool(res[0]) + ")"
    i = 1
    while i < len(res):
        op, rhs = res[i], res[i + 1]
        joiner = "AND" if op in AND_TOKENS else "OR"
        sql += f" {joiner} (" + compile_bool(rhs) + ")"
        i += 2
    return sql


def compile_math(res):
    """Parsed math expression tree -> SQL arithmetic expression."""
    if isinstance(res, str):
        if _is_var(res):
            return f"TRY_CAST({_col(res)} AS DOUBLE)"
        return res  # number literal or operator
    if len(res) == 1:
        return compile_math(res[0])
    out = compile_math(res[0])
    i = 1
    while i < len(res):
        op, rhs = res[i], compile_math(res[i + 1])
        out = f"power({out}, {rhs})" if op == "^" else f"({out} {op} {rhs})"
        i += 2
    return out


def compile_boolean_statement(statement):
    """Compile a boolean_statement (filter / gate) to a SQL boolean, or None."""
    if not statement:
        return None
    return compile_bool(get_expression(statement))


def compile_boolean_add_computed_field(field_cfg):
    """One boolean_add_computed_field `field` config -> a SQL CASE expression.

    bcodmo overwrites the target on every matching function (last-match-wins), so
    we reverse the WHEN branches (CASE returns the first match =>
    first-in-reversed == last-in-original).
    """
    whens = []
    for fn in field_cfg.get("functions", []):
        cond = compile_bool(get_expression(fn["boolean"]))
        if fn.get("math_operation", False):
            val = compile_math(get_expression(fn["value"], math_expr))
        else:
            val = _sql_str(fn.get("value", ""))  # real BATS values are literals
        whens.append((cond, val))

    lines = ["CASE"]
    for cond, val in reversed(whens):
        lines.append(f"    WHEN {cond} THEN {val}")
    lines.append("    ELSE NULL")
    lines.append("END")
    return "\n".join(lines)
