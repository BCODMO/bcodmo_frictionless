import sys
from typing import Pattern
import logging
import re
from datetime import datetime, date as date_type
from dateutil import parser
from pyparsing import (
    Regex,
    Group,
    infix_notation,
    opAssoc,
    Literal,
    Forward,
    ZeroOrMore,
    ParseException,
    ParseResults,
)
from decimal import Decimal, InvalidOperation

NULL_VALUES = ["null", "NULL", "None", "NONE"]
ROW_NUMBER = ["ROW_NUMBER", "LINE_NUMBER"]

"""
BOOLEAN OPERATION
"""

# Set up a language logic for parsing boolean strings and mathematical expresion
boolean_operator = Regex(">=|<=|!=|>|<|==").setName("boolean_operator")
math_operator = Regex(">=|<=|!=|>|<|==").setName("math_operator")
date = Regex(r"(\d+[/:\- ])+(\d+)?")
number = Regex(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
variable = Regex(r"\{.*?\}")
regex = Regex(r"re'.*?'")
string = Regex(r"'.*?'")
null = Regex("|".join(NULL_VALUES))
row_number = Regex("|".join(ROW_NUMBER))

boolean_comparison_term = date | number | variable | regex | string | null | row_number
boolean_condition = Group(
    boolean_comparison_term + boolean_operator + boolean_comparison_term,
)
boolean_expr = infix_notation(
    boolean_condition,
    [
        ("AND", 2, opAssoc.LEFT,),
        ("and", 2, opAssoc.LEFT,),
        ("&&", 2, opAssoc.LEFT,),
        ("OR", 2, opAssoc.LEFT,),
        ("or", 2, opAssoc.LEFT,),
        ("||", 2, opAssoc.LEFT,),
    ],
)


def parse_boolean(row_counter, res, row, missing_data_values):
    """ Parse a boolean result from pyparser """
    if type(res) in [bool, Decimal, float, int, datetime]:
        return res
    # Parse string
    elif type(res) == str:
        # Handle null passed in
        if res in NULL_VALUES:
            return None
        if res in ROW_NUMBER:
            return row_counter
        if res.startswith("re'") and res.endswith("'"):
            return re.compile(res[3:-1])
        if res.startswith("'") and res.endswith("'"):
            return res[1:-1]
        if res.startswith("{") and res.endswith("}"):
            key = res[1:-1]
            if key not in row:
                raise Exception(f"Invalid field name {key}")
            val = row[key]
            if isinstance(val, datetime):
                return val
            if type(val) in [Decimal, float, int]:
                return Decimal(val)
            if (
                val in missing_data_values
                or val is None
                or (val == "None" and row[res[1:-1]] == None)
            ):
                return None
            return val
        if res.replace(".", "", 1).strip().isdigit():
            return Decimal(res)
        # Try to convert to a date
        try:
            return parser.parse(res)
        except Exception as e:
            raise e
    if type(res) is not ParseResults:
        raise Exception(f"Unable to parse value: {res}")

    if len(res) == 0:
        return False
    if len(res) == 1:
        return parse_boolean(row_counter, res[0], row, missing_data_values)
    first_value = None
    operation = None
    try:
        for term in res:

            if first_value is None:
                first_value = term
            elif operation is None:
                operation = term
            else:
                first_parsed = parse_boolean(
                    row_counter, first_value, row, missing_data_values
                )
                second_parsed = parse_boolean(
                    row_counter, term, row, missing_data_values
                )

                # Throw error for invalid regex comparision
                f_type = type(first_parsed)
                s_type = type(second_parsed)
                if (
                    isinstance(first_parsed, Pattern)
                    and s_type not in [str, type(None)]
                ) or (
                    isinstance(second_parsed, Pattern)
                    and f_type not in [str, type(None)]
                ):
                    raise Exception(
                        f"For regular expression boolean comparison the other value has to be of type string: {f_type} and {s_type}"
                    )

                # Convert to date if we are comparing with a date field
                if f_type is datetime and s_type is date_type:
                    first_parsed = first_parsed.date()
                if f_type is date_type and s_type is datetime:
                    second_parsed = second_parsed.date()

                if (first_parsed is None or second_parsed is None) and operation in [
                    ">",
                    ">=",
                    "<",
                    "<=",
                ]:
                    first_value = False
                elif operation == ">":
                    first_value = first_parsed > second_parsed
                elif operation == ">=":
                    first_value = first_parsed >= second_parsed
                elif operation == "<":
                    first_value = first_parsed < second_parsed
                elif operation == "<=":
                    first_value = first_parsed <= second_parsed
                elif operation == "!=" or operation == "==":
                    if isinstance(first_parsed, Pattern):
                        if second_parsed is None:
                            first_value = False
                        else:
                            first_value = first_parsed.match(second_parsed)
                    elif isinstance(second_parsed, Pattern):
                        if first_parsed is None:
                            first_value = False
                        else:
                            first_value = second_parsed.match(first_parsed)
                    else:
                        first_value = first_parsed == second_parsed
                    if operation == "!=":
                        first_value = not first_value
                elif operation in ["AND", "and", "&&"]:
                    first_value = first_parsed and second_parsed
                elif operation in ["OR", "or", "||"]:
                    first_value = first_parsed or second_parsed
                operation = None
    except TypeError as e:
        raise e

    return first_value


"""
MATHEMATICAL OPERATION
"""
exprStack = []


def pushFirst(strg, loc, toks):
    exprStack.append(toks[0])


def pushUMinus(strg, loc, toks):
    for t in toks:
        if t == "-":
            exprStack.append("unary -")
            # ~ exprStack.append( '-1' )
            # ~ exprStack.append( '*' )
        else:
            break


# An expresion for the value variable for a mathematical expression
math_term = number | variable
plus = Literal("+")
minus = Literal("-")
mult = Literal("*")
div = Literal("/")
addop = plus | minus
multop = mult | div
expop = Literal("^")

math_expr = infix_notation(
    math_term,
    [(expop, 2, opAssoc.LEFT,), (multop, 2, opAssoc.LEFT,), (addop, 2, opAssoc.LEFT,),],
)


def parse_math(row_counter, res, row, missing_data_values):
    """ Parse a math result from pyparser """
    # Try to convert to float
    if type(res) in [Decimal, float, int]:
        return Decimal(res)
    # Parse string
    elif type(res) == str:
        if res.startswith("{") and res.endswith("}"):
            key = res[1:-1]
            if key not in row:
                raise Exception(f"Invalid field name {key}")
            val = row[key]
            if val in missing_data_values or val is None:
                return None
            if type(val) in [Decimal, float, int]:
                return Decimal(val)
            raise Exception(f"Field {key} is not a number or integer")
        if res in ["+", "-", "*", "/", "^"]:
            return res
        try:
            return Decimal(res)
        except InvalidOperation:
            raise Exception(f"Invalid input to math operation: {res}")

    if len(res) == 0:
        return None
    if len(res) == 1:
        return parse_math(row_counter, res[0], row, missing_data_values)
    current_operation = None
    current_value = None
    try:
        for term in res:
            if current_value == None:
                current_value = parse_math(row_counter, term, row, missing_data_values)
                if current_value == None:
                    return None
                continue
            if current_operation == None:
                current_operation = parse_math(
                    row_counter, term, row, missing_data_values
                )
                continue
            next_value = parse_math(row_counter, term, row, missing_data_values)
            if next_value == None:
                return None
            if current_operation == "+":
                current_value = current_value + next_value
            elif current_operation == "-":
                current_value = current_value - next_value
            elif current_operation == "*":
                current_value = current_value * next_value
            elif current_operation == "/":
                current_value = current_value / next_value
            elif current_operation == "^":
                current_value = current_value ** next_value
            else:
                raise Exception(f"Operation {current_operation} not found")

            current_operation = None
    except TypeError as e:
        raise e

    return current_value


"""
COMPILED EXPRESSIONS

parse_boolean/parse_math above walk the pyparsing parse tree from scratch for
every single row: they re-run isinstance dispatch on every node, re-compile
regex literals (re.compile), re-convert numeric/date literals to Decimal/datetime
and rebuild the operator chain each time. For a step with many branches over a
large resource this is the dominant cost (and shows up as a worker pegged at
100% CPU making no visible progress).

compile_boolean/compile_math do that tree walk once and return a closure of the
form ``fn(row_counter, row, missing_data_values) -> value``. Everything that is
constant for a given expression - regex patterns, numeric/date literals, quoted
strings, which operator applies at each node - is resolved at compile time and
captured in the closure. Only the irreducible per-row work (field lookups and
the actual comparisons/arithmetic) remains. The returned value is identical to
what parse_boolean/parse_math would have returned for the same row.
"""


def compile_boolean(res):
    """Compile a parsed boolean expression into fn(row_counter, row, missing)."""
    t = type(res)
    if t in [bool, Decimal, float, int, datetime]:
        return lambda row_counter, row, missing_data_values: res
    if t is str:
        if res in NULL_VALUES:
            return lambda row_counter, row, missing_data_values: None
        if res in ROW_NUMBER:
            return lambda row_counter, row, missing_data_values: row_counter
        if res.startswith("re'") and res.endswith("'"):
            # Compile the regex once instead of on every row.
            pattern = re.compile(res[3:-1])
            return lambda row_counter, row, missing_data_values: pattern
        if res.startswith("'") and res.endswith("'"):
            const = res[1:-1]
            return lambda row_counter, row, missing_data_values: const
        if res.startswith("{") and res.endswith("}"):
            key = res[1:-1]

            def field_lookup(row_counter, row, missing_data_values, key=key):
                if key not in row:
                    raise Exception(f"Invalid field name {key}")
                val = row[key]
                if isinstance(val, datetime):
                    return val
                if type(val) in [Decimal, float, int]:
                    return Decimal(val)
                if (
                    val in missing_data_values
                    or val is None
                    or (val == "None" and row[key] is None)
                ):
                    return None
                return val

            return field_lookup
        if res.replace(".", "", 1).strip().isdigit():
            const = Decimal(res)
            return lambda row_counter, row, missing_data_values: const
        # Date literal - parse it once.
        const = parser.parse(res)
        return lambda row_counter, row, missing_data_values: const
    if t is not ParseResults:
        raise Exception(f"Unable to parse value: {res}")

    if len(res) == 0:
        return lambda row_counter, row, missing_data_values: False
    if len(res) == 1:
        return compile_boolean(res[0])

    # A chain of (operator, operand) pairs folded left to right, mirroring the
    # first_value/operation loop in parse_boolean.
    terms = list(res)
    first_fn = compile_boolean(terms[0])
    ops = []
    index = 1
    while index < len(terms):
        operation = terms[index]
        operand_fn = compile_boolean(terms[index + 1])
        ops.append((operation, operand_fn))
        index += 2

    def evaluate(row_counter, row, missing_data_values):
        first_value = first_fn(row_counter, row, missing_data_values)
        for operation, operand_fn in ops:
            first_parsed = first_value
            second_parsed = operand_fn(row_counter, row, missing_data_values)

            f_type = type(first_parsed)
            s_type = type(second_parsed)
            if (
                isinstance(first_parsed, Pattern) and s_type not in [str, type(None)]
            ) or (
                isinstance(second_parsed, Pattern) and f_type not in [str, type(None)]
            ):
                raise Exception(
                    f"For regular expression boolean comparison the other value has to be of type string: {f_type} and {s_type}"
                )

            if f_type is datetime and s_type is date_type:
                first_parsed = first_parsed.date()
            if f_type is date_type and s_type is datetime:
                second_parsed = second_parsed.date()

            if (first_parsed is None or second_parsed is None) and operation in [
                ">",
                ">=",
                "<",
                "<=",
            ]:
                first_value = False
            elif operation == ">":
                first_value = first_parsed > second_parsed
            elif operation == ">=":
                first_value = first_parsed >= second_parsed
            elif operation == "<":
                first_value = first_parsed < second_parsed
            elif operation == "<=":
                first_value = first_parsed <= second_parsed
            elif operation == "!=" or operation == "==":
                if isinstance(first_parsed, Pattern):
                    if second_parsed is None:
                        first_value = False
                    else:
                        first_value = first_parsed.match(second_parsed)
                elif isinstance(second_parsed, Pattern):
                    if first_parsed is None:
                        first_value = False
                    else:
                        first_value = second_parsed.match(first_parsed)
                else:
                    first_value = first_parsed == second_parsed
                if operation == "!=":
                    first_value = not first_value
            elif operation in ["AND", "and", "&&"]:
                first_value = first_parsed and second_parsed
            elif operation in ["OR", "or", "||"]:
                first_value = first_parsed or second_parsed
        return first_value

    return evaluate


def compile_math(res):
    """Compile a parsed math expression into fn(row_counter, row, missing)."""
    t = type(res)
    if t in [Decimal, float, int]:
        const = Decimal(res)
        return lambda row_counter, row, missing_data_values: const
    if t is str:
        if res.startswith("{") and res.endswith("}"):
            key = res[1:-1]

            def field_lookup(row_counter, row, missing_data_values, key=key):
                if key not in row:
                    raise Exception(f"Invalid field name {key}")
                val = row[key]
                if val in missing_data_values or val is None:
                    return None
                if type(val) in [Decimal, float, int]:
                    return Decimal(val)
                raise Exception(f"Field {key} is not a number or integer")

            return field_lookup
        if res in ["+", "-", "*", "/", "^"]:
            return lambda row_counter, row, missing_data_values: res
        try:
            const = Decimal(res)
        except InvalidOperation:
            raise Exception(f"Invalid input to math operation: {res}")
        return lambda row_counter, row, missing_data_values: const

    if len(res) == 0:
        return lambda row_counter, row, missing_data_values: None
    if len(res) == 1:
        return compile_math(res[0])

    terms = list(res)
    first_fn = compile_math(terms[0])
    ops = []
    index = 1
    while index < len(terms):
        operation = terms[index]  # a raw operator token "+", "-", ...
        operand_fn = compile_math(terms[index + 1])
        ops.append((operation, operand_fn))
        index += 2

    def evaluate(row_counter, row, missing_data_values):
        current_value = first_fn(row_counter, row, missing_data_values)
        if current_value is None:
            return None
        for operation, operand_fn in ops:
            next_value = operand_fn(row_counter, row, missing_data_values)
            if next_value is None:
                return None
            if operation == "+":
                current_value = current_value + next_value
            elif operation == "-":
                current_value = current_value - next_value
            elif operation == "*":
                current_value = current_value * next_value
            elif operation == "/":
                current_value = current_value / next_value
            elif operation == "^":
                current_value = current_value ** next_value
            else:
                raise Exception(f"Operation {operation} not found")
        return current_value

    return evaluate


def get_expression(function, expr_parser=boolean_expr):
    if not function:
        return None
    try:
        return expr_parser.parseString(function)
    except ParseException as e:
        raise type(e)(
            f"Error parsing boolean string {function}. Make sure all strings are surrounded by ' ' and all fields are surrounded by {{ }}: "
            + str(e)
        ).with_traceback(sys.exc_info()[2])


def get_compiled_boolean(function):
    """Parse and compile a boolean string into a per-row callable (or None)."""
    expression = get_expression(function, boolean_expr)
    if expression is None:
        return None
    return compile_boolean(expression)


def get_compiled_math(function):
    """Parse and compile a math string into a per-row callable (or None)."""
    expression = get_expression(function, math_expr)
    if expression is None:
        return None
    return compile_math(expression)


def check_line(expression, row_counter, row, missing_data_values, parser=parse_boolean):
    if not expression:
        return True
    try:
        return parser(row_counter, expression, row, missing_data_values)
    except Exception as e:
        raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
            sys.exc_info()[2]
        )
    return False


def check_line_compiled(compiled_expression, row_counter, row, missing_data_values):
    """
    Drop-in replacement for check_line that takes a pre-compiled boolean closure
    (from get_compiled_boolean) instead of re-walking a pyparsing tree every row.

    Semantics match check_line exactly: a missing expression (None) passes, and
    any error raised while evaluating is re-raised annotated with the row number.
    Callers should compile the boolean_statement once at setup with
    get_compiled_boolean and then call this per row.
    """
    if compiled_expression is None:
        return True
    try:
        return compiled_expression(row_counter, row, missing_data_values)
    except Exception as e:
        raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
            sys.exc_info()[2]
        )
