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
