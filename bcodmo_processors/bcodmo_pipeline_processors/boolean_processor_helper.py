import sys
import logging
from datetime import datetime
from dateutil import parser
from pyparsing import (
    Regex, Group, operatorPrecedence,
    opAssoc, Literal, Forward,
    ZeroOrMore, ParseException,
)

NULL_VALUES = ['null', 'NULL', 'None', 'NONE']
ROW_NUMBER = ['ROW_NUMBER', 'LINE_NUMBER']

'''
BOOLEAN OPERATION
'''

# Set up a language logic for parsing boolean strings and mathematical expresion
boolean_operator = Regex(">=|<=|!=|>|<|==").setName("boolean_operator")
math_operator = Regex(">=|<=|!=|>|<|==").setName("math_operator")
date = Regex("(\d+[/:\- ])+(\d+)?")
number = Regex(r"[+-]?\d+(:?\.\d*)?(:?[eE][+-]?\d+)?")
variable = Regex(r"\{.*?\}")
string = Regex(r"'.*?'")
null = Regex('|'.join(NULL_VALUES))
row_number = Regex('|'.join(ROW_NUMBER))

boolean_comparison_term = date | number | variable | string | null | row_number
boolean_condition = Group(
    boolean_comparison_term + boolean_operator + boolean_comparison_term,
)
boolean_expr = operatorPrecedence(
    boolean_condition,
    [
        ("AND", 2, opAssoc.LEFT, ),
        ("and", 2, opAssoc.LEFT, ),
        ("&&", 2, opAssoc.LEFT, ),
        ("OR", 2, opAssoc.LEFT, ),
        ("or", 2, opAssoc.LEFT, ),
        ("||", 2, opAssoc.LEFT, ),
    ],
)

def parse_boolean(row_counter, res, row, missing_data_values):
    ''' Parse a boolean result from pyparser '''
    if type(res) == bool:
        return res
    # Try to convert to float
    try:
        return float(res)
    except (ValueError, TypeError):
        pass
    # Parse string
    if type(res) == str:
        # Handle null passed in
        if res in NULL_VALUES:
            return None
        if res in ROW_NUMBER:
            return row_counter
        if res.startswith("'") and res.endswith("'"):
            return res[1:-1]

        try:
            val = res.format(**row)
            if isinstance(val, datetime):
                return val
            return float(val)
        except ValueError:
            try:
                val = res.format(**row)
                # Handle val being part of missing_data_Values
                if val in missing_data_values or val is None or (val == 'None' and row[res[1:-1]] == None):
                    return None
                # Parse into datetime
                return parser.parse(val)
            except ValueError:
                return val

    if len(res) == 0:
        return False
    if len(res) == 1:
        return parse_boolean(row_counter, res[0], row, missing_data_values)
    first_value = None
    operation = None
    try:
        for term in res:
            if not first_value:
                first_value = term
            elif not operation:
                operation = term
            else:
                first_parsed = parse_boolean(row_counter, first_value, row, missing_data_values)
                second_parsed = parse_boolean(row_counter, term, row, missing_data_values)
                if operation == '>':
                    first_value = first_parsed > second_parsed
                elif operation == '>=':
                    first_value = first_parsed >= second_parsed
                elif operation == '<':
                    first_value = first_parsed < second_parsed
                elif operation == '<=':
                    first_value = first_parsed <= second_parsed
                elif operation == '!=':
                    first_value = first_parsed != second_parsed
                elif operation == '==':
                    first_value = first_parsed == second_parsed
                elif operation in ['AND', 'and', '&&']:
                    first_value = first_parsed and second_parsed
                elif operation in ['OR', 'or', '||']:
                    first_value = first_parsed or second_parsed
                operation = None
    except TypeError as e:
        raise e

    return first_value




'''
MATHEMATICAL OPERATION
'''
exprStack = []
def pushFirst( strg, loc, toks ):
    exprStack.append( toks[0] )
def pushUMinus( strg, loc, toks ):
    for t in toks:
      if t == '-':
        exprStack.append( 'unary -' )
        #~ exprStack.append( '-1' )
        #~ exprStack.append( '*' )
      else:
        break

# An expresion for the value variable for a mathematical expression
math_term = number | variable
plus  = Literal("+")
minus = Literal("-")
mult  = Literal("*")
div   = Literal("/")
addop  = plus | minus
multop = mult | div
expop = Literal("^")

math_expr = operatorPrecedence(
    math_term,
    [
        (expop, 2, opAssoc.LEFT, ),
        (multop, 2, opAssoc.LEFT, ),
        (addop, 2, opAssoc.LEFT, ),
    ]
)



def parse_math(row_counter, res, row, missing_data_values):
    ''' Parse a math result from pyparser '''
    # Try to convert to float
    try:
        return float(res)
    except (ValueError, TypeError):
        pass
    # Parse string
    if type(res) == str:
        try:
            return float((res.format(**row)))
        except ValueError:
            try:
                val = res.format(**row)
                if val in ['+', '-', '*', '/', '^']:
                    return val
                # Handle val being part of missing_data_Values
                if val in missing_data_values or val is None or (val == 'None' and row[res[1:-1]] == None):
                    return None
                raise Exception(f'String {res} has a value of {val} which cannot be parsed to a number or NoneType')
            except ValueError:
                raise Exception(f'String {res} has no corresponding field in the row')

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
                current_operation = parse_math(row_counter, term, row, missing_data_values)
                continue
            next_value = parse_math(row_counter, term, row, missing_data_values)
            if next_value == None:
                return None
            if current_operation == '+':
                current_value = current_value + next_value
            elif current_operation == '-':
                current_value = current_value - next_value
            elif current_operation == '*':
                current_value = current_value * next_value
            elif current_operation == '/':
                current_value = current_value / next_value
            elif current_operation == '^':
                current_value = current_value ** next_value
            else:
                raise Exception(f'Operation {current_operation} not found')

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
                f'Error parsing boolean string {function}. Make sure all strings are surrounded by \' \' and all fields are surrounded by {{ }}: '
            + str(e)
        ).with_traceback(sys.exc_info()[2])

def check_line(expression, row_counter, row, missing_data_values, parser=parse_boolean):
    if not expression:
        return True
    try:
        return parser(row_counter, expression, row, missing_data_values)
    except Exception as e:
        raise type(e)(
            str(e) +
            f' at row {row_counter}'
        ).with_traceback(sys.exc_info()[2])
    return False
