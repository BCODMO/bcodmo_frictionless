import sys
import functools
import collections
import logging
import time
import dateutil.parser
import datetime

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher


from bcodmo_frictionless.bcodmo_pipeline_processors.helper import get_missing_values


def process_resource(rows, edited, missing_values):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            if row_counter in edited or str(row_counter) in edited:
                try:
                    edited_cells = edited.pop(row_counter)
                except KeyError:
                    edited_cells = edited.pop(str(row_counter))
                for edited_cell in edited_cells:
                    field = edited_cell.get("field")
                    value = edited_cell.get("value")
                    if field not in row:
                        raise Exception(
                            f"field given to edit_cells processor not found in row: '{field}'"
                        )
                    row[field] = value
                pass
            yield row
        except Exception as e:
            raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                sys.exc_info()[2]
            )
    if len(edited.keys()):
        raise Exception(
            f"Passed in row numbers that were not used ({str(list(edited.keys()))}) to the edit_cells processor."
        )


def edit_cells(edited, resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(
                    rows,
                    edited,
                    missing_values,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        edit_cells(
            parameters.get("edited", {}),
            resources=parameters.get("resources"),
        )
    )
