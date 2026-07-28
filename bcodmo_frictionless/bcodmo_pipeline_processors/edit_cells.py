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


def normalize_edited(edited):
    """Normalize the "edited" parameter into an ordered list of row edits.

    The expected shape is an ordered list, each item targeting a single row by
    its (1-based) row number and listing the cells to edit on that row:

        [{"row": 20, "cells": [{"field": "col1", "value": "hello"}]}, ...]

    A list is used so that the order the edits were entered in is preserved.

    For backwards compatibility a legacy dict mapping row number -> cells is
    also accepted and migrated to the list shape. A dict cannot preserve the
    intended order (integer-like keys are always iterated in numeric order),
    which is precisely why the list shape exists.
    """
    if isinstance(edited, dict):
        return [{"row": row_num, "cells": cells} for row_num, cells in edited.items()]
    return edited or []


def build_remaining(edited):
    """Build an ordered row-number -> cells lookup from the edited list.

    Cells are accumulated when the same row number appears more than once so
    that every edit is applied. Entries whose row number is not a valid integer
    (e.g. a blank, still-being-entered row) are skipped.
    """
    remaining = collections.OrderedDict()
    for edit in edited:
        try:
            row_num = int(edit["row"])
        except (KeyError, TypeError, ValueError):
            continue
        cells = edit.get("cells", [])
        if row_num in remaining:
            remaining[row_num] = remaining[row_num] + list(cells)
        else:
            remaining[row_num] = list(cells)
    return remaining


def process_resource(rows, remaining, missing_values):
    row_counter = 0
    for row in rows:
        row_counter += 1
        try:
            if row_counter in remaining:
                edited_cells = remaining.pop(row_counter)
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
    if len(remaining.keys()):
        raise Exception(
            f"Passed in row numbers that were not used ({str(list(remaining.keys()))}) to the edit_cells processor."
        )


def edit_cells(edited, resources=None):
    edited = normalize_edited(edited)
    # Shared across matching resources and consumed as rows are edited, mirroring
    # the previous behavior where edits were popped as they were applied.
    remaining = build_remaining(edited)

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                package_field_names = {f["name"] for f in resource["schema"]["fields"]}
                for edit in edited:
                    for cell in edit.get("cells", []):
                        field = cell.get("field")
                        if field and field not in package_field_names:
                            raise Exception(
                                f'Field "{field}" not found in resource "{resource["name"]}". '
                                f'Available fields: {sorted(package_field_names)}'
                            )
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                missing_values = get_missing_values(rows.res)
                yield process_resource(
                    rows,
                    remaining,
                    missing_values,
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        edit_cells(
            parameters.get("edited", []),
            resources=parameters.get("resources"),
        )
    )
