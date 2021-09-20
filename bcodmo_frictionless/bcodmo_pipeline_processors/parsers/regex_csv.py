# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import six
from itertools import chain
from codecs import iterencode
from tabulator.parser import Parser
from tabulator import helpers
import pandas as pd
import re


# Module API


class RegexCSVParser(Parser):
    """Parser to parse regex-delimited CSV data format."""

    options = [
        "stream",
        "delimiter",
        "capture_skipped_rows",
        "capture_skipped_rows_join_string",
        "capture_skipped_rows_join",
    ]

    def __init__(self, loader, force_parse=False, **options):
        # Make bytes
        if six.PY2:
            for key, value in options.items():
                if isinstance(value, six.string_types):
                    options[key] = str(value)

        # Set attributes
        self.__loader = loader
        self.__stream = options.get("stream", None)
        self.__delimiter = options.get("delimiter", ",")
        self.__capture_skipped_rows = options.get("capture_skipped_rows", None)
        self.__capture_skipped_rows_join = options.get(
            "capture_skipped_rows_join", True
        )
        self.__capture_skipped_rows_join_string = options.get(
            "capture_skipped_rows_join_string", ";"
        )
        self.__force_parse = force_parse
        self.__chars = None

    @property
    def closed(self):
        return self.__chars is None or self.__chars.closed

    def open(self, source, encoding=None):
        self.close()
        self.__chars = self.__loader.load(source, encoding=encoding)
        self.__encoding = getattr(self.__chars, "encoding", encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__chars.close()

    def reset(self):
        helpers.reset_stream(self.__chars)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    def _is_data_row(self, row, row_number):
        headers_row = self.__stream._Stream__headers_row or 1
        return not self.__stream._Stream__check_if_row_for_skipping(
            row_number, None, row
        ) and (headers_row < row_number)

    def __iter_extended_rows(self):

        headers_row = self.__stream._Stream__headers_row or 1
        print("HEADERS ROW IS", headers_row)
        items = self.__chars
        captured_rows_dict = {}
        if self.__capture_skipped_rows:
            for row_number, item in enumerate(iter(items.readline, "")):
                # If we're not in a comment anymore (as long as we are past the header row)
                if self._is_data_row([item], row_number + 1):
                    break
                for c in self.__capture_skipped_rows:
                    match = re.match(c["regex"], item)
                    if match:
                        if not len(match.groups()):
                            continue
                        column_name = c["column_name"]
                        if column_name not in captured_rows_dict:
                            captured_rows_dict[column_name] = []
                        captured_rows_dict[column_name].append(match.groups()[0])

        items.seek(0)
        print(captured_rows_dict)
        captured_rows = []
        for header_name, v in captured_rows_dict.items():
            if self.__capture_skipped_rows_join:
                captured_rows.append(
                    {
                        "name": header_name,
                        "value": self.__capture_skipped_rows_join_string.join(v),
                    }
                )
            else:
                for value in v:
                    captured_rows.append(
                        {
                            "name": header_name,
                            "value": value,
                        }
                    )
        # For PY2 encode/decode
        if six.PY2:
            # Reader requires utf-8 encoded stream
            bytes = iterencode(items, "utf-8")

            reader = pd.read_csv(
                bytes,
                sep=self.__delimiter,
                engine="python",
                chunksize=2,
                header=None,
                dtype=str,
                index_col=False,
            )
            index_offset = 0
            for chunk in reader:
                for index, row in chunk.iterrows():
                    actual_index = index + 1 + index_offset
                    l = row.tolist()
                    l = [str(item) for item in l]
                    for captured_row in captured_rows:
                        # Append the name if it's not a data row (either header or to be skipped)
                        if not self._is_data_row(l, actual_index):
                            l.append(captured_row["name"])
                        else:
                            l.append(captured_row["value"])
                    yield (actual_index, None, l)

        # For PY3 use chars
        else:
            reader = pd.read_csv(
                items,
                sep=self.__delimiter,
                engine="python",
                chunksize=2,
                header=None,
                dtype=str,
                index_col=False,
            )
            index_offset = 0
            for chunk in reader:
                for index, row in chunk.iterrows():
                    actual_index = index + 1 + index_offset
                    l = row.tolist()
                    l = [str(item) for item in l]
                    for captured_row in captured_rows:
                        # Append the name if it's not a data row (either header or to be skipped)
                        if not self._is_data_row(l, actual_index):
                            l.append(captured_row["name"])
                        else:
                            l.append(captured_row["value"])
                    print("yielding", actual_index, l)
                    yield (actual_index, None, l)
