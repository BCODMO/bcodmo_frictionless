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

    options = ["delimiter", "capture_skipped_rows", "capture_skipped_rows_join"]

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
        self.__capture_skipped_rows_join = options.get("capture_skipped_rows_join", ";")
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

    def __iter_extended_rows(self):

        items = self.__chars
        captured_rows_dict = {}
        if self.__capture_skipped_rows:
            for row_number, item in enumerate(iter(items.readline, "")):
                for c in self.__capture_skipped_rows:
                    match = re.match(c["regex"], item)
                    if match:
                        column_name = c["column_name"]
                        if column_name not in captured_rows_dict:
                            captured_rows_dict[column_name] = []
                        captured_rows_dict[column_name].append(match.groups()[0])

                # If we're not in a comment anymore
                if not self.__stream._Stream__check_if_row_for_skipping(
                    row_number, None, [item]
                ):
                    break
        captured_rows = []
        for header_name, v in captured_rows_dict.items():
            captured_rows.append(
                {
                    "name": header_name,
                    "value": self.__capture_skipped_rows_join.join(v),
                }
            )
        # For PY2 encode/decode
        if six.PY2:
            # Reader requires utf-8 encoded stream
            bytes = iterencode(self.__chars, "utf-8")

            reader = pd.read_csv(
                bytes, sep=self.__delimiter, engine="python", chunksize=2, header=None
            )
            index_offset = 0
            for chunk in reader:
                for index, row in chunk.iterrows():
                    actual_index = index + 1 + index_offset
                    l = row.tolist()
                    l = [str(item) for item in l]
                    for captured_row in captured_rows:
                        if actual_index == 1:
                            l.append(captured_row["name"])
                        else:
                            l.append(captured_row["value"])
                    yield (actual_index, None, l)

        # For PY3 use chars
        else:
            reader = pd.read_csv(
                self.__chars,
                sep=self.__delimiter,
                engine="python",
                chunksize=2,
                header=None,
            )
            index_offset = 0
            for chunk in reader:
                for index, row in chunk.iterrows():
                    actual_index = index + 1 + index_offset
                    l = row.tolist()
                    l = [str(item) for item in l]
                    for captured_row in captured_rows:
                        if actual_index == 1:
                            l.append(captured_row["name"])
                        else:
                            l.append(captured_row["value"])
                    yield (actual_index, None, l)
