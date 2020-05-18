# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import math
import six
import re
from itertools import chain
from codecs import iterencode
from tabulator.parser import Parser
from tabulator import helpers, config, exceptions
import pandas as pd
import logging


# Module API


class FixedWidthParser(Parser):
    """Parser to parse FixedWidth data format.
    """

    # Public

    options = [
        "width",
        "infer",
        "parse_seabird_header",
        "fixedwidth_skip_header",
        "fixedwidth_sample_size",
    ]

    def __init__(
        self,
        loader,
        force_parse=False,
        width=None,
        infer=None,
        parse_seabird_header=False,
        fixedwidth_skip_header=[],
        fixedwidth_sample_size=100,
    ):
        self.__loader = loader
        self.__width = width
        self.__infer = infer
        self.__parse_seabird_header = parse_seabird_header
        # A list of strings that will be used to determine what is a comment at the top of the file
        self.__fixedwidth_skip_header = fixedwidth_skip_header
        # Ensure that # is included in seabird files, because that's how we will parse the header
        if parse_seabird_header and "#" not in self.__fixedwidth_skip_header:
            self.__fixedwidth_skip_header.append("#")
        # Sample size for the pandas fixed width parser
        self.__fixedwidth_sample_size = fixedwidth_sample_size
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
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

    # Private

    def __iter_extended_rows(self):
        width = self.__width
        if width is None and not self.__infer:
            raise exceptions.TabulatorException(
                "width is a required parameter for fixedwidth format if infer is not set"
            )
        items = self.__chars
        last_item = None
        file_pos = None
        header_values = []
        for item in iter(items.readline, ""):
            last_item = item
            if self.__parse_seabird_header:
                match = re.match("^# name \d* = (.*):.*$", item)
                if match:
                    header_values.append(match.groups()[0])
            is_comment = False
            for skip_str in self.__fixedwidth_skip_header:
                if item.startswith(skip_str):
                    is_comment = True

            if not is_comment:
                break
            file_pos = items.tell()

        # Set the header value to the parsed result
        if self.__parse_seabird_header:
            if not self.__infer and len(width) != len(header_values):
                raise exceptions.TabulatorException(
                    f"The inferred header is of length {len(header_values)} but there are {len(width)} width values"
                )
            # Yield the header value as the first row
            yield (1, None, header_values)

        # Set stream back to previous value
        if file_pos:
            items.seek(file_pos)
        else:
            items.seek(0)

        if self.__infer:
            reader = pd.read_fwf(
                items,
                colspecs="infer",
                infer_nrows=self.__fixedwidth_sample_size,
                chunksize=2,
                dtype=str,
                header=None,
            )
        else:
            reader = pd.read_fwf(
                items,
                widths=width,
                infer_nrows=self.__fixedwidth_sample_size,
                chunksize=2,
                dtype=str,
                header=None,
            )
        index_offset = 0
        if self.__parse_seabird_header:
            index_offset = 1
        for chunk in reader:
            for index, row in chunk.iterrows():
                l = row.tolist()
                l = [str(item) for item in l]
                yield (index + 1 + index_offset, None, l)
