# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import csv
import math
import six
from itertools import chain
from codecs import iterencode
from tabulator.parser import Parser
from tabulator import helpers, config
import pandas as pd
import logging


# Module API

class FixedWidthParser(Parser):
    """Parser to parse FixedWidth data format.
    """

    # Public

    options = [
        'width',
        'infer',
    ]

    def __init__(self, loader, force_parse=False, width=None, infer=None):
        self.__loader = loader
        self.__width = width
        self.__infer = infer
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
        self.__encoding = getattr(self.__chars, 'encoding', encoding)
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
                'width is a required parameter for fixedwidth format if infer is not set'
            )
        items = self.__chars
        if self.__infer:

            reader = pd.read_fwf(
                items,
                colspecs='infer',
                chunksize=2,
            )
        else:
            reader = pd.read_fwf(
                items,
                widths=width,
                chunksize=2,
            )
        for chunk in reader:
            for index, row in chunk.iterrows():
                if index == 0:
                    yield (1, None, list(chunk))
                l = row.tolist()
                l = [str(item) for item in l]
                yield (index + 2, None, l)
