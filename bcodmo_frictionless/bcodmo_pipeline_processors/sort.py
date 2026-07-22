import re
import os
import decimal

from kvfile import KVFile
from bitstring import BitArray

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher


# Number of entries the sort's in-memory KVFile cache holds before it starts
# spilling to (and re-reading from) the on-disk SQLite/LevelDB store. The kvfile
# default is only 10,240, which is tiny: sort drains *every* row of the resource
# into the KVFile, so any resource with more rows than this falls off a cliff
# into per-row SQLite queries + pickle (de)serialization, pegging a core while
# the pipeline appears stalled. Each cached entry is on the order of ~1KB, so the
# default below trades ~1GB of RAM (only if actually reached) for keeping the
# whole sort buffer in memory. Tune with the KVFILE_CACHE_SIZE env var.
KVFILE_CACHE_SIZE = int(os.environ.get("KVFILE_CACHE_SIZE", 1_000_000))


FIELDS_RE = re.compile(r'(\{[^\}]+\})')
KEY_RE = re.compile(r'[^!:\}]+')


class KeyCalc(object):
    def __init__(self, key_spec):
        self.calculator = self.__calculator(key_spec)

    def __calculator(self, key_spec):
        if callable(key_spec):
            return key_spec
        formatters = None
        if isinstance(key_spec, str):
            formatters = FIELDS_RE.findall(key_spec)
            key_spec = [KEY_RE.findall(fmt[1:])[0] for fmt in formatters]
        if isinstance(key_spec, (list, tuple)):
            def func(row):
                ret = ''
                for i, key in enumerate(key_spec):
                    value = row[key]
                    # numbers
                    # https://www.h-schmidt.net/FloatConverter/IEEE754.html
                    raw = not formatters or formatters[i] == '{' + key + '}'
                    if raw and isinstance(value, (int, float, decimal.Decimal)):
                        bits = BitArray(float=value, length=64)
                        # invert the sign bit
                        bits.invert(0)
                        # invert negative numbers
                        if value < 0:
                            bits.invert(range(1, 64))
                        value = bits.hex
                    if formatters:
                        ret += formatters[i].format(**{key: value})
                    else:
                        ret += str(value)
                return ret
            return func
        assert False, 'key should be either a format string or a row->string callable'

    def __call__(self, row):
        return self.calculator(row)


def _sorter(rows, key_calc, reverse, batch_size):
    db = KVFile(size=KVFILE_CACHE_SIZE)

    def process(rows):
        for row_num, row in enumerate(rows):
            key = key_calc(row) + '{:08x}'.format(row_num)
            yield (key, row)

    db.insert(process(rows), batch_size=batch_size)
    for _, value in db.items(reverse=reverse):
        yield value
    db.close()


def sort_rows(key, resources=None, reverse=False, batch_size=1000):
    key_calc = KeyCalc(key)

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield _sorter(rows, key_calc, reverse, batch_size)
            else:
                yield rows

    return func


def load_lazy_json(resources):
    # Source rows loaded from a checkpoint arrive as lazily-parsed json wrappers.
    # Unwrap them to their evaluated dicts as they stream, matching the behaviour
    # of the standard dataflows sort wrapper.
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield (
                    row.inner if hasattr(row, "inner") else row for row in rows
                )
            else:
                yield rows

    return func


def flow(parameters):
    return Flow(
        load_lazy_json(parameters.get("resources")),
        sort_rows(
            parameters["sort-by"],
            resources=parameters.get("resources"),
            reverse=parameters.get("reverse"),
        ),
    )
