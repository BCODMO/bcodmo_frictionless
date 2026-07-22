import re
import copy
import os
import time
import logging
import warnings
import collections

from kvfile import KVFile

from dataflows import Flow, PackageWrapper, update_resource
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_progress_resource_key,
    get_redis_progress_join_key,
    get_redis_connection,
    REDIS_PROGRESS_JOINING_FLAG,
    REDIS_EXPIRES,
)


log = logging.getLogger(__name__)

# Number of entries the join's in-memory KVFile cache holds before it starts
# spilling to (and re-reading from) the on-disk SQLite/LevelDB store. The kvfile
# default is only 10,240, which is tiny: any join whose source has more distinct
# keys than this falls off a cliff into per-row SQLite queries + pickle
# (de)serialization, pegging a core while the pipeline appears stalled. Each
# cached entry is on the order of ~1KB, so the default below trades ~1GB of RAM
# (only if actually reached) for keeping the whole index in memory. Tune with
# the KVFILE_CACHE_SIZE env var.
KVFILE_CACHE_SIZE = int(os.environ.get("KVFILE_CACHE_SIZE", 1_000_000))

# How often (seconds) to publish join key-building progress to redis. Matches the
# throttle used by dump_to_s3 so we don't hammer redis on every row.
PROGRESS_THROTTLE = 0.75


PROP_STREAMING = "dpp:streaming"


# DB Helper
class KeyCalc(object):

    def __init__(self, key_spec):
        if isinstance(key_spec, list):
            key_list = key_spec
            key_spec = ':'.join('{%s}' % key for key in key_spec)
        else:
            key_list = re.findall(r'\{(.*?)\}', key_spec)
        self.key_spec = key_spec
        self.key_list = key_list

    def __call__(self, row, row_number):
        return self.key_spec.format(**{**row, '#': row_number})


# Aggregator helpers
def identity(x):
    return x


def median(values):
    if values is None:
        return None
    ll = len(values)
    mid = int(ll/2)
    values = sorted(values)
    if ll % 2 == 0:
        return (values[mid - 1] + values[mid])/2
    else:
        return values[mid]


def update_counter(curr, new):
    if new is None:
        return curr
    if curr is None:
        curr = collections.Counter()
    if isinstance(new, str):
        new = [new]
    if not isinstance(curr, collections.Counter):
        curr = collections.Counter(curr)
    curr.update(new)
    return curr


# Aggregators
Aggregator = collections.namedtuple('Aggregator',
                                    ['func', 'finaliser', 'dataType', 'copyProperties'])
AGGREGATORS = {
    'sum': Aggregator(lambda curr, new:
                      new + curr if curr is not None else new,
                      identity,
                      None,
                      False),
    'avg': Aggregator(lambda curr, new:
                      (curr[0] + 1, new + curr[1])
                      if curr is not None
                      else (1, new),
                      lambda value: value[1] / value[0],
                      None,
                      False),
    'median': Aggregator(lambda curr, new:
                         curr + [new] if curr is not None else [new],
                         median,
                         None,
                         True),
    'max': Aggregator(lambda curr, new:
                      max(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'min': Aggregator(lambda curr, new:
                      min(new, curr) if curr is not None else new,
                      identity,
                      None,
                      False),
    'first': Aggregator(lambda curr, new:
                        curr if curr is not None else new,
                        identity,
                        None,
                        True),
    'last': Aggregator(lambda curr, new: new,
                       identity,
                       None,
                       True),
    'count': Aggregator(lambda curr, new:
                        curr+1 if curr is not None else 1,
                        identity,
                        'integer',
                        False),
    'any': Aggregator(lambda curr, new: new,
                      identity,
                      None,
                      True),
    'set': Aggregator(lambda curr, new:
                      curr.union({new}) if curr is not None else {new},
                      lambda value: list(value) if value is not None else [],
                      'array',
                      False),
    'array': Aggregator(lambda curr, new:
                        curr + [new] if curr is not None else [new],
                        lambda value: value if value is not None else [],
                        'array',
                        False),
    'counters': Aggregator(lambda curr, new:
                           update_counter(curr, new),
                           lambda value:
                           list(collections.Counter(value).most_common()) if value is not None else [],
                           'array',
                           False),
}


# Input helpers

def fix_fields(fields):
    for field in sorted(fields.keys()):
        spec = fields[field]
        if spec is None:
            fields[field] = spec = {}
        if 'name' not in spec:
            spec['name'] = field
        if 'aggregate' not in spec:
            spec['aggregate'] = 'any'
    return fields


def expand_fields(fields, schema_fields):
    if '*' in fields:
        existing_names = set(f['name'] for f in fields.values())
        spec = fields.pop('*')
        for sf in schema_fields:
            sf_name = sf['name']
            if sf_name not in existing_names:
                fields[sf_name] = copy.deepcopy(spec)
                fields[sf_name]['name'] = sf_name


def order_fields(fields, schema_fields):
    ordered_fields = collections.OrderedDict()
    for descriptor in schema_fields:
        name = descriptor['name']
        if name in fields:
            ordered_fields[name] = fields.pop(name)
    for name in sorted(fields.keys()):
        ordered_fields[name] = fields[name]
    return ordered_fields


def concatenator(resources, all_target_fields, field_mapping):
    for resource_ in resources:
        for row in resource_:
            processed = dict((k, '') for k in all_target_fields)
            values = [(field_mapping[k], v) for (k, v)
                    in row.items()
                    if k in field_mapping]
            assert len(values) > 0
            processed.update(dict(values))
            yield processed


def load_lazy_json(resources):
    # Source rows loaded from a checkpoint arrive as lazily-parsed json wrappers.
    # Unwrap them to their evaluated dicts as they stream, matching the behaviour
    # of the standard dataflows join wrapper.
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


def join_aux(source_name, source_key, source_delete,  # noqa: C901
             target_name, target_key, fields, full, mode, cache_id=None):

    deduplication = target_key is None
    fields = fix_fields(fields)
    source_key = KeyCalc(source_key)
    target_key = KeyCalc(target_key) if target_key is not None else target_key
    # We will store db keys as boolean flags:
    # - False -> inserted/not used
    # - True -> inserted/used
    db_keys_usage = KVFile(size=KVFILE_CACHE_SIZE)
    db = KVFile(size=KVFILE_CACHE_SIZE)

    # Mode of join operation
    if full is not None:
        warnings.warn(
            'For the `join` processor the `full=True` flag is deprecated. '
            'Please use the "mode" parameter instead.',
            UserWarning)
        mode = 'half-outer' if full else 'inner'
    assert mode in ['inner', 'half-outer', 'full-outer']

    # Indexes the source data
    def indexer(resource):
        # Building the index for a join is a blocking step: the entire source
        # resource is drained into the KVFile before any target rows can flow
        # downstream. While that happens no other processor reports progress, so
        # we publish the number of distinct keys built so far to redis. The front
        # end reads this to show the join "building up" (and that it's alive).
        num_keys = 0
        redis_conn = get_redis_connection() if cache_id else None
        join_key = None
        print(
            f"[BCODMO JOIN] indexer start: source={source_name!r} "
            f"cache_id={cache_id!r} "
            f"redis_progress_url={'set' if os.environ.get('REDIS_PROGRESS_URL') else 'MISSING'} "
            f"redis_conn={'yes' if redis_conn is not None else 'None'}",
            flush=True,
        )
        if redis_conn is not None:
            resource_set_key = get_redis_progress_resource_key(cache_id)
            redis_conn.sadd(resource_set_key, source_name)
            redis_conn.expire(resource_set_key, REDIS_EXPIRES)
            redis_conn.set(
                get_redis_progress_key(source_name, cache_id),
                REDIS_PROGRESS_JOINING_FLAG,
                ex=REDIS_EXPIRES,
            )
            join_key = get_redis_progress_join_key(source_name, cache_id)
            redis_conn.set(join_key, num_keys, ex=REDIS_EXPIRES)
            print(
                f"[BCODMO JOIN] wrote initial progress: resource_set_key={resource_set_key!r} "
                f"progress_key={get_redis_progress_key(source_name, cache_id)!r}(={REDIS_PROGRESS_JOINING_FLAG}) "
                f"join_key={join_key!r}(=0)",
                flush=True,
            )

        timer = time.time()
        for row_number, row in enumerate(resource, start=1):
            key = source_key(row, row_number)
            try:
                current = db.get(key)
            except KeyError:
                current = {}
                num_keys += 1
            for field, spec in fields.items():
                name = spec['name']
                curr = current.get(field)
                agg = spec['aggregate']
                if agg != 'count':
                    new = row.get(name)
                else:
                    new = ''
                if new is not None:
                    current[field] = AGGREGATORS[agg].func(curr, new)
                elif field not in current:
                    current[field] = None
            if mode == 'full-outer':
                current['__key__'] = [row.get(field) for field in source_key.key_list]
            db.set(key, current)
            db_keys_usage.set(key, False)
            if redis_conn is not None and time.time() - timer > PROGRESS_THROTTLE:
                redis_conn.set(join_key, num_keys, ex=REDIS_EXPIRES)
                print(
                    f"[BCODMO JOIN] progress: source={source_name!r} "
                    f"row={row_number} num_keys={num_keys} join_key={join_key!r}",
                    flush=True,
                )
                timer = time.time()
            yield row
        if redis_conn is not None:
            redis_conn.set(join_key, num_keys, ex=REDIS_EXPIRES)
        print(
            f"[BCODMO JOIN] indexer done: source={source_name!r} "
            f"total_keys={num_keys} redis_conn={'yes' if redis_conn is not None else 'None'}",
            flush=True,
        )

    # Generates the joined data
    def process_target(resource):
        if deduplication:
            # just empty the iterable
            collections.deque(indexer(resource), maxlen=0)
            for key, value in db.items():
                row = dict(
                    (f, None) for f in fields.keys()
                )
                row.update(dict(
                    (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
                    for k, v in value.items()
                ))
                yield row
        else:
            for row_number, row in enumerate(resource, start=1):
                key = target_key(row, row_number)
                try:
                    extra = create_extra_by_key(key)
                    db_keys_usage.set(key, True)
                except KeyError:
                    if mode == 'inner':
                        continue
                    extra = dict(
                        (k, row.get(k))
                        for k in fields.keys()
                    )
                row.update(extra)
                yield row
            if mode == 'full-outer':
                for key, value in db_keys_usage.items():
                    if value is False:
                        extra = create_extra_by_key(key)
                        yield extra

    # Creates extra by key
    def create_extra_by_key(key):
        extra = db.get(key)
        key = extra.pop('__key__', None)
        extra = dict(
            (k, AGGREGATORS[fields[k]['aggregate']].finaliser(v))
            for k, v in extra.items()
            if k in fields
        )
        if key:
            for k, v in zip(target_key.key_list, key):
                extra[k] = v
        return extra

    # Yields the new resources
    def new_resource_iterator(resource_iterator):
        has_index = False
        for resource in resource_iterator:
            name = resource.res.name
            if name == source_name:
                has_index = True
                if source_delete:
                    # just empty the iterable
                    collections.deque(indexer(resource), maxlen=0)
                else:
                    yield indexer(resource)
                if deduplication:
                    yield process_target(resource)
            elif name == target_name:
                assert has_index
                yield process_target(resource)
            else:
                yield resource

    # Updates / creates the target resource descriptor
    def process_target_resource(source_spec, resource):
        target_fields = \
            resource.setdefault('schema', {}).setdefault('fields', [])
        for name, spec in fields.items():
            agg = spec['aggregate']
            data_type = AGGREGATORS[agg].dataType
            copy_properties = AGGREGATORS[agg].copyProperties
            to_copy = {}
            if data_type is None:
                try:
                    source_field = \
                        next(filter(lambda f: f['name'] == spec['name'],
                                    source_spec['schema']['fields']))
                except StopIteration:
                    raise KeyError('Failed to find field with name %s in resource %s' %
                                   (spec['name'], source_spec['name']))
                if copy_properties:
                    to_copy = copy.deepcopy(source_field)
                data_type = source_field['type']
            try:
                existing_field = next(iter(filter(
                    lambda f: f['name'] == name,
                    target_fields)))
                assert existing_field['type'] == data_type, \
                    'Reusing %s but with different data types: %s != %s' % (name, existing_field['type'], data_type)
            except StopIteration:
                to_copy.update({
                    'name': name,
                    'type': data_type
                })
                target_fields.append(to_copy)
        return resource

    # Updates the datapackage descriptor based on parameters
    def process_datapackage(datapackage):

        new_resources = []
        source_spec = None

        resource_names = [resource['name'] for resource in datapackage['resources']]
        assert source_name in resource_names, \
            'Source resource ({}) not found package (target={}, found: {})'\
            .format(source_name, target_name, resource_names)
        assert target_name in resource_names, \
            'Target resource ({}) not found package (source={}, found: {})'\
            .format(target_name, source_name, resource_names)

        for resource in datapackage['resources']:

            if resource['name'] == source_name:
                nonlocal fields
                source_spec = resource
                schema_fields = source_spec.get('schema', {}).get('fields', [])
                expand_fields(fields, schema_fields)
                fields = order_fields(fields, schema_fields)
                if not source_delete:
                    new_resources.append(resource)
                if deduplication:
                    resource = process_target_resource(
                        source_spec,
                        {
                            'name': target_name,
                            'path': os.path.join('data', target_name + '.csv')
                        })
                    new_resources.append(resource)

            elif resource['name'] == target_name:
                assert isinstance(source_spec, dict),\
                       'Source resource ({}) must appear before target resource ({}), found: {}'\
                       .format(source_name, target_name, resource_names)
                resource = process_target_resource(source_spec, resource)
                new_resources.append(resource)

            else:
                new_resources.append(resource)

        datapackage['resources'] = new_resources

    def func(package: PackageWrapper):
        process_datapackage(package.pkg.descriptor)
        yield package.pkg
        yield from new_resource_iterator(package)
        db.close()
        db_keys_usage.close()

    return func


def join(source_name, source_key, target_name, target_key, fields={}, full=None, mode='half-outer', source_delete=True, cache_id=None):
    return join_aux(source_name, source_key, source_delete, target_name, target_key, fields, full, mode, cache_id=cache_id)


def flow(parameters):
    source = parameters["source"]
    target = parameters["target"]
    print(
        f"[BCODMO JOIN] flow() invoked: source={source.get('name')!r} "
        f"target={target.get('name')!r} mode={parameters.get('mode')!r} "
        f"cache_id={parameters.get('cache_id')!r} "
        f"param_keys={sorted(parameters.keys())}",
        flush=True,
    )
    return Flow(
        load_lazy_json(source["name"]),
        join(
            source["name"],
            source["key"],
            target["name"],
            target["key"],
            parameters.get("fields", {}),
            parameters.get("full", None),
            parameters.get("mode", "half-outer"),
            source.get("delete", False),
            cache_id=parameters.get("cache_id"),
        ),
        update_resource(target["name"], **{PROP_STREAMING: True}),
    )
