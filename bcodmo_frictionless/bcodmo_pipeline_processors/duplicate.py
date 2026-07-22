import copy
import os

from kvfile import KVFile

from dataflows import Flow
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    KVFileBuildProgress,
)


# Number of entries the duplicate's in-memory KVFile cache holds before it starts
# spilling to (and re-reading from) the on-disk SQLite/LevelDB store. The kvfile
# default is only 10,240, which is tiny: duplicate buffers *every* row of the
# source resource into the KVFile (keyed by row index) before replaying it, so
# any resource with more rows than this falls off a cliff into per-row SQLite
# queries + pickle (de)serialization, pegging a core while the pipeline appears
# stalled. Each cached entry is on the order of ~1KB, so the default below trades
# ~1GB of RAM (only if actually reached) for keeping the whole buffer in memory.
# Tune with the KVFILE_CACHE_SIZE env var.
KVFILE_CACHE_SIZE = int(os.environ.get("KVFILE_CACHE_SIZE", 1_000_000))


def saver(resource, db, batch_size, cache_id=None):
    # Buffering every source row into the KVFile is a blocking step; publish the
    # number of rows buffered so far so the frontend can see it building up (and
    # that it's alive vs stalled).
    progress = KVFileBuildProgress(cache_id, resource.res.name, "duplicate")
    gen = db.insert_generator(
        (('{:08x}'.format(idx), row)
         for idx, row
         in enumerate(resource)),
        batch_size=batch_size
    )
    for count, (_, row) in enumerate(gen, start=1):
        progress.update(count)
        yield row
    progress.finish()


def loader(db):
    for _, value in db.items():
        yield value
    db.close()


def duplicate(
    source=None,
    target_name=None,
    target_path=None,
    batch_size=1000,
    duplicate_to_end=False,
    cache_id=None,
):
    def func(package):
        source_, target_name_, target_path_ = source, target_name, target_path
        if source_ is None:
            source_ = package.pkg.descriptor['resources'][0]['name']
        if target_name_ is None:
            target_name_ = source_ + '_copy'
        if target_path is None:
            target_path_ = target_name_ + '.csv'

        def traverse_resources(resources):
            new_res_list = []
            for res in resources:
                yield res
                if res['name'] == source_:
                    res = copy.deepcopy(res)
                    res['name'] = target_name_
                    res['path'] = target_path_
                    if duplicate_to_end:
                        new_res_list.append(res)
                    else:
                        yield res
            for res in new_res_list:
                yield res

        descriptor = package.pkg.descriptor
        descriptor['resources'] = list(traverse_resources(descriptor['resources']))
        yield package.pkg

        dbs = []
        for resource in package:
            if resource.res.name == source_:
                db = KVFile(size=KVFILE_CACHE_SIZE)
                yield saver(resource, db, batch_size, cache_id=cache_id)
                if duplicate_to_end:
                    dbs.append(db)
                else:
                    yield loader(db)
            else:
                yield resource
        for db in dbs:
            yield loader(db)

    return func


def load_lazy_json(resources):
    # Source rows loaded from a checkpoint arrive as lazily-parsed json wrappers.
    # Unwrap them to their evaluated dicts as they stream, matching the behaviour
    # of the standard dataflows duplicate wrapper.
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
        load_lazy_json(parameters.get("source")),
        duplicate(
            parameters.get("source"),
            parameters.get("target-name"),
            parameters.get("target-path"),
            parameters.get("batch_size", 1000),
            parameters.get("duplicate_to_end", False),
            cache_id=parameters.get("cache_id"),
        ),
    )
