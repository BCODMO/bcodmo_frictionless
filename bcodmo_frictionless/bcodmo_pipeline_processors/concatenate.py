import logging
import sys
import itertools
import os

from dataflows import Flow, update_resource
from dataflows.helpers.resource_matcher import ResourceMatcher

from bcodmo_frictionless.bcodmo_pipeline_processors.helper import (
    get_redis_progress_key,
    get_redis_connection,
    get_redis_progress_resource_key,
    REDIS_PROGRESS_DELETED_FLAG,
    REDIS_EXPIRES,
)

PROP_STREAMED_FROM = "dpp:streamedFrom"
PROP_STREAMING = "dpp:streaming"


def concatenator(resources, all_target_fields, field_mapping, include_source_names):
    for resource_ in resources:
        res_name = resource_.res.name
        if "dpp:streamedFrom" not in resource_.res._Resource__current_descriptor:
            path_name = None
            file_name = None
            logging.warn(
                "Concatenating a resource with no dpp:streamedFrom so the path name will be empty"
            )
        else:
            path_name = resource_.res._Resource__current_descriptor["dpp:streamedFrom"]
            file_name = os.path.basename(path_name)
        row_counter = 0
        for row in resource_:
            row_counter += 1
            try:
                processed = dict((k, "") for k in all_target_fields)
                values = [
                    (field_mapping[k], v)
                    for (k, v) in row.items()
                    if k in field_mapping
                ]
                if len(values) == 0:
                    raise Exception(
                        f"The resource {res_name} had no values to be concatenated"
                    )
                processed.update(dict(values))
                for source in include_source_names:
                    if source["type"] == "resource":
                        processed[source["column_name"]] = res_name
                    if source["type"] == "path":
                        processed[source["column_name"]] = path_name
                    if source["type"] == "file":
                        processed[source["column_name"]] = file_name
                yield processed
            except Exception as e:
                raise type(e)(str(e) + f" at row {row_counter}").with_traceback(
                    sys.exc_info()[2]
                )


def concatenate(
    fields,
    target={},
    resources=None,
    include_source_names=[],
    missing_values=[],
    cache_id=None,
):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        resource_names = [res["name"] for res in package.pkg.descriptor["resources"]]
        if not any(matcher.match(name) for name in resource_names):
            raise Exception(
                f'Source resource pattern {resources} did not match any resources in datapackage. '
                f'Available resources: {resource_names}'
            )
        # Prepare target resource
        if "name" not in target:
            target["name"] = "concat"
        if not target["name"]:
            raise Exception("The concatenate target name cannot be empty")
        if "path" not in target:
            target["path"] = "data/" + target["name"] + ".csv"
        target.update(
            dict(
                mediatype="text/csv",
                schema=dict(fields=[], primaryKey=[]),
            )
        )

        # Create mapping between source field names to target field names
        field_mapping = {}
        for target_field, source_fields in fields.items():
            if source_fields is not None:
                for source_field in source_fields:
                    if source_field in field_mapping:
                        raise RuntimeError(
                            "Duplicate appearance of %s (%r)"
                            % (source_field, field_mapping)
                        )
                    field_mapping[source_field] = target_field

            if target_field in field_mapping:
                raise RuntimeError("Duplicate appearance of %s" % target_field)

            field_mapping[target_field] = target_field

        # Create the schema for the target resource
        needed_fields = sorted(fields.keys())
        for source in include_source_names:
            if source["column_name"] in needed_fields:
                raise Exception(
                    f"source_field_name \"{source['column_name']}\" field name already exists"
                )
        for resource in package.pkg.descriptor["resources"]:
            if not matcher.match(resource["name"]):
                continue

            schema = resource.get("schema", {})
            pk = schema.get("primaryKey", [])
            for field in schema.get("fields", []):
                orig_name = field["name"]
                if orig_name in field_mapping:
                    name = field_mapping[orig_name]
                    if name not in needed_fields:
                        continue
                    if orig_name in pk:
                        target["schema"]["primaryKey"].append(name)
                    target["schema"]["fields"].append(field)
                    field["name"] = name
                    needed_fields.remove(name)
            target["schema"]["missingValues"] = missing_values
        if len(target["schema"]["primaryKey"]) == 0:
            del target["schema"]["primaryKey"]

        for name in needed_fields:
            target["schema"]["fields"].append(dict(name=name, type="string"))

        for source in include_source_names:
            target["schema"]["fields"].append(
                {
                    "name": source["column_name"],
                    "type": "string",
                }
            )

        # Update resources in datapackage (make sure they are consecutive)
        prefix = True
        suffix = False
        redis_conn = None
        if cache_id is not None:
            redis_conn = get_redis_connection()
        num_concatenated = 0
        new_resources = []
        for resource in package.pkg.descriptor["resources"]:
            name = resource["name"]
            if name == target["name"]:
                raise Exception(
                    f'Name of concatenate target ({target["name"]}) cannot match an existing resource name ({name})'
                )
            match = matcher.match(name)
            if prefix:
                if match:
                    prefix = False
                    num_concatenated += 1
                    if redis_conn is not None:
                        progress_key = get_redis_progress_key(name, cache_id)
                        redis_conn.set(
                            progress_key, REDIS_PROGRESS_DELETED_FLAG, ex=REDIS_EXPIRES
                        )

                else:
                    new_resources.append(resource)
            elif suffix:
                if match:
                    raise Exception(
                        "Concatenated resources must be appear in consecutive order in the datapackage"
                    )
                new_resources.append(resource)
            else:
                if not match:
                    suffix = True
                    new_resources.append(target)
                    new_resources.append(resource)
                else:
                    num_concatenated += 1
                    if redis_conn is not None:
                        progress_key = get_redis_progress_key(name, cache_id)
                        redis_conn.set(
                            progress_key, REDIS_PROGRESS_DELETED_FLAG, ex=REDIS_EXPIRES
                        )

        if not suffix:
            new_resources.append(target)

        if redis_conn is not None:
            redis_key = get_redis_progress_resource_key(cache_id)
            redis_conn.sadd(
                redis_key,
                target["name"],
            )
            redis_conn.expire(redis_key, REDIS_EXPIRES)
        package.pkg.descriptor["resources"] = new_resources
        yield package.pkg

        needed_fields = sorted(fields.keys())
        it = iter(package)
        for resource in it:
            if matcher.match(resource.res.name):
                resource_chain = itertools.chain(
                    [resource], itertools.islice(it, num_concatenated - 1)
                )
                yield concatenator(
                    resource_chain,
                    needed_fields,
                    field_mapping,
                    include_source_names,
                )
            else:
                yield resource

    return func


def flow(parameters):
    # Support deprecated include_source_name
    include_source_names = parameters.get("include_source_names", [])
    if parameters.get("include_source_name", False) and not len(include_source_names):
        include_source_names = [
            {
                "type": parameters.get("include_source_name", False),
                "column_name": parameters.get("source_field_name", "source_name"),
            }
        ]

    return Flow(
        concatenate(
            parameters.get("fields", {}),
            parameters.get("target", {}),
            parameters.get("sources"),
            include_source_names,
            parameters.get("missing_values", []),
            parameters.get("cache_id", None),
        ),
        update_resource(
            parameters.get("target", {}).get("name", "concat"),
            **{
                PROP_STREAMING: True,
            },
        ),
    )
