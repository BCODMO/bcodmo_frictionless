from dataflows import Flow, PackageWrapper, schema_validator
from dataflows.helpers.resource_matcher import ResourceMatcher
import re


def set_types(parameters, resources=None, regex=None, types={}):
    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                fields = resource["schema"]["fields"]
                field_names = [f["name"] for f in fields]
                for name, options in types.items():
                    if not regex:
                        pattern = re.compile(f"^{re.escape(name)}$")
                    else:
                        pattern = re.compile(f"^{name}$")
                    if not any(pattern.match(f) for f in field_names):
                        raise Exception(
                            f'Type pattern "{name}" did not match any fields in resource "{resource["name"]}". '
                            f'Available fields: {sorted(field_names)}'
                        )
                    for field in fields:
                        if pattern.match(field["name"]):
                            field.update(options)

        yield package.pkg
        for rows in package:
            if matcher.match(rows.res.name):
                yield schema_validator(rows.res, rows)
            else:
                yield rows
        yield from package

    return func


def flow(parameters):
    resources = parameters.get("resources", None)
    regex = parameters.get("regex", True)
    types = parameters.get("types", {})
    return Flow(set_types(parameters, resources=resources, regex=regex, types=types))
