from dataflows import Flow, PackageWrapper, schema_validator
from dataflows.helpers.resource_matcher import ResourceMatcher
import re


def set_types(parameters, resources=None, regex=None, types={}):
    def func(package: PackageWrapper):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor["resources"]:
            if matcher.match(resource["name"]):
                fields = resource["schema"]["fields"]
                for name, options in types.items():
                    if not regex:
                        name = re.escape(name)
                    name = re.compile(f"^{name}$")
                    for field in fields:
                        if name.match(field["name"]):
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
