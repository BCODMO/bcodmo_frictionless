"""Key-name validation of Laminar pipeline steps.

Every place that executes a pipeline step installs this package (laminar_server's
workers and request handlers, the laminar-pipeline-run Lambda, the
file_statistics Lambda), so this is where the one machine-readable description
of each processor's parameters lives: ``schemas/<processor>.json``, a JSON
Schema that names the keys a step may carry at every level, plus
``schemas/_step.json`` for the step envelope (``run``, ``parameters``, the
annotation keys laminar_web and the LLM tooling add).

Key names are checked everywhere, plus the few constraints a schema declares
(``type``, ``required``, ``minItems``, ``maxItems``; see below). Pipelines are
never hand-edited; an unknown key comes from an LLM inventing one, and until
now it passed through silently because most processors read their parameters
with ``.get()`` and ignore the rest. Constraints are declared only where a
shape the processor accepts is one the rest of the system cannot handle:
``update_fields.resources`` must list exactly one resource, because the
laminar editor shows one resource per step.

Schema conventions (a subset of JSON Schema draft-07):

* ``properties`` lists the keys allowed at a level; ``additionalProperties:
  false`` rejects any other key.
* ``additionalProperties: true`` makes a level an open map (``update_package``
  takes arbitrary metadata); ``additionalProperties: <schema>`` makes it a map
  whose keys are data and whose values are checked against the schema
  (``update_fields.fields`` is keyed by column name).
* ``items`` is the schema of an array's elements.
* ``deprecated: true`` on a property accepts the key but never suggests or
  lists it.
* ``required`` on an object is enforced. ``minItems`` / ``maxItems`` on a
  property make it a constrained array: it must then be a list of that
  length, and its ``items.type`` (when declared) is checked. A scalar
  ``type`` (``string``, ``number``, ``integer``, ``boolean``) on a property
  is enforced too: ``add_computed_field.fields[].target`` must be a string
  because laminar's step editor shows it in a text input. ``object`` and
  ``array`` stay documentation only, as they always were (a list where an
  object is expected is the processor's error to report); everything else
  in JSON Schema is ignored.

Usage::

    from bcodmo_frictionless.step_schema import validate_steps, StepSchemaError
    try:
        validate_steps(steps)
    except StepSchemaError as e:
        print(e)   # every problem in every step, one paragraph each
"""

from __future__ import annotations

import difflib
import json
import os
import re
from functools import lru_cache
from typing import Iterator

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas")
ENVELOPE_NAME = "_step"


class StepSchemaError(Exception):
    """One or more steps carry keys their processors do not know.

    ``problems`` is the list of individual findings (``Problem``); ``str(e)``
    is the report the pipeline runner shows, all problems together, so the
    author (or the model) can fix everything in one pass.
    """

    def __init__(self, problems):
        self.problems = list(problems)
        super().__init__(format_problems(self.problems))


UNKNOWN_KEY = "unknown_key"
CONSTRAINT = "constraint"


class Problem:
    """One finding: an unknown key (where it is, what it might have meant,
    what is valid) or a constraint a declared key breaks (``kind`` is
    ``CONSTRAINT`` and ``message`` says what; ``key`` is the offending
    property, ``path`` its parent)."""

    def __init__(self, step_index, run, path, key, suggestions, valid_keys, kind=UNKNOWN_KEY, message=None):
        self.step_index = step_index
        self.run = run
        self.path = path
        self.key = key
        self.suggestions = suggestions
        self.valid_keys = valid_keys
        self.kind = kind
        self.message = message

    @property
    def step_label(self):
        return f"step {self.step_index + 1} ({self.run})"

    def __str__(self):
        if self.kind == CONSTRAINT:
            return f"{self.step_label}: {self.message} at {self.path}"
        lines = [f'{self.step_label}: unknown key "{self.key}" at {self.path}']
        for s in self.suggestions:
            lines.append(f"  {s}")
        if self.valid_keys:
            lines.append(f"  valid keys here: {', '.join(self.valid_keys)}")
        else:
            lines.append("  no keys are valid here")
        return "\n".join(lines)

    def as_dict(self):
        return {
            "step_index": self.step_index,
            "run": self.run,
            "path": self.path,
            "key": self.key,
            "suggestions": list(self.suggestions),
            "valid_keys": list(self.valid_keys),
            "kind": self.kind,
            "message": self.message,
        }


def format_problems(problems):
    return "\n".join(str(p) for p in problems)


@lru_cache(maxsize=None)
def schema_names():
    """Every processor with a schema (the envelope excluded)."""
    names = []
    for filename in sorted(os.listdir(SCHEMA_DIR)):
        if filename.endswith(".json") and not filename.startswith("_"):
            names.append(filename[: -len(".json")])
    return tuple(names)


@lru_cache(maxsize=None)
def load_schema(name):
    """The schema of a processor (or ``_step`` for the envelope); None when
    the processor has no schema."""
    path = os.path.join(SCHEMA_DIR, name + ".json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def has_schema(run):
    return load_schema(run) is not None


# Aliases for keys that are not lexically close to the right one but mean the
# same thing. Seeded from what the historical audit found; extend as new
# misses appear.
ALIASES = {
    "columns": "fields",
    "column": "field",
    "resource": "resources",
    "field_names": "fields",
    "standard_name": "standard_name_id",
    "parameter_id": "standard_name_id",
    "units_supplied": "supplied_units",
    "bcodmo": "bcodmo:",
}


def _normalize(key):
    """Fold case, separators and camelCase: standardNameId -> standard_name_id,
    and the bcodmo / bcodmo: slip."""
    key = key.rstrip(":")
    key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    key = key.replace("-", "_").lower()
    return key


def _suggest(key, valid_keys, elsewhere):
    """Suggestions for an unknown key, in order of strength.

    1. Normalized equality (case, _ / -, camelCase, bcodmo vs bcodmo:).
    2. Misplacement: the key is valid elsewhere in the same step.
    3. An alias from ALIASES.
    4. Close matches by string similarity.
    """
    out = []
    norm = _normalize(key)
    for v in valid_keys:
        if _normalize(v) == norm and v != key:
            out.append(f'did you mean "{v}"?')
    for path in elsewhere.get(key, ()):
        out.append(f'"{key}" is valid at {path}, not here')
    alias = ALIASES.get(key) or ALIASES.get(norm)
    if alias and alias in valid_keys and f'did you mean "{alias}"?' not in out:
        out.append(f'did you mean "{alias}"?')
    if not any(s.startswith("did you mean") for s in out):
        for m in difflib.get_close_matches(key, valid_keys, n=3, cutoff=0.6):
            out.append(f'did you mean "{m}"?')
    return out


def _valid_keys(schema):
    """The keys to offer at a level: every property that is not deprecated."""
    props = schema.get("properties", {})
    return [k for k, v in props.items() if not (isinstance(v, dict) and v.get("deprecated"))]


def _index_paths(schema, path, acc):
    """Every key the schema allows anywhere below path -> the paths it is
    valid at. Used to say "resources is valid at parameters, not here"."""
    props = schema.get("properties", {})
    for k, sub in props.items():
        acc.setdefault(k, []).append(path)
        if isinstance(sub, dict):
            _index_paths(sub, f"{path}.{k}", acc)
            if "items" in sub and isinstance(sub["items"], dict):
                _index_paths(sub["items"], f"{path}.{k}[]", acc)
            extra = sub.get("additionalProperties")
            if isinstance(extra, dict):
                _index_paths(extra, f"{path}.{k}.*", acc)
    extra = schema.get("additionalProperties")
    if isinstance(extra, dict):
        _index_paths(extra, f"{path}.*", acc)
    if "items" in schema and isinstance(schema["items"], dict):
        _index_paths(schema["items"], f"{path}[]", acc)
    return acc


_TYPES = {
    "object": (dict, "an object"),
    "array": (list, "a list"),
    "string": (str, "a string"),
    "number": ((int, float), "a number"),
    "integer": (int, "an integer"),
    "boolean": (bool, "true or false"),
}


def _type_name(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, int):
        return "an integer"
    for name, (py, article) in _TYPES.items():
        if name not in ("boolean", "integer") and isinstance(value, py):
            return article
    return type(value).__name__


def _is_constrained(schema):
    return "minItems" in schema or "maxItems" in schema


# The declared types that are enforced on a property. object and array are
# documentation only: many schemas declare them on levels whose shape the
# processor itself reports on, and saved pipelines rely on that leniency.
_SCALAR_TYPES = ("string", "number", "integer", "boolean")


def _type_ok(value, declared):
    if declared not in _TYPES:
        return True
    py = _TYPES[declared][0]
    if declared != "boolean" and isinstance(value, bool):
        return False
    return isinstance(value, py)


def _constraints(value, schema, key):
    """The messages a constrained array property `key` breaks: it must be a
    list, of the declared length, of items of the declared type."""
    if not isinstance(value, list):
        yield f'"{key}" must be a list, got {_type_name(value)}'
        return
    lo, hi, n = schema.get("minItems"), schema.get("maxItems"), len(value)
    if lo is not None and hi is not None and lo == hi and n != lo:
        yield f'"{key}" must list exactly {lo} item{"" if lo == 1 else "s"}, got {n}'
    elif lo is not None and n < lo:
        yield f'"{key}" must list at least {lo} item{"" if lo == 1 else "s"}, got {n}'
    elif hi is not None and n > hi:
        yield f'"{key}" must list at most {hi} item{"" if hi == 1 else "s"}, got {n}'


def _constraint(path, key, message):
    return (CONSTRAINT, path, key, [], [], message)


def _descend(child, schema, parent, key, child_path, elsewhere):
    """Check the property `key` of `parent` (whose own path is child_path):
    a declared scalar type, a constrained array's constraints and item types,
    then what is below."""
    if isinstance(schema, dict):
        declared = schema.get("type")
        if declared in _SCALAR_TYPES and not _type_ok(child, declared):
            yield _constraint(parent, key, f'"{key}" must be {_TYPES[declared][1]}, got {_type_name(child)}')
            return
    if isinstance(schema, dict) and _is_constrained(schema):
        for message in _constraints(child, schema, key):
            yield _constraint(parent, key, message)
        if not isinstance(child, list):
            return
        items = schema.get("items")
        declared = items.get("type") if isinstance(items, dict) else None
        if isinstance(declared, str):
            for i, item in enumerate(child):
                if not _type_ok(item, declared):
                    yield _constraint(child_path, f"[{i}]", f'"[{i}]" must be {_TYPES[declared][1]}, got {_type_name(item)}')
    yield from _check(child, schema, child_path, elsewhere)


def _check(value, schema, path, elsewhere) -> Iterator[tuple]:
    """Walk value against schema, yielding
    (kind, path, key, suggestions, valid, message) per finding."""
    if not isinstance(schema, dict):
        return
    if isinstance(value, dict):
        props = schema.get("properties")
        extra = schema.get("additionalProperties", True)
        if props is not None:
            valid = _valid_keys(schema)
            for key, child in value.items():
                if key in props:
                    yield from _descend(child, props[key], path, key, f"{path}.{key}", elsewhere)
                elif extra is True:
                    continue
                elif isinstance(extra, dict):
                    yield from _descend(child, extra, path, key, f"{path}.{key}", elsewhere)
                else:
                    yield (UNKNOWN_KEY, path, key, _suggest(key, valid, elsewhere), valid, None)
        elif isinstance(extra, dict):
            for key, child in value.items():
                yield from _descend(child, extra, path, key, f"{path}.{key}", elsewhere)
        # Required keys last, so unknown keys (the common finding) lead.
        for name in schema.get("required", ()):
            if isinstance(name, str) and name not in value:
                yield _constraint(path, name, f'required key "{name}" is missing')
    elif isinstance(value, list):
        items = schema.get("items")
        if isinstance(items, dict):
            for i, child in enumerate(value):
                yield from _check(child, items, f"{path}[{i}]", elsewhere)


def check_step(step, step_index=0):
    """The problems of one step, as Problem objects (an empty list when it is
    clean). A step whose processor has no schema is checked against the
    envelope only."""
    if not isinstance(step, dict):
        return []
    run = step.get("run")
    run_label = run if isinstance(run, str) else "unknown"
    problems = []

    envelope = load_schema(ENVELOPE_NAME)
    processor = load_schema(run) if isinstance(run, str) else None
    # The envelope's parameters are open; the processor schema takes over
    # there. Build one combined schema so "valid elsewhere" sees both.
    combined = json.loads(json.dumps(envelope))
    if processor is not None:
        combined["properties"]["parameters"] = processor
    elsewhere = _index_paths(combined, "step", {})

    for kind, path, key, suggestions, valid, message in _check(step, combined, "step", elsewhere):
        problems.append(Problem(step_index, run_label, path, key, suggestions, valid, kind=kind, message=message))
    return problems


def check_steps(steps):
    """The problems of every step, in order."""
    problems = []
    if not isinstance(steps, list):
        return problems
    for i, step in enumerate(steps):
        problems.extend(check_step(step, i))
    return problems


def validate_step(step, step_index=0):
    """Raise StepSchemaError when the step carries unknown keys."""
    problems = check_step(step, step_index)
    if problems:
        raise StepSchemaError(problems)


def validate_steps(steps):
    """Raise StepSchemaError listing every unknown key in every step, so the
    author can fix them all at once."""
    problems = check_steps(steps)
    if problems:
        raise StepSchemaError(problems)
