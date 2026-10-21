"""Key-name validation of Laminar pipeline steps.

Every place that executes a pipeline step installs this package (laminar_server's
workers and request handlers, the laminar-pipeline-run Lambda, the
file_statistics Lambda), so this is where the one machine-readable description
of each processor's parameters lives: ``schemas/<processor>.json``, a JSON
Schema that names the keys a step may carry at every level, plus
``schemas/_step.json`` for the step envelope (``run``, ``parameters``, the
annotation keys laminar_web and the LLM tooling add).

Only key names are checked. Pipelines are never hand-edited; an unknown key
comes from an LLM inventing one, and until now it passed through silently
because most processors read their parameters with ``.get()`` and ignore the
rest. Types and enums can be tightened later without changing the mechanism.

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


class Problem:
    """One unknown key: where it is, what it might have meant, what is valid."""

    def __init__(self, step_index, run, path, key, suggestions, valid_keys):
        self.step_index = step_index
        self.run = run
        self.path = path
        self.key = key
        self.suggestions = suggestions
        self.valid_keys = valid_keys

    @property
    def step_label(self):
        return f"step {self.step_index + 1} ({self.run})"

    def __str__(self):
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


def _check(value, schema, path, elsewhere) -> Iterator[tuple]:
    """Walk value against schema, yielding (path, key, suggestions, valid)."""
    if not isinstance(schema, dict):
        return
    if isinstance(value, dict):
        props = schema.get("properties")
        extra = schema.get("additionalProperties", True)
        if props is not None:
            valid = _valid_keys(schema)
            for key, child in value.items():
                if key in props:
                    yield from _check(child, props[key], f"{path}.{key}", elsewhere)
                elif extra is True:
                    continue
                elif isinstance(extra, dict):
                    yield from _check(child, extra, f"{path}.{key}", elsewhere)
                else:
                    yield (path, key, _suggest(key, valid, elsewhere), valid)
        elif isinstance(extra, dict):
            for key, child in value.items():
                yield from _check(child, extra, f"{path}.{key}", elsewhere)
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

    for path, key, suggestions, valid in _check(step, combined, "step", elsewhere):
        problems.append(Problem(step_index, run_label, path, key, suggestions, valid))
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
