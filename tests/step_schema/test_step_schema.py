import json
import os

import pytest

from bcodmo_frictionless.step_schema import (
    ALIASES,
    SCHEMA_DIR,
    StepSchemaError,
    check_steps,
    has_schema,
    load_schema,
    schema_names,
    validate_step,
    validate_steps,
)


def step(run, parameters, **extra):
    return {"run": run, "parameters": parameters, **extra}


class TestSchemas:
    def test_every_schema_file_parses_and_closes_its_keys(self):
        for name in schema_names():
            schema = load_schema(name)
            assert schema["type"] == "object"
            assert "properties" in schema or schema.get("additionalProperties") is True, name

    def test_envelope_names_the_annotation_keys(self):
        props = load_schema("_step")["properties"]
        for key in ("run", "parameters", "cache", "checkpoint", "bcodmo_notes", "llm_notes", "human_validated", "submission_source", "parameter_mapper_result"):
            assert key in props

    def test_has_schema(self):
        assert has_schema("bcodmo_pipeline_processors.load")
        assert has_schema("update_package")
        assert not has_schema("bcodmo_pipeline_processors.dump_to_path")
        assert not has_schema("nope")


class TestClean:
    @pytest.mark.parametrize(
        "s",
        [
            step("bcodmo_pipeline_processors.load", {"from": "s3://b/a.csv", "name": "a", "skip_rows": [1, {"type": "regex", "value": "^#"}], "override_schema": {"missingValues": ["", "nd"]}}),
            step("bcodmo_pipeline_processors.update_fields", {"resources": ["a"], "fields": {"Temp C": {"bcodmo:": {"standard_name_id": "730", "units": ""}}}}, submission_source=[{"resource": "a", "submission_id": "S1", "resource_id": "r1", "filename": "a.csv", "sheet_name": None, "linked_by": "load_step"}], parameter_mapper_result={"anything": 1}, llm_notes="x", human_validated=False),
            step("bcodmo_pipeline_processors.set_types", {"resources": ["a"], "types": {"lat": {"type": "number"}, "junk": None}}),
            step("bcodmo_pipeline_processors.join", {"source": {"name": "a", "key": ["id"], "delete": True}, "target": {"name": "b", "key": ["id"]}, "fields": {"temp": {"name": "temp", "aggregate": "any"}}, "mode": "full-outer"}),
            step("update_package", {"title": "x", "whatever": {"deep": 1}}),
            step("bcodmo_pipeline_processors.add_schema_metadata", {"resources": ["a"], "primaryKey": ["id"], "bcodmo:anything": 1}),
            step("bcodmo_pipeline_processors.convert_date", {"resources": ["a"], "fields": [{"input_type": "python", "inputs": [{"field": "d", "format": "%Y"}], "output_field": "ISO", "output_format": "%Y", "output_type": "datetime"}]}),
            step("bcodmo_pipeline_processors.dump_to_s3", {"bucket_name": "b", "prefix": "p", "force-format": True, "save_pipeline_spec": True}),
            step("bcodmo_pipeline_processors.find_replace", {"resources": ["a"], "fields": [{"name": "x", "patterns": [{"find": "a", "replace": "b", "replace_missing_values": True}]}]}),
        ],
    )
    def test_accepted(self, s):
        validate_step(s)
        assert check_steps([s]) == []

    def test_unknown_processor_is_checked_against_the_envelope_only(self):
        validate_step(step("nope", {"anything": 1}))
        with pytest.raises(StepSchemaError, match='unknown key "bogus" at step'):
            validate_step(step("nope", {}, bogus=1))


class TestProblems:
    def test_message_shape(self):
        s = step("bcodmo_pipeline_processors.update_fields", {"resources": ["a"], "fields": {"lat": {"bcodmo:": {"standard_name": "808"}}}})
        with pytest.raises(StepSchemaError) as e:
            validate_steps([{"run": "bcodmo_pipeline_processors.load", "parameters": {"from": "x", "name": "a"}}, {"run": "bcodmo_pipeline_processors.rename_fields", "parameters": {"resources": ["a"], "fields": []}}, s])
        msg = str(e.value)
        assert 'step 3 (bcodmo_pipeline_processors.update_fields): unknown key "standard_name" at step.parameters.fields.lat.bcodmo:' in msg
        assert 'did you mean "standard_name_id"?' in msg
        assert "valid keys here: standard_name_id, is_primary, supplied_units, description, units" in msg

    def test_all_problems_in_all_steps_are_reported_together(self):
        steps = [
            step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a", "Limit_Rows": 5}),
            step("bcodmo_pipeline_processors.rename_fields", {"resources": ["a"], "fields": [{"old": "a", "new_field": "b"}]}, typo=1),
        ]
        problems = check_steps(steps)
        # In step order, and within a step in the step's own key order
        # (parameters come before the envelope's trailing keys).
        assert [(p.step_index, p.key) for p in problems] == [(0, "Limit_Rows"), (1, "old"), (1, "typo")]

    def test_normalized_equality_suggestions(self):
        s = step("bcodmo_pipeline_processors.update_fields", {"resources": ["a"], "fields": {"lat": {"bcodmo": {"standardNameId": "808"}}}})
        problems = check_steps([s])
        assert len(problems) == 1
        assert problems[0].key == "bcodmo"
        assert 'did you mean "bcodmo:"?' in problems[0].suggestions
        # Once bcodmo: is right, the camelCase key is caught.
        s["parameters"]["fields"]["lat"] = {"bcodmo:": {"standardNameId": "808"}}
        problems = check_steps([s])
        assert 'did you mean "standard_name_id"?' in problems[0].suggestions

    def test_misplaced_key(self):
        s = step("bcodmo_pipeline_processors.update_fields", {"fields": {"lat": {"resources": ["a"]}}})
        problems = check_steps([s])
        assert any('"resources" is valid at step.parameters, not here' in sug for sug in problems[0].suggestions)

    def test_alias_and_close_match(self):
        s = step("bcodmo_pipeline_processors.rename_fields", {"resources": ["a"], "columns": []})
        problems = check_steps([s])
        assert 'did you mean "fields"?' in problems[0].suggestions
        s = step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a", "limit_row": 5})
        problems = check_steps([s])
        assert 'did you mean "limit_rows"?' in problems[0].suggestions
        assert "columns" in ALIASES

    def test_deprecated_keys_are_accepted_but_never_offered(self):
        s = step("bcodmo_pipeline_processors.dump_to_s3", {"bucket_name": "b", "prefix": "p", "force-format": True})
        assert check_steps([s]) == []
        s["parameters"]["forceformat"] = True
        problems = check_steps([s])
        assert problems[0].key == "forceformat"
        assert "force-format" not in problems[0].valid_keys
        assert all("force-format" not in sug for sug in problems[0].suggestions)
        assert 'did you mean "force_format"?' in problems[0].suggestions

    def test_array_items_and_keyed_maps(self):
        s = step("bcodmo_pipeline_processors.convert_date", {"resources": ["a"], "fields": [{"output_field": "x", "inputs": [{"field": "d", "fmt": "%Y"}]}]})
        problems = check_steps([s])
        assert [(p.path, p.key) for p in problems] == [("step.parameters.fields[0].inputs[0]", "fmt")]
        s = step("bcodmo_pipeline_processors.set_types", {"types": {"lat": {"type": "number", "typo": 1}}})
        problems = check_steps([s])
        assert [(p.path, p.key) for p in problems] == [("step.parameters.types.lat", "typo")]

    def test_non_dict_values_are_ignored(self):
        # Key checks only: a list where an object is expected is the
        # processor's error to report.
        validate_step(step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a", "skip_rows": "oops", "override_schema": 3}))

    def test_problem_as_dict_roundtrip(self):
        p = check_steps([step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a", "bad": 1})])[0]
        d = p.as_dict()
        assert d["key"] == "bad" and d["path"] == "step.parameters" and d["step_index"] == 0
        json.dumps(d)


def test_schema_dir_is_packaged():
    assert os.path.isdir(SCHEMA_DIR)
    assert "bcodmo_pipeline_processors.load.json" in os.listdir(SCHEMA_DIR)


class TestHistoricalKeys:
    """Keys the 2026-10 audit of saved pipelines found and a human kept."""

    def test_client_step_id_is_accepted_but_never_offered(self):
        s = step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a"}, id="s-abc")
        assert check_steps([s]) == []
        s["idd"] = 1
        p = check_steps([s])[0]
        assert "id" not in p.valid_keys

    def test_edit_cells_old_value(self):
        validate_step(step("bcodmo_pipeline_processors.edit_cells", {"resources": ["a"], "edited": [{"row": 1, "cells": [{"field": "x", "value": "1", "oldValue": "0"}]}]}))

    def test_old_load_and_dump_keys(self):
        validate_step(step("bcodmo_pipeline_processors.load", {"from": "x", "name": "a", "validate": True, "force_strings": True, "missing_values": [""]}))
        validate_step(step("bcodmo_pipeline_processors.dump_to_s3", {"bucket_name": "b", "prefix": "p", "dataset_id": "1", "dataset_version": "1"}))
