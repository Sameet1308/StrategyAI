"""Registry invariants: schemas are valid, strict, and previews exist."""

import jsonschema

from app.agent.registry import TOOLS, anthropic_tools


def test_every_schema_is_valid_draft202012():
    for tool in TOOLS.values():
        jsonschema.Draft202012Validator.check_schema(tool.input_schema)


def test_every_schema_rejects_extra_properties():
    for tool in TOOLS.values():
        assert tool.input_schema.get("additionalProperties") is False, tool.name


def test_mutating_tools_have_previews():
    for tool in TOOLS.values():
        if tool.mutating:
            assert tool.preview is not None, tool.name
            text = tool.preview({"project_id": "A" * 32,
                                 "subscription_id": "B" * 32,
                                 "cube_id": "C" * 32,
                                 "cache_id": "cache-x"}, {})
            assert isinstance(text, str) and len(text) > 20


def test_anthropic_format():
    tools = anthropic_tools()
    assert len(tools) == len(TOOLS)
    for t in tools:
        assert set(t) == {"name", "description", "input_schema"}


def test_id_patterns_reject_injection():
    schema = TOOLS["pause_subscription"].input_schema
    bad = {"project_id": "x' OR 1=1 --", "subscription_id": "B" * 32}
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(bad))
    assert errors
