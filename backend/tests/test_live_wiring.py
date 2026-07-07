"""Live-mode wiring: provider selection, adapter dispatch, block normalization.

These lock in the paths that only run when STRATEGYAI_MOCK_* is off, so live
Bedrock / Anthropic-API mode can't silently break without a live server.
"""

import pytest

from app.config import Settings


def _settings(monkeypatch, **env):
    for key in ("STRATEGYAI_LLM_PROVIDER", "STRATEGYAI_MOCK_LLM",
                "STRATEGYAI_MOCK_MSTR"):
        monkeypatch.delenv(key, raising=False)
    for key, val in env.items():
        monkeypatch.setenv(key, val)
    return Settings()


def test_provider_explicit_anthropic(monkeypatch):
    s = _settings(monkeypatch, STRATEGYAI_LLM_PROVIDER="anthropic")
    assert s.llm_provider == "anthropic"
    assert s.mock_llm is False


def test_provider_explicit_bedrock(monkeypatch):
    s = _settings(monkeypatch, STRATEGYAI_LLM_PROVIDER="bedrock")
    assert s.llm_provider == "bedrock"


def test_provider_defaults_to_mock(monkeypatch):
    s = _settings(monkeypatch)
    assert s.llm_provider == "mock"
    assert s.mock_llm is True


def test_legacy_mock_flag_off_selects_bedrock(monkeypatch):
    s = _settings(monkeypatch, STRATEGYAI_MOCK_LLM="false")
    assert s.llm_provider == "bedrock"


def test_build_llm_dispatch(monkeypatch):
    import app.agent.llm as llm
    monkeypatch.setattr(llm.settings, "llm_provider", "mock")
    assert isinstance(llm.build_llm(), llm.MockLLM)

    monkeypatch.setattr(llm.settings, "llm_provider", "anthropic")
    monkeypatch.setattr(llm, "AnthropicDirectLLM", lambda: "ANTHROPIC")
    assert llm.build_llm() == "ANTHROPIC"

    monkeypatch.setattr(llm.settings, "llm_provider", "bedrock")
    monkeypatch.setattr(llm, "BedrockClaudeLLM", lambda: "BEDROCK")
    assert llm.build_llm() == "BEDROCK"

    monkeypatch.setattr(llm.settings, "llm_provider", "bogus")
    with pytest.raises(ValueError):
        llm.build_llm()


def test_build_executor_dispatch(monkeypatch):
    import app.main as main
    from app.mstr.executors import RealMstrExecutor
    from app.mstr.mock import MockMstrExecutor

    monkeypatch.setattr(main.settings, "mock_mstr", True)
    assert isinstance(main.build_executor(), MockMstrExecutor)

    monkeypatch.setattr(main.settings, "mock_mstr", False)
    monkeypatch.setattr(main.settings, "mstr_base_url",
                        "https://mstr.example/MicroStrategyLibrary/api")
    assert isinstance(main.build_executor(), RealMstrExecutor)


class _FakeBlock:
    def __init__(self, **kw):
        self.__dict__.update(kw)

    def model_dump(self):
        return dict(self.__dict__)


class _FakeResp:
    def __init__(self, content):
        self.content = content


def test_extract_drops_empty_text_and_builds_tool_call():
    from app.agent.llm import _extract
    r = _extract(_FakeResp([
        _FakeBlock(type="text", text=""),                 # dropped
        _FakeBlock(type="text", text="Working on it"),
        _FakeBlock(type="tool_use", id="toolu_1",
                   name="list_projects", input={}),
    ]))
    assert r.text == "Working on it"
    assert r.tool_call.name == "list_projects"
    assert r.raw_content == [
        {"type": "text", "text": "Working on it"},
        {"type": "tool_use", "id": "toolu_1", "name": "list_projects", "input": {}},
    ]


def test_extract_handles_empty_content():
    from app.agent.llm import _extract
    r = _extract(_FakeResp([]))
    assert r.raw_content == [{"type": "text", "text": "(no response)"}]
    assert r.tool_call is None


def test_extract_ignores_non_dict_tool_input():
    from app.agent.llm import _extract
    r = _extract(_FakeResp([
        _FakeBlock(type="tool_use", id="t2", name="list_projects", input=None),
    ]))
    assert r.tool_call.arguments == {}
    assert r.raw_content[0]["input"] == {}
