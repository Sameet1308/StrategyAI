"""Scripted LLM for driving the loop through exact scenarios in tests."""

from app.agent.llm import LLMResult, ToolCall


class ScriptedLLM:
    """Returns queued responses in order; repeats the last one if exhausted."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def complete(self, system, messages, tools):
        self.calls += 1
        item = self.responses.pop(0) if self.responses else "…"
        if isinstance(item, str):
            return LLMResult(text=item, raw_content=[{"type": "text", "text": item}])
        name, args = item
        call_id = f"toolu_scripted_{self.calls}"
        return LLMResult(
            tool_call=ToolCall(id=call_id, name=name, arguments=args),
            raw_content=[{"type": "tool_use", "id": call_id,
                          "name": name, "input": args}],
        )
