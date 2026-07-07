"""LLM adapters.

Three interchangeable providers, all speaking the Anthropic Messages API
message format ({"role": ..., "content": [content blocks]}):

- BedrockClaudeLLM   — Claude on Amazon Bedrock (client's AWS deployment;
                       `anthropic.`-prefixed model IDs, AWS credentials).
- AnthropicDirectLLM — Claude via the Anthropic API with an API key. Lets the
                       whole stack run LIVE on a laptop with just MSTR creds +
                       one ANTHROPIC_API_KEY, no AWS setup.
- MockLLM            — deterministic offline stand-in for dev and tests.

Selected by STRATEGYAI_LLM_PROVIDER (mock | anthropic | bedrock).
"""

import json
import re
from dataclasses import dataclass, field

from ..config import settings


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict


@dataclass
class LLMResult:
    text: str = ""
    tool_call: ToolCall | None = None
    # Assistant content blocks, normalized to minimal input shape and replayed
    # verbatim on the next request.
    raw_content: list = field(default_factory=list)


def _extract(response) -> LLMResult:
    """Turn an Anthropic response into an LLMResult.

    Blocks are normalized to the minimal shape the Messages API accepts as
    INPUT — model_dump() can carry output-only fields (e.g. citations=None)
    that are invalid when replayed, so we rebuild them. Empty text blocks are
    dropped (the API rejects them); one tool call max (parallel disabled).
    """
    text_parts: list[str] = []
    tool_call: ToolCall | None = None
    blocks: list[dict] = []
    for b in response.content:
        btype = getattr(b, "type", None)
        if btype == "text":
            if b.text:
                text_parts.append(b.text)
                blocks.append({"type": "text", "text": b.text})
        elif btype == "tool_use":
            args = b.input if isinstance(b.input, dict) else {}
            blocks.append({"type": "tool_use", "id": b.id, "name": b.name,
                           "input": args})
            if tool_call is None:
                tool_call = ToolCall(id=b.id, name=b.name, arguments=args)
        elif btype in ("thinking", "redacted_thinking"):
            # replay unchanged if the model ever emits it
            blocks.append(b.model_dump())
    if not blocks:
        blocks = [{"type": "text", "text": "(no response)"}]
    return LLMResult(text=" ".join(text_parts).strip(),
                     tool_call=tool_call, raw_content=blocks)


class BedrockClaudeLLM:
    """Claude on Bedrock. One tool call max per turn (parallel disabled)."""

    def __init__(self) -> None:
        from anthropic import AnthropicBedrockMantle
        self._client = AnthropicBedrockMantle(aws_region=settings.aws_region)
        self._model = settings.bedrock_model_id

    def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResult:
        response = self._client.messages.create(
            model=self._model, max_tokens=4096, system=system,
            messages=messages, tools=tools,
            tool_choice={"type": "auto", "disable_parallel_tool_use": True},
        )
        return _extract(response)


class AnthropicDirectLLM:
    """Claude via the Anthropic API (ANTHROPIC_API_KEY). Same loop as Bedrock."""

    def __init__(self) -> None:
        import anthropic
        kwargs = {}
        if settings.anthropic_api_key:
            kwargs["api_key"] = settings.anthropic_api_key
        self._client = anthropic.Anthropic(**kwargs)
        self._model = settings.anthropic_model_id

    def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResult:
        response = self._client.messages.create(
            model=self._model, max_tokens=4096, system=system,
            messages=messages, tools=tools,
            tool_choice={"type": "auto", "disable_parallel_tool_use": True},
        )
        return _extract(response)


# --------------------------------------------------------------------------
# Mock LLM
# --------------------------------------------------------------------------

_INTENTS = [
    # (intent, needs project, needs subscription, needs cube)
    ("pause_subscription", True, True, False),
    ("resume_subscription", True, True, False),
    ("delete_subscription", True, True, False),
    ("trigger_subscription_now", True, True, False),
    ("get_subscription", True, True, False),
    ("get_subscription_status", True, True, False),
    ("list_subscriptions", True, False, False),
    ("publish_cube", True, False, True),
    ("refresh_cube", True, False, True),
    ("get_cube_status", True, False, True),
    ("unload_cube_cache", False, False, False),
    ("list_cube_caches", False, False, False),
    ("list_schedules", False, False, False),
    ("list_projects", False, False, False),
]
_INTENT_META = {name: (proj, sub, cube) for name, proj, sub, cube in _INTENTS}

_HELP = ("I can manage subscriptions (list, pause, resume, delete, send now) "
         "and cubes (status, publish, refresh, list/unload caches) plus list "
         "projects and schedules. What would you like to do?")


def _detect_intent(text: str) -> str | None:
    t = text.lower()
    listing = any(w in t for w in ("list", "show", "what", "which", "all", "view", "display"))
    sub_word = "subscription" in t or re.search(r"\bsubs?\b", t) is not None
    if "cache" in t:
        if any(w in t for w in ("unload", "purge", "clear", "evict", "remove", "delete")):
            return "unload_cube_cache"
        return "list_cube_caches"
    if "cube" in t:
        if "publish" in t and "republish" not in t:
            return "publish_cube"
        if any(w in t for w in ("refresh", "republish", "reload", "update")):
            return "refresh_cube"
        if any(w in t for w in ("status", "state", "last", "when", "row", "size")) or listing:
            return "get_cube_status"
    # subscription delivery status — checked before "trigger" so "did it
    # deliver?" (a question) isn't read as "deliver it" (a command). Cubes are
    # handled above, so a delivery question here is always about a subscription.
    status_phrase = any(p in t for p in (
        "delivery status", "did it deliver", "did it go out", "did it run",
        "last run", "last delivery", "did the"))
    if status_phrase or (sub_word and any(
            w in t for w in ("failed", "failure", "why did", "status"))):
        return "get_subscription_status"
    # subscription verbs don't require the word "subscription" — pausing and
    # triggering only apply to subscriptions in this tool set
    if any(w in t for w in ("resume", "unpause", "reactivate")) or \
            ("enable" in t and "disable" not in t):
        return "resume_subscription"
    if any(w in t for w in ("pause", "disable", "hold")):
        return "pause_subscription"
    if any(w in t for w in ("send now", "trigger", "run now", "deliver",
                            "execute now", "fire")):
        return "trigger_subscription_now"
    if sub_word and any(w in t for w in ("delete", "remove", "drop")):
        return "delete_subscription"
    if sub_word and ("detail" in t or "definition" in t):
        return "get_subscription"
    if sub_word and listing:
        return "list_subscriptions"
    if "schedule" in t and listing:
        return "list_schedules"
    if "project" in t and listing:
        return "list_projects"
    return None


class MockLLM:
    """Deterministic intent + slot-filling engine mirroring Claude's behavior."""

    def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResult:
        state = _ConvState(messages)

        # A tool just errored -> surface it and stop.
        if state.last_error is not None:
            return _text(f"That didn't work: {state.last_error}")

        intent = state.current_intent()
        if intent is None:
            return _text(_HELP)

        needs_project, needs_sub, needs_cube = _INTENT_META[intent]
        args: dict = {}

        if needs_project:
            project = state.resolve_project()
            if project == "ASK":
                names = ", ".join(p["name"] for p in state.projects)
                return _text(f"Which project should I use? Available: {names}.")
            if project is None:
                return state.call("list_projects", {})
            args["project_id"] = project["id"]

        if needs_sub:
            sub = state.resolve_subscription(args["project_id"])
            if sub == "ASK":
                names = ", ".join(f"“{s['name']}”" for s in state.subscriptions)
                return _text(f"Which subscription do you mean? In this project: {names}.")
            if sub is None:
                return state.call("list_subscriptions", {"project_id": args["project_id"]})
            args["subscription_id"] = sub["id"]

        if needs_cube:
            cube = state.resolve_cube(args["project_id"])
            if cube == "ASK":
                names = ", ".join(f"“{c['name']}”" for c in state.search_hits)
                return _text(f"Which cube do you mean? Matches: {names or 'none found'}. "
                             f"Please give the exact cube name.")
            if cube is None:
                term = state.cube_search_term()
                if not term:
                    return _text("Which cube? Please give me its name.")
                return state.call("search_objects",
                                  {"project_id": args["project_id"], "name": term,
                                   "object_type": "cube"})
            args["cube_id"] = cube["id"]

        if intent == "unload_cube_cache":
            cache = state.resolve_cache()
            if cache == "ASK":
                names = ", ".join(f"“{c['cube_name']}” ({c['cache_id']})"
                                  for c in state.caches)
                return _text(f"Which cache should I unload? Loaded caches: {names}.")
            if cache is None:
                return state.call("list_cube_caches", {})
            args = {"cache_id": cache["cache_id"], "node_name": cache["node"]}

        if intent == "list_subscriptions":
            filter_text = state.quoted_term()
            if filter_text:
                args["filter_text"] = filter_text

        # Already have this intent's result? Then this turn is the summary.
        summary = state.summarize_if_done(intent)
        if summary is not None:
            return _text(summary)

        return state.call(intent, args)


def _text(t: str) -> LLMResult:
    return LLMResult(text=t, raw_content=[{"type": "text", "text": t}])


class _ConvState:
    """Parses the conversation into intents, entities and tool results."""

    def __init__(self, messages: list[dict]) -> None:
        self.messages = messages
        self.user_texts: list[str] = []
        self.tool_uses: dict[str, dict] = {}       # tool_use_id -> {name, input}
        self.results: list[dict] = []              # {name, input, data|error, msg_idx}
        self.last_error: str | None = None
        self.last_user_idx = -1                    # index of last human text message
        self._parse()
        # environment catalogs stay valid across turns
        self.projects = self._latest("list_projects") or []
        self.subscriptions = self._latest("list_subscriptions") or []
        self.caches = self._latest("list_cube_caches") or []
        # search results only bind to the current request
        self.search_hits = self._latest("search_objects") or []

    def _parse(self) -> None:
        for idx, msg in enumerate(self.messages):
            content = msg.get("content")
            if isinstance(content, str):
                if msg["role"] == "user":
                    self.user_texts.append(content)
                    self.last_user_idx = idx
                continue
            for block in content or []:
                btype = block.get("type")
                if msg["role"] == "user" and btype == "text":
                    self.user_texts.append(block.get("text", ""))
                    self.last_user_idx = idx
                elif msg["role"] == "assistant" and btype == "tool_use":
                    self.tool_uses[block["id"]] = {
                        "name": block["name"], "input": block.get("input") or {}}
                elif msg["role"] == "user" and btype == "tool_result":
                    use = self.tool_uses.get(block.get("tool_use_id", ""), {})
                    raw = block.get("content", "")
                    if isinstance(raw, list):
                        raw = " ".join(b.get("text", "") for b in raw)
                    entry = {"name": use.get("name", ""),
                             "input": use.get("input", {}), "msg_idx": idx}
                    if block.get("is_error"):
                        entry["error"] = raw
                    else:
                        try:
                            entry["data"] = json.loads(raw)
                        except (json.JSONDecodeError, TypeError):
                            entry["data"] = raw
                    self.results.append(entry)
        # Only report an error if it is the most recent event in the thread.
        if self.results and "error" in self.results[-1]:
            last_block = (self.messages[-1].get("content") or [{}])[-1] \
                if isinstance(self.messages[-1].get("content"), list) else {}
            if last_block.get("type") == "tool_result" and last_block.get("is_error"):
                self.last_error = self.results[-1]["error"]

    def _latest(self, tool: str, after: int | None = None):
        for entry in reversed(self.results):
            if entry["name"] == tool and "data" in entry:
                if after is not None and entry["msg_idx"] <= after:
                    return None
                return entry["data"]
        return None

    # ----- intent ---------------------------------------------------------

    def current_intent(self) -> str | None:
        for text in reversed(self.user_texts):
            intent = _detect_intent(text)
            if intent:
                return intent
        return None

    def _all_text(self) -> str:
        return " ".join(self.user_texts).lower()

    def quoted_term(self) -> str | None:
        for text in reversed(self.user_texts):
            m = re.search(r"[\"“']([^\"”']{2,})[\"”']", text)
            if m:
                return m.group(1)
        return None

    # ----- slot resolution --------------------------------------------------

    def resolve_project(self):
        """Returns project dict, None (need list_projects), or 'ASK'.

        Scans user messages newest-first so "…in Sales Operations" wins over
        a project mentioned earlier in the conversation. Quoted spans are
        object names, not project names, so they're stripped before matching.
        """
        if not self.projects:
            return None
        by_id = {p["id"].lower(): p for p in self.projects}
        for text in reversed(self.user_texts):
            t = re.sub(r"[\"“'][^\"”']*[\"”']", " ", text.lower())
            for h in re.findall(r"\b[a-f0-9]{32}\b", t):
                if h in by_id:
                    return by_id[h]
            full = [p for p in self.projects if p["name"].lower() in t]
            if len(full) == 1:
                return full[0]
            if len(full) > 1:
                return "ASK"
            word = [p for p in self.projects if _word_match(p["name"], t)]
            if len(word) == 1:
                return word[0]
            if len(word) > 1:
                return "ASK"
        return "ASK"

    def resolve_subscription(self, project_id: str):
        if not self.subscriptions:
            return None
        blob = self._all_text()
        quoted = self.quoted_term()
        pool = self.subscriptions
        if quoted:
            hits = [s for s in pool if quoted.lower() in s["name"].lower()]
        else:
            hits = [s for s in pool if s["name"].lower() in blob]
        if len(hits) == 1:
            return hits[0]
        return "ASK"

    def cube_search_term(self) -> str | None:
        quoted = self.quoted_term()
        if quoted:
            return quoted
        for text in reversed(self.user_texts):
            m = re.search(r"(?:cube|status of)\s+([A-Za-z0-9 _-]{3,}?)(?:\s+cube)?\s*$",
                          text.strip(), re.IGNORECASE)
            if m:
                term = m.group(1).strip()
                if term.lower() not in ("the", "a", "my"):
                    return term
        return None

    def resolve_cube(self, project_id: str):
        blob = self._all_text()
        hexes = re.findall(r"\b[a-f0-9]{32}\b", blob)
        hits_by_id = {c["id"].lower(): c for c in self.search_hits}
        for h in hexes:
            if h in hits_by_id:
                return hits_by_id[h]
        # a search executed for the CURRENT user request is authoritative
        recent = self._latest("search_objects", after=self.last_user_idx)
        if recent is not None:
            if len(recent) == 1:
                return recent[0]
            named = [c for c in recent if c["name"].lower() in blob]
            if len(named) == 1:
                return named[0]
            return "ASK"
        # older hits still usable if the user names one unambiguously
        named = [c for c in self.search_hits if c["name"].lower() in blob]
        if len(named) == 1:
            return named[0]
        return None  # triggers a fresh search

    def resolve_cache(self):
        if not self.caches:
            return None
        blob = self._all_text()
        quoted = self.quoted_term()
        if quoted:
            hits = [c for c in self.caches if quoted.lower() in c["cube_name"].lower()]
        else:
            hits = [c for c in self.caches if c["cube_name"].lower() in blob]
        if len(hits) == 1:
            return hits[0]
        return "ASK"

    # ----- output ----------------------------------------------------------

    def call(self, tool: str, args: dict) -> LLMResult:
        call_id = f"toolu_mock_{len(self.messages)}"
        blocks = [{"type": "tool_use", "id": call_id, "name": tool, "input": args}]
        return LLMResult(tool_call=ToolCall(id=call_id, name=tool, arguments=args),
                         raw_content=blocks)

    def summarize_if_done(self, intent: str) -> str | None:
        """If the intent's tool returned data THIS turn, produce the reply."""
        entry = None
        for r in reversed(self.results):
            if (r["name"] == intent and "data" in r
                    and r["msg_idx"] > self.last_user_idx):
                entry = r
                break
        if entry is None:
            return None
        data = entry["data"]
        if intent == "list_projects":
            return f"You have {len(data)} projects — details in the table below."
        if intent == "list_subscriptions":
            enabled = sum(1 for s in data if s.get("enabled"))
            return (f"Found {len(data)} subscriptions ({enabled} enabled, "
                    f"{len(data) - enabled} paused) — see the table below.")
        if intent == "list_schedules":
            return f"There are {len(data)} schedules defined — see the table below."
        if intent == "list_cube_caches":
            return f"{len(data)} cube caches are loaded — see the table below."
        if intent == "get_subscription":
            return (f"Subscription “{data.get('name')}” is "
                    f"{'enabled' if data.get('enabled') else 'paused'}, owned by "
                    f"{data.get('owner')}, schedule: {data.get('schedule')}.")
        if intent == "get_subscription_status":
            state = data.get("last_run_state", "unknown")
            when = data.get("last_run") or "not recorded"
            fails = data.get("failures", 0)
            return (f"Subscription “{data.get('name')}” last run: {state} "
                    f"(at {when}, {fails} failures).")
        if intent == "get_cube_status":
            return (f"Cube “{data.get('name')}” is {data.get('status')}: "
                    f"{data.get('row_count'):,} rows, {data.get('size_mb')} MB, "
                    f"last updated {data.get('last_update')}.")
        if intent in ("pause_subscription", "resume_subscription",
                      "delete_subscription", "trigger_subscription_now"):
            verb = {"pause_subscription": "paused",
                    "resume_subscription": "resumed",
                    "delete_subscription": "deleted",
                    "trigger_subscription_now": "delivered"}[intent]
            return f"Done — subscription “{data.get('name', '?')}” has been {verb}."
        if intent in ("publish_cube", "refresh_cube"):
            verb = "publish" if intent == "publish_cube" else "refresh"
            return (f"Started {verb} of cube “{data.get('name', '?')}” "
                    f"(instance {data.get('instance_id', '?')}). Ask me for the "
                    f"refresh status to track progress.")
        if intent == "unload_cube_cache":
            return f"Done — cache {data.get('cache_id', '?')} was unloaded."
        return "Done — result is shown below."


def _word_match(name: str, blob: str) -> bool:
    words = [w for w in re.split(r"\W+", name.lower()) if len(w) >= 4]
    return any(w in blob for w in words)


def build_llm():
    provider = settings.llm_provider
    if provider == "mock":
        return MockLLM()
    if provider == "anthropic":
        return AnthropicDirectLLM()
    if provider == "bedrock":
        return BedrockClaudeLLM()
    raise ValueError(
        f"Unknown STRATEGYAI_LLM_PROVIDER '{provider}' "
        f"(expected: mock | anthropic | bedrock)")
