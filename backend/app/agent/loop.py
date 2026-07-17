"""The agent loop.

Six steps, exactly as agreed:
  1. intent detection        — LLM + tool registry
  2. converse to finalize    — LLM asks clarifying questions (multi-turn)
  3. capability check        — unknown tool -> error result back to the LLM
  4. payload collection      — jsonschema validation, errors loop back
  5. confirmation gate       — mutating tools create a PendingAction; the
                               ONLY code path that executes a mutating tool
                               is confirm() with an explicit user approval
  6. execute + audit         — every step lands in the audit log

The LLM proposes; this module verifies and gates. The LLM physically cannot
execute a mutating tool — there is no code path from an LLM response to a
mutating executor call that does not pass through a confirmed PendingAction.
"""

import json
from dataclasses import dataclass, field
from datetime import timedelta

import jsonschema
from sqlalchemy.orm import Session

from ..config import settings
from ..models import AuditLog, Conversation, Message, PendingAction, utcnow
from .llm import LLMResult, build_llm
from .prompts import SYSTEM_PROMPT
from .registry import TOOLS, anthropic_tools


@dataclass
class AgentReply:
    conversation_id: str
    kind: str                      # text | confirm | result | cancelled | error
    reply: str
    pending_action: PendingAction | None = None
    results: list[dict] = field(default_factory=list)   # [{tool, data}]


class AgentLoop:
    def __init__(self, executor, llm=None) -> None:
        self.executor = executor
        self.llm = llm or build_llm()
        self._tools = anthropic_tools()

    # ------------------------------------------------------------- chat

    def handle_chat(self, db: Session, user: str, conversation_id: str | None,
                    text: str) -> AgentReply:
        conv = self._get_or_create_conversation(db, user, conversation_id)

        # A new message supersedes any action still waiting for confirmation —
        # resolve it as cancelled so the tool_use/tool_result pairing stays valid.
        self._cancel_open_actions(db, conv, reason="superseded by a new message")

        self._append(db, conv, "user", text, [{"type": "text", "text": text}])
        self._audit(db, user, conv.id, "user_message", detail={"text": text})

        return self._run_llm_turns(db, conv, user)

    # ---------------------------------------------------------- confirm

    def handle_confirm(self, db: Session, user: str, action_id: str,
                       approved: bool) -> AgentReply:
        action = db.get(PendingAction, action_id)
        if action is None:
            return AgentReply("", "error", "Unknown action.")
        conv = db.get(Conversation, action.conversation_id)
        if conv is None or action.user_id != user or conv.user_id != user:
            # Don't leak whether the action exists for another user.
            return AgentReply("", "error", "Unknown action.")

        if action.status != "pending":
            return AgentReply(conv.id, "error",
                              f"This action was already {action.status}.")

        if utcnow() > _aware(action.expires_at):
            self._resolve_action(db, action, "expired")
            self._append_tool_result(db, conv, action.tool_use_id,
                                     "Confirmation window expired. Not executed.",
                                     is_error=True)
            self._audit(db, user, conv.id, "action_expired",
                        tool=action.tool_name)
            return AgentReply(conv.id, "cancelled",
                              "That confirmation expired — nothing was executed. "
                              "Ask again if you still want it done.")

        if not approved:
            self._resolve_action(db, action, "cancelled")
            self._append_tool_result(db, conv, action.tool_use_id,
                                     "User declined the action. Not executed.")
            self._audit(db, user, conv.id, "action_cancelled",
                        tool=action.tool_name)
            return AgentReply(conv.id, "cancelled",
                              "Okay — cancelled. Nothing was executed.")

        # ---- the gate: explicit approval is the only way to reach this ----
        self._resolve_action(db, action, "confirmed")
        self._audit(db, user, conv.id, "action_confirmed", tool=action.tool_name,
                    detail={"arguments": json.loads(action.arguments)})

        args = json.loads(action.arguments)
        try:
            data = self.executor.execute(action.tool_name, args)
        except Exception as exc:  # executor failures must never crash the API
            action.status = "failed"
            action.result = json.dumps({"error": str(exc)})
            db.commit()
            self._append_tool_result(db, conv, action.tool_use_id,
                                     f"Execution failed: {exc}", is_error=True)
            self._audit(db, user, conv.id, "action_failed",
                        tool=action.tool_name, detail={"error": str(exc)})
            return AgentReply(conv.id, "error",
                              f"The action failed on the MicroStrategy side: {exc}")

        action.status = "executed"
        action.result = json.dumps(data)
        db.commit()
        self._append_tool_result(db, conv, action.tool_use_id, json.dumps(data))
        self._audit(db, user, conv.id, "action_executed", tool=action.tool_name,
                    detail={"arguments": args, "result": data})

        reply = self._run_llm_turns(db, conv, user)
        reply.kind = "result" if reply.kind == "text" else reply.kind
        reply.results.insert(0, {"tool": action.tool_name, "data": data})
        return reply

    # ------------------------------------------------------ LLM turn loop

    def _run_llm_turns(self, db: Session, conv: Conversation, user: str) -> AgentReply:
        collected: list[dict] = []
        for _ in range(settings.max_tool_turns):
            messages = self._load_history(db, conv)
            result: LLMResult = self.llm.complete(SYSTEM_PROMPT, messages, self._tools)
            self._append(db, conv, "assistant", result.text, result.raw_content)

            if result.tool_call is None:
                self._audit(db, user, conv.id, "assistant_reply",
                            detail={"text": result.text})
                return AgentReply(conv.id, "text", result.text or "…",
                                  results=collected)

            call = result.tool_call
            spec = TOOLS.get(call.name)

            # capability check — the LLM referenced a tool we don't have
            if spec is None:
                self._append_tool_result(db, conv, call.id,
                                         f"Unknown tool '{call.name}'. Tell the "
                                         f"user this isn't supported yet.",
                                         is_error=True)
                self._audit(db, user, conv.id, "validation_failed",
                            tool=call.name, detail={"error": "unknown tool"})
                continue

            # deterministic payload validation — the LLM proposes, code verifies
            try:
                jsonschema.validate(call.arguments, spec.input_schema)
            except jsonschema.ValidationError as err:
                self._append_tool_result(db, conv, call.id,
                                         f"Invalid arguments: {err.message}. "
                                         f"Fix them or ask the user.",
                                         is_error=True)
                self._audit(db, user, conv.id, "validation_failed",
                            tool=call.name, detail={"error": err.message,
                                                    "arguments": call.arguments})
                continue

            if spec.mutating:
                pending = self._propose_action(db, conv, user, spec, call)
                return AgentReply(
                    conv.id, "confirm",
                    result.text or "Please review and confirm the action below.",
                    pending_action=pending, results=collected)

            # read-only tools auto-execute (still audited)
            try:
                data = self.executor.execute(call.name, call.arguments)
            except Exception as exc:
                self._append_tool_result(db, conv, call.id, str(exc), is_error=True)
                self._audit(db, user, conv.id, "tool_auto_executed", tool=call.name,
                            detail={"arguments": call.arguments, "error": str(exc)})
                continue

            collected.append({"tool": call.name, "data": data})
            self._append_tool_result(db, conv, call.id, json.dumps(data))
            self._audit(db, user, conv.id, "tool_auto_executed", tool=call.name,
                        detail={"arguments": call.arguments,
                                "rows": len(data) if isinstance(data, list) else 1})

        return AgentReply(conv.id, "error",
                          "I couldn't complete that within the allowed number of "
                          "steps. Try a more specific request.", results=collected)

    # ------------------------------------------------------------ helpers

    def _propose_action(self, db: Session, conv: Conversation, user: str,
                        spec, call) -> PendingAction:
        names = self._enrich_names(call.name, call.arguments)
        preview = spec.preview(call.arguments, names) if spec.preview else spec.name
        pending = PendingAction(
            conversation_id=conv.id,
            user_id=user,
            tool_name=spec.name,
            arguments=json.dumps(call.arguments),
            preview=preview,
            tool_use_id=call.id,
            expires_at=utcnow() + timedelta(seconds=settings.pending_action_ttl_seconds),
        )
        db.add(pending)
        db.commit()
        self._audit(db, user, conv.id, "action_proposed", tool=spec.name,
                    detail={"arguments": call.arguments, "preview": preview})
        return pending

    def _enrich_names(self, tool: str, args: dict) -> dict:
        """Best-effort name lookups so previews read like English, not IDs."""
        names: dict = {}
        try:
            if "project_id" in args:
                for p in self.executor.execute("list_projects", {}):
                    if p["id"].lower() == args["project_id"].lower():
                        names["project_name"] = p["name"]
                        break
            if "subscription_id" in args:
                sub = self.executor.execute("get_subscription", {
                    "project_id": args["project_id"],
                    "subscription_id": args["subscription_id"]})
                names["subscription_name"] = sub.get("name")
            if "cube_id" in args:
                cube = self.executor.execute("get_cube_status", {
                    "project_id": args["project_id"], "cube_id": args["cube_id"]})
                names["cube_name"] = cube.get("name")
            if "cache_id" in args:
                for c in self.executor.execute("list_cube_caches", {}):
                    if c["cache_id"] == args["cache_id"]:
                        names["cache_name"] = c.get("cube_name")
                        break
            if "object_id" in args and tool == "delete_object":
                for o in self.executor.execute("search_objects", {
                        "project_id": args["project_id"], "name": "",
                        "object_type": args.get("object_type", "cube")}):
                    if o["id"].lower() == args["object_id"].lower():
                        names["object_name"] = o.get("name")
                        break
            if "job_id" in args:
                for j in self.executor.execute("list_jobs", {}):
                    if j.get("job_id") == args["job_id"]:
                        names["job_desc"] = f"{j.get('type', '')} — {j.get('object', '')}"
                        break
            if "schedule_id" in args:
                for s in self.executor.execute("list_schedules", {}):
                    if s["id"] == args["schedule_id"]:
                        names["schedule_name"] = s.get("name")
                        break
        except Exception:
            pass  # previews fall back to raw IDs
        return names

    def _cancel_open_actions(self, db: Session, conv: Conversation, reason: str) -> None:
        open_actions = (db.query(PendingAction)
                        .filter(PendingAction.conversation_id == conv.id,
                                PendingAction.status == "pending").all())
        for action in open_actions:
            self._resolve_action(db, action, "cancelled")
            self._append_tool_result(db, conv, action.tool_use_id,
                                     f"Not executed ({reason}).")
            self._audit(db, conv.user_id, conv.id, "action_cancelled",
                        tool=action.tool_name, detail={"reason": reason})

    def _resolve_action(self, db: Session, action: PendingAction, status: str) -> None:
        action.status = status
        action.resolved_at = utcnow()
        db.commit()

    def _get_or_create_conversation(self, db: Session, user: str,
                                    conversation_id: str | None) -> Conversation:
        if conversation_id:
            conv = db.get(Conversation, conversation_id)
            if conv is not None and conv.user_id == user:
                return conv
        conv = Conversation(user_id=user)
        db.add(conv)
        db.commit()
        return conv

    def _load_history(self, db: Session, conv: Conversation) -> list[dict]:
        rows = (db.query(Message)
                .filter(Message.conversation_id == conv.id)
                .order_by(Message.id).all())
        return [{"role": r.role, "content": json.loads(r.raw_blocks)} for r in rows]

    def _append(self, db: Session, conv: Conversation, role: str, text: str,
                raw_blocks: list) -> None:
        db.add(Message(conversation_id=conv.id, role=role, text=text or "",
                       raw_blocks=json.dumps(raw_blocks)))
        conv.updated_at = utcnow()
        db.commit()

    def _append_tool_result(self, db: Session, conv: Conversation,
                            tool_use_id: str, content: str,
                            is_error: bool = False) -> None:
        block = {"type": "tool_result", "tool_use_id": tool_use_id,
                 "content": content}
        if is_error:
            block["is_error"] = True
        self._append(db, conv, "user", "", [block])

    def _audit(self, db: Session, user: str, conversation_id: str, event: str,
               tool: str = "", detail: dict | None = None) -> None:
        db.add(AuditLog(user_id=user, conversation_id=conversation_id,
                        event=event, tool_name=tool,
                        detail=json.dumps(detail or {})))
        db.commit()


def _aware(dt):
    """SQLite drops tzinfo; normalize to aware-UTC before comparing."""
    from datetime import timezone
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
