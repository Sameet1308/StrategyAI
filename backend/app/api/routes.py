"""HTTP API: chat, confirmation, conversation history, audit, tool listing."""

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..agent.loop import AgentLoop, AgentReply
from ..agent.registry import TOOLS
from ..db import SessionLocal
from ..identity import get_current_user
from ..models import AuditLog, Conversation, Message
from ..schemas import (ChatRequest, ChatResponse, ConfirmRequest,
                       PendingActionOut)

router = APIRouter(prefix="/api")

_loop: AgentLoop | None = None


def set_agent_loop(loop: AgentLoop) -> None:
    global _loop
    _loop = loop


def get_agent_loop() -> AgentLoop:
    assert _loop is not None, "Agent loop not initialized"
    return _loop


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _to_response(reply: AgentReply) -> ChatResponse:
    pending = None
    if reply.pending_action is not None:
        a = reply.pending_action
        pending = PendingActionOut(
            action_id=a.id, tool_name=a.tool_name,
            arguments=json.loads(a.arguments), preview=a.preview,
            expires_at=a.expires_at.isoformat())
    return ChatResponse(conversation_id=reply.conversation_id, kind=reply.kind,
                        reply=reply.reply, pending_action=pending,
                        results=reply.results)


@router.get("/healthz")
def healthz():
    return {"status": "ok"}


@router.get("/tools")
def list_tools():
    return [{"name": t.name, "description": t.description,
             "mutating": t.mutating} for t in TOOLS.values()]


@router.post("/chat", response_model=ChatResponse)
def chat(body: ChatRequest, user: str = Depends(get_current_user),
         db: Session = Depends(get_db),
         loop: AgentLoop = Depends(get_agent_loop)):
    reply = loop.handle_chat(db, user, body.conversation_id, body.message)
    return _to_response(reply)


@router.post("/actions/{action_id}/confirm", response_model=ChatResponse)
def confirm(action_id: str, body: ConfirmRequest,
            user: str = Depends(get_current_user),
            db: Session = Depends(get_db),
            loop: AgentLoop = Depends(get_agent_loop)):
    reply = loop.handle_confirm(db, user, action_id, body.approved)
    if reply.kind == "error" and not reply.conversation_id:
        raise HTTPException(status_code=404, detail=reply.reply)
    return _to_response(reply)


@router.get("/conversations/{conversation_id}/messages")
def conversation_messages(conversation_id: str,
                          user: str = Depends(get_current_user),
                          db: Session = Depends(get_db)):
    conv = db.get(Conversation, conversation_id)
    if conv is None or conv.user_id != user:
        raise HTTPException(status_code=404, detail="Conversation not found")
    rows = (db.query(Message)
            .filter(Message.conversation_id == conversation_id)
            .order_by(Message.id).all())
    return [{"role": r.role, "text": r.text,
             "created_at": r.created_at.isoformat()}
            for r in rows if r.text]


@router.get("/audit")
def audit(limit: int = 50, user: str = Depends(get_current_user),
          db: Session = Depends(get_db)):
    limit = max(1, min(limit, 500))
    rows = (db.query(AuditLog).order_by(AuditLog.id.desc()).limit(limit).all())
    return [{"ts": r.ts.isoformat(), "user": r.user_id, "event": r.event,
             "tool": r.tool_name, "conversation_id": r.conversation_id,
             "detail": json.loads(r.detail or "{}")} for r in rows]
