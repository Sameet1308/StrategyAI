"""Pydantic request/response models for the chat API."""

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    conversation_id: str | None = None
    message: str = Field(min_length=1, max_length=4000)


class ConfirmRequest(BaseModel):
    approved: bool


class PendingActionOut(BaseModel):
    action_id: str
    tool_name: str
    arguments: dict
    preview: str
    expires_at: str


class ToolData(BaseModel):
    tool: str
    data: list | dict


class ChatResponse(BaseModel):
    conversation_id: str
    kind: str  # "text" | "confirm" | "result" | "cancelled" | "error"
    reply: str
    pending_action: PendingActionOut | None = None
    results: list[ToolData] = []
