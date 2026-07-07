"""Agent loop behavior: intent → resolve → confirm gate → execute → audit."""

import json
from datetime import timedelta

from app.agent.loop import AgentLoop
from app.models import AuditLog, PendingAction, utcnow
from app.mstr.mock import P_FIN

from .helpers import ScriptedLLM

USER = "admin@corp.example"


def _sub(executor, name):
    return next(s for s in executor.subscriptions if s["name"] == name)


# ---------------------------------------------------------------- read flows

def test_list_projects(agent, db_session):
    reply = agent.handle_chat(db_session, USER, None, "List projects")
    assert reply.kind == "text"
    assert "2 projects" in reply.reply
    assert reply.results and reply.results[0]["tool"] == "list_projects"
    assert len(reply.results[0]["data"]) == 2


def test_list_subscriptions_resolves_project_by_name(agent, db_session):
    reply = agent.handle_chat(db_session, USER, None,
                              "Show subscriptions in Finance Analytics")
    assert reply.kind == "text"
    assert "4 subscriptions" in reply.reply
    tools_used = [r["tool"] for r in reply.results]
    assert "list_projects" in tools_used      # resolved the name first
    assert "list_subscriptions" in tools_used


def test_ambiguous_project_asks(agent, db_session):
    reply = agent.handle_chat(db_session, USER, None, "Show subscriptions")
    assert reply.kind == "text"
    assert "Which project" in reply.reply
    # answering the question completes the flow in the same conversation
    reply2 = agent.handle_chat(db_session, USER, reply.conversation_id,
                               "Sales Operations")
    assert "4 subscriptions" in reply2.reply


def test_project_switch_mid_conversation(agent, db_session):
    """The most recent project mention wins over earlier ones."""
    reply = agent.handle_chat(db_session, USER, None,
                              "Show subscriptions in Finance Analytics")
    assert "4 subscriptions" in reply.reply
    reply2 = agent.handle_chat(
        db_session, USER, reply.conversation_id,
        'What is the status of the "Customer 360" cube in Sales Operations?')
    assert reply2.kind == "text"
    assert "Customer 360 Cube" in reply2.reply


def test_unqualified_object_asks_for_project(agent, db_session):
    """A quoted object name must not leak into project matching."""
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email"')
    assert reply.kind == "text"
    assert "Which project" in reply.reply
    reply2 = agent.handle_chat(db_session, USER, reply.conversation_id,
                               "Finance Analytics")
    assert reply2.kind == "confirm"
    assert reply2.pending_action.tool_name == "pause_subscription"


def test_subscription_status_is_read_only(agent, db_session):
    reply = agent.handle_chat(
        db_session, USER, None,
        'What is the delivery status of "Weekly P&L Distribution" in Finance Analytics?')
    assert reply.kind == "text"          # read-only: no confirmation card
    assert reply.pending_action is None
    assert "Weekly P&L Distribution" in reply.reply
    assert "success" in reply.reply


def test_cube_status_via_search(agent, db_session):
    reply = agent.handle_chat(
        db_session, USER, None,
        'What is the status of the "Customer 360" cube in Sales Operations?')
    assert reply.kind == "text"
    assert "Customer 360 Cube" in reply.reply
    assert "27,300,000" in reply.reply


# ------------------------------------------------------------- confirm gate

def test_mutating_tool_requires_confirmation(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    assert sub["enabled"] is True

    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    assert reply.kind == "confirm"
    assert reply.pending_action is not None
    assert reply.pending_action.tool_name == "pause_subscription"
    assert "Daily Sales Email" in reply.pending_action.preview
    assert "Finance Analytics" in reply.pending_action.preview
    # THE GATE: nothing executed yet
    assert sub["enabled"] is True


def test_confirm_executes_and_audits(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    action_id = reply.pending_action.id

    done = agent.handle_confirm(db_session, USER, action_id, approved=True)
    assert done.kind == "result"
    assert sub["enabled"] is False
    assert "paused" in done.reply

    action = db_session.get(PendingAction, action_id)
    assert action.status == "executed"
    events = [a.event for a in db_session.query(AuditLog).all()]
    for expected in ("user_message", "action_proposed", "action_confirmed",
                     "action_executed"):
        assert expected in events


def test_decline_does_not_execute(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    done = agent.handle_confirm(db_session, USER, reply.pending_action.id,
                                approved=False)
    assert done.kind == "cancelled"
    assert sub["enabled"] is True
    action = db_session.get(PendingAction, reply.pending_action.id)
    assert action.status == "cancelled"


def test_confirm_wrong_user_rejected(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    done = agent.handle_confirm(db_session, "intruder@corp.example",
                                reply.pending_action.id, approved=True)
    assert done.kind == "error"
    assert sub["enabled"] is True
    assert db_session.get(PendingAction, reply.pending_action.id).status == "pending"


def test_double_confirm_rejected(agent, db_session, executor):
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    agent.handle_confirm(db_session, USER, reply.pending_action.id, approved=True)
    again = agent.handle_confirm(db_session, USER, reply.pending_action.id,
                                 approved=True)
    assert again.kind == "error"
    assert "already executed" in again.reply


def test_expired_action_not_executed(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    action = db_session.get(PendingAction, reply.pending_action.id)
    action.expires_at = utcnow() - timedelta(seconds=1)
    db_session.commit()

    done = agent.handle_confirm(db_session, USER, action.id, approved=True)
    assert done.kind == "cancelled"
    assert "expired" in done.reply
    assert sub["enabled"] is True
    assert db_session.get(PendingAction, action.id).status == "expired"


def test_new_message_supersedes_pending_action(agent, db_session, executor):
    sub = _sub(executor, "Daily Sales Email")
    reply = agent.handle_chat(db_session, USER, None,
                              'Pause "Daily Sales Email" in Finance Analytics')
    action_id = reply.pending_action.id
    agent.handle_chat(db_session, USER, reply.conversation_id, "List projects")
    assert db_session.get(PendingAction, action_id).status == "cancelled"
    assert sub["enabled"] is True
    # a superseded action can no longer be confirmed
    late = agent.handle_confirm(db_session, USER, action_id, approved=True)
    assert late.kind == "error"
    assert sub["enabled"] is True


# ------------------------------------------------- validation + capability

def test_invalid_arguments_rejected_before_execution(db_session, executor):
    llm = ScriptedLLM([
        ("pause_subscription", {"project_id": "not-a-hex-id",
                                "subscription_id": "also-bad"}),
        "I could not do that.",
    ])
    agent = AgentLoop(executor, llm=llm)
    reply = agent.handle_chat(db_session, USER, None, "pause something")
    assert reply.kind == "text"
    assert db_session.query(PendingAction).count() == 0
    events = [a.event for a in db_session.query(AuditLog).all()]
    assert "validation_failed" in events


def test_unknown_tool_rejected(db_session, executor):
    llm = ScriptedLLM([
        ("restart_iserver", {}),
        "I can't restart servers yet.",
    ])
    agent = AgentLoop(executor, llm=llm)
    reply = agent.handle_chat(db_session, USER, None, "restart the server")
    assert reply.kind == "text"
    assert "can't restart" in reply.reply


def test_extra_arguments_rejected(db_session, executor):
    llm = ScriptedLLM([
        ("list_subscriptions", {"project_id": P_FIN, "evil": "x"}),
        "Sorry.",
    ])
    agent = AgentLoop(executor, llm=llm)
    agent.handle_chat(db_session, USER, None, "subs")
    events = [a.event for a in db_session.query(AuditLog).all()]
    assert "validation_failed" in events


def test_executor_error_surfaces_gracefully(db_session, executor):
    llm = ScriptedLLM([
        ("get_cube_status", {"project_id": P_FIN,
                             "cube_id": "D" * 32}),
        "That cube does not exist.",
    ])
    agent = AgentLoop(executor, llm=llm)
    reply = agent.handle_chat(db_session, USER, None, "cube status")
    assert reply.kind == "text"
    assert "does not exist" in reply.reply


def test_loop_bounded(db_session, executor):
    llm = ScriptedLLM([("list_projects", {})] * 50)
    agent = AgentLoop(executor, llm=llm)
    reply = agent.handle_chat(db_session, USER, None, "loop forever")
    assert reply.kind == "error"
    from app.config import settings
    assert llm.calls == settings.max_tool_turns


# ------------------------------------------------------------- cube actions

def test_refresh_cube_flow(agent, db_session, executor):
    reply = agent.handle_chat(db_session, USER, None,
                              'Refresh the "Customer 360" cube in Sales Operations')
    assert reply.kind == "confirm"
    assert reply.pending_action.tool_name == "refresh_cube"

    done = agent.handle_confirm(db_session, USER, reply.pending_action.id,
                                approved=True)
    assert done.kind == "result"
    assert "instance" in done.reply
    cube = next(c for c in executor.cubes if c["name"] == "Customer 360 Cube")
    assert cube["status"] == "processing"
    result = json.loads(db_session.get(PendingAction,
                                       reply.pending_action.id).result)
    assert result["instance_id"].startswith("inst-")


def test_delete_subscription_flow(agent, db_session, executor):
    count_before = len(executor.subscriptions)
    reply = agent.handle_chat(db_session, USER, None,
                              'Delete the "Churn Watchlist" subscription in Sales Operations')
    assert reply.kind == "confirm"
    assert "PERMANENTLY DELETE" in reply.pending_action.preview
    assert len(executor.subscriptions) == count_before  # gate holds

    done = agent.handle_confirm(db_session, USER, reply.pending_action.id, True)
    assert done.kind == "result"
    assert len(executor.subscriptions) == count_before - 1
