"""API-level tests through the FastAPI TestClient (mock LLM + mock MSTR)."""

USER_A = {"X-Dev-User": "alice@corp.example"}
USER_B = {"X-Dev-User": "bob@corp.example"}


def test_healthz(client):
    res = client.get("/api/healthz")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}


def test_tools_endpoint(client):
    res = client.get("/api/tools")
    assert res.status_code == 200
    tools = res.json()
    assert len(tools) == 25
    mutating = {t["name"] for t in tools if t["mutating"]}
    assert mutating == {"pause_subscription", "resume_subscription",
                        "delete_subscription", "trigger_subscription_now",
                        "publish_cube", "refresh_cube", "unload_cube_cache",
                        "kill_job", "delete_object", "delete_schedule"}


def test_chat_and_confirm_end_to_end(client):
    res = client.post("/api/chat", headers=USER_A, json={
        "message": 'Pause "Daily Sales Email" in Finance Analytics'})
    assert res.status_code == 200
    body = res.json()
    assert body["kind"] == "confirm"
    action = body["pending_action"]
    assert action["tool_name"] == "pause_subscription"
    assert "Daily Sales Email" in action["preview"]

    res2 = client.post(f"/api/actions/{action['action_id']}/confirm",
                       headers=USER_A, json={"approved": True})
    assert res2.status_code == 200
    body2 = res2.json()
    assert body2["kind"] == "result"
    assert "paused" in body2["reply"]

    # the subscription now shows as disabled
    res3 = client.post("/api/chat", headers=USER_A, json={
        "conversation_id": body2["conversation_id"],
        "message": "Show subscriptions in Finance Analytics"})
    subs = next(r["data"] for r in res3.json()["results"]
                if r["tool"] == "list_subscriptions")
    daily = next(s for s in subs if s["name"] == "Daily Sales Email")
    assert daily["enabled"] is False


def test_confirm_requires_same_user(client):
    res = client.post("/api/chat", headers=USER_A, json={
        "message": 'Pause "Daily Sales Email" in Finance Analytics'})
    action_id = res.json()["pending_action"]["action_id"]

    res2 = client.post(f"/api/actions/{action_id}/confirm",
                       headers=USER_B, json={"approved": True})
    assert res2.status_code == 404  # existence not leaked to other users


def test_conversation_isolation(client):
    res = client.post("/api/chat", headers=USER_A,
                      json={"message": "List projects"})
    conv_id = res.json()["conversation_id"]

    mine = client.get(f"/api/conversations/{conv_id}/messages", headers=USER_A)
    assert mine.status_code == 200
    assert len(mine.json()) >= 2

    theirs = client.get(f"/api/conversations/{conv_id}/messages", headers=USER_B)
    assert theirs.status_code == 404


def test_unknown_action_404(client):
    res = client.post("/api/actions/deadbeef/confirm", headers=USER_A,
                      json={"approved": True})
    assert res.status_code == 404


def test_audit_endpoint_records_user(client):
    client.post("/api/chat", headers=USER_A, json={"message": "List projects"})
    res = client.get("/api/audit", headers=USER_A)
    assert res.status_code == 200
    events = res.json()
    assert any(e["event"] == "user_message" and e["user"] == "alice@corp.example"
               for e in events)


def test_empty_message_rejected(client):
    res = client.post("/api/chat", headers=USER_A, json={"message": ""})
    assert res.status_code == 422


def test_capability_miss_is_graceful(client):
    res = client.post("/api/chat", headers=USER_A,
                      json={"message": "Please restart the Intelligence Server"})
    body = res.json()
    assert body["kind"] == "text"
    assert "subscriptions" in body["reply"]  # help text listing capabilities
