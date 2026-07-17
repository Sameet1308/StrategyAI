"""Wire-level fidelity tests for RealMstrExecutor.

A fake client records every request and returns canned MSTR responses, so we
can assert the exact method/path/params/body against the verified March-2026
API spec (backend/MSTR_API_NOTES.md) without a live server.
"""

import json

import pytest

from app.mstr.errors import MstrApiError
from app.mstr.executors import RealMstrExecutor

PROJ = "B7CA92F04B9FAE8D941C3E9B7E0CD754"
SUB = "1A111111111111111111111111111111"
CUBE = "C111111111111111111111111111C111"


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body
        self.content = b"{}" if body is not None else b""

    def json(self):
        if self._body is None:
            raise ValueError("no body")
        return self._body


class FakeClient:
    """Route table keyed by 'METHOD path' -> body or (status, body)."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def request(self, method, path, *, project_id=None, params=None,
                json_body=None, headers=None, ok=(200, 201, 202, 204)):
        self.calls.append({"method": method, "path": path,
                           "project_id": project_id, "params": params,
                           "json_body": json_body, "headers": headers})
        key = f"{method} {path}"
        if key not in self.routes:
            raise MstrApiError(f"unexpected call {key}", status=404)
        entry = self.routes[key]
        status, body = entry if isinstance(entry, tuple) else (200, entry)
        if status not in ok:
            raise MstrApiError(f"{key} -> {status}", status=status)
        return FakeResponse(status, body)

    def get_json(self, path, **kw):
        return self.request("GET", path, **kw).json()


_SUB_BODY = {
    "id": SUB, "name": "Daily Sales Email",
    "owner": {"name": "j.smith"},
    "schedules": [{"id": "S1", "name": "Daily 6:00 AM"}],
    "recipients": [{"id": "U1", "name": "sales-leads@corp.example"}],
    "delivery": {"mode": "EMAIL", "softDisabled": False},
}


def _executor(routes):
    return RealMstrExecutor(client=FakeClient(routes))


def test_pause_uses_patch_soft_disabled():
    ex = _executor({
        f"GET /subscriptions/{SUB}": _SUB_BODY,
        f"PATCH /subscriptions/{SUB}": (204, None),
    })
    out = ex.execute("pause_subscription",
                     {"project_id": PROJ, "subscription_id": SUB})
    patch = ex.client.calls[-1]
    assert patch["method"] == "PATCH"
    assert patch["json_body"] == {"softDisabled": True}
    assert patch["project_id"] == PROJ
    assert out == {"id": SUB, "name": "Daily Sales Email", "enabled": False}


def test_resume_uses_patch_soft_disabled_false():
    ex = _executor({
        f"GET /subscriptions/{SUB}": _SUB_BODY,
        f"PATCH /subscriptions/{SUB}": (204, None),
    })
    out = ex.execute("resume_subscription",
                     {"project_id": PROJ, "subscription_id": SUB})
    assert ex.client.calls[-1]["json_body"] == {"softDisabled": False}
    assert out["enabled"] is True


def test_subscription_status_reads_status_endpoint():
    ex = _executor({
        f"GET /subscriptions/{SUB}": _SUB_BODY,
        f"GET /subscriptions/{SUB}/status": {
            "state": 3, "start": "2026-07-06 06:00", "failure": 0},
    })
    out = ex.execute("get_subscription_status",
                     {"project_id": PROJ, "subscription_id": SUB})
    assert ex.client.calls[-1]["path"] == f"/subscriptions/{SUB}/status"
    assert out["last_run_state"] == "success"    # state 3 = success
    assert out["failures"] == 0


def test_trigger_uses_v2_send():
    ex = _executor({
        f"GET /subscriptions/{SUB}": _SUB_BODY,
        f"POST /v2/subscriptions/{SUB}/send": (202, None),
    })
    out = ex.execute("trigger_subscription_now",
                     {"project_id": PROJ, "subscription_id": SUB})
    assert ex.client.calls[-1]["path"] == f"/v2/subscriptions/{SUB}/send"
    assert out["delivered"] is True


def test_refresh_and_publish_share_v2_cubes_endpoint():
    """There is NO /cubes/{id}/refresh — republish IS refresh."""
    routes = {
        "GET /cubes/": {"cubesInfos": [{"cubeName": "Finance Master Cube",
                                        "size": 851443712, "status": 96}]},
        f"POST /v2/cubes/{CUBE}": (202, {"id": "478:abc", "jobId": 478}),
    }
    for tool in ("refresh_cube", "publish_cube"):
        ex = _executor(routes)
        out = ex.execute(tool, {"project_id": PROJ, "cube_id": CUBE})
        post = ex.client.calls[-1]
        assert post["path"] == f"/v2/cubes/{CUBE}"
        assert post["json_body"] is None
        assert out["instance_id"] == "478"
        assert out["status"] == "processing"


def test_cube_status_decodes_bitfield():
    ex = _executor({
        "GET /cubes/": {"cubesInfos": [{"cubeName": "Finance Master Cube",
                                        "size": 851443712, "status": 96}]},
    })
    out = ex.execute("get_cube_status", {"project_id": PROJ, "cube_id": CUBE})
    assert out["status"] == "ready"          # 96 = READY(64) | LOADED(32)
    assert out["size_mb"] == 812.0
    info_call = ex.client.calls[0]
    assert info_call["params"] == {"id": CUBE}
    assert info_call["project_id"] == PROJ


def test_cube_status_processing_bit_wins():
    ex = _executor({
        "GET /cubes/": {"cubesInfos": [{"cubeName": "X", "size": 0,
                                        "status": 97}]},  # processing + ready
    })
    out = ex.execute("get_cube_status", {"project_id": PROJ, "cube_id": CUBE})
    assert out["status"] == "processing"


def test_search_uses_cube_subtypes():
    ex = _executor({
        "GET /searches/results": {"totalItems": 1, "result": [
            {"id": CUBE, "name": "Finance Master Cube", "type": 3,
             "subtype": 776}]},
    })
    out = ex.execute("search_objects", {"project_id": PROJ, "name": "Finance"})
    params = ex.client.calls[0]["params"]
    types = [v for k, v in params if k == "type"]
    assert types == [776, 779]               # OLAP cube + super cube
    assert ("pattern", 4) in params          # CONTAINS
    assert out[0]["name"] == "Finance Master Cube"


def test_list_caches_resolves_cluster_nodes():
    ex = _executor({
        "GET /monitors/iServer/nodes": {"nodes": [{"name": "node-1"}]},
        "GET /monitors/caches/cubes": {"cubeCaches": [
            {"id": "abc:def:xyz", "projectId": PROJ,
             "source": {"id": CUBE, "name": "Finance Master Cube"},
             "state": {"loadedState": "loaded"},
             "size": 1048576, "hitCount": 42}]},
    })
    out = ex.execute("list_cube_caches", {"project_id": PROJ})
    cache_call = ex.client.calls[-1]
    assert cache_call["params"]["clusterNode"] == "node-1"
    assert cache_call["params"]["projectIds"] == PROJ
    assert out[0]["size_mb"] == 1.0
    assert out[0]["status"] == "loaded"


def test_unload_cache_async_patch():
    ex = _executor({
        "PATCH /monitors/caches/cubes/abc:def:xyz":
            (202, {"manipulationId": "m1", "status": "executing"}),
    })
    out = ex.execute("unload_cube_cache", {"cache_id": "abc:def:xyz"})
    call = ex.client.calls[-1]
    assert call["headers"] == {"Prefer": "respond-async"}
    assert call["json_body"] == {"state": {"loadedState": "unloaded"}}
    assert out["unloaded"] is True


def test_subscription_summary_reads_soft_disabled():
    disabled = json.loads(json.dumps(_SUB_BODY))
    disabled["delivery"]["softDisabled"] = True
    ex = _executor({"GET /subscriptions": {"subscriptions": [_SUB_BODY, disabled]}})
    out = ex.execute("list_subscriptions", {"project_id": PROJ})
    assert out[0]["enabled"] is True
    assert out[1]["enabled"] is False
    assert out[0]["schedule"] == "Daily 6:00 AM"


def test_missing_cube_raises_not_found():
    ex = _executor({"GET /cubes/": {"cubesInfos": []}})
    with pytest.raises(MstrApiError) as err:
        ex.execute("get_cube_status", {"project_id": PROJ, "cube_id": CUBE})
    assert err.value.status == 404


# ------------------------------------------------------------ Tier A wire tests

def test_cube_definition_uses_v2():
    ex = _executor({
        f"GET /v2/cubes/{CUBE}": {"name": "Finance Master Cube", "definition": {
            "availableObjects": {
                "attributes": [{"name": "GL Account"}, {"name": "Entity"}],
                "metrics": [{"name": "Net Revenue"}]}}},
    })
    out = ex.execute("get_cube_definition", {"project_id": PROJ, "cube_id": CUBE})
    assert ex.client.calls[0]["path"] == f"/v2/cubes/{CUBE}"
    assert out["attributes"] == ["GL Account", "Entity"]
    assert out["metrics"] == ["Net Revenue"]


def test_run_cube_posts_instance():
    ex = _executor({
        f"POST /v2/cubes/{CUBE}/instances": {
            "instanceId": "IX9", "data": {"paging": {"total": 27300000}},
            "definition": {"grid": {"rows": [{"name": "Customer"}],
                                    "columns": [{"elements": [{"name": "LTV"}]}]}}},
    })
    out = ex.execute("run_cube", {"project_id": PROJ, "cube_id": CUBE, "row_limit": 5})
    call = ex.client.calls[-1]
    assert call["method"] == "POST" and call["path"] == f"/v2/cubes/{CUBE}/instances"
    assert call["params"] == {"offset": 0, "limit": 5}
    assert out["instance_id"] == "IX9"
    assert out["row_count"] == 27300000
    assert out["columns"] == ["Customer", "LTV"]


def test_list_all_subscriptions_posts_query():
    ex = _executor({
        "GET /projects": [{"id": PROJ, "name": "Finance Analytics", "status": 0}],
        "POST /subscriptions/query": {"subscriptions": [
            {"id": SUB, "name": "Daily Sales Email", "projectId": PROJ,
             "owner": {"name": "j.smith"}, "delivery": {"softDisabled": False}}]},
    })
    out = ex.execute("list_all_subscriptions", {})
    query = next(c for c in ex.client.calls if c["path"] == "/subscriptions/query")
    assert query["method"] == "POST"
    assert query["json_body"] == {"projectIds": [PROJ]}
    assert out[0]["project"] == "Finance Analytics"
    assert out[0]["enabled"] is True


def test_cache_usage_requires_cluster_node():
    ex = _executor({
        "GET /monitors/iServer/nodes": {"nodes": [{"name": "node-1"}]},
        "GET /monitors/caches/cubes/aggregatedUsages": {"aggregatedUsages": [
            {"name": "Finance Analytics", "size": 2097152, "count": 3}]},
    })
    out = ex.execute("get_cube_cache_usage", {"group_by": "project"})
    call = ex.client.calls[-1]
    assert call["params"]["clusterNode"] == "node-1"
    assert call["params"]["groupByObject"] == "project"
    assert out[0]["size_mb"] == 2.0


def test_list_jobs_uses_monitor_endpoint():
    ex = _executor({
        "GET /monitors/iServer/nodes": {"nodes": [{"name": "node-1"}]},
        "GET /monitors/jobs": {"jobs": [
            {"id": 4821, "jobType": "Cube Publish", "status": "executing",
             "userFullName": "svc", "objectName": "Finance Master Cube",
             "projectId": PROJ, "projectName": "Finance Analytics"}]},
    })
    out = ex.execute("list_jobs", {"project_id": PROJ})
    assert ex.client.calls[-1]["path"] == "/monitors/jobs"
    assert ex.client.calls[-1]["params"]["clusterNode"] == "node-1"
    assert out[0]["job_id"] == 4821


def test_dependencies_uses_metadata_search():
    ex = _executor({
        "POST /v2/metadataSearches/results": {"id": "srch1", "totalItems": 2},
        "GET /metadataSearches/results": {"result": [
            {"name": "Monthly P&L Report", "type": 3, "subtype": 768}]},
    })
    out = ex.execute("get_object_dependencies", {
        "project_id": PROJ, "object_id": CUBE, "direction": "used_by"})
    post = next(c for c in ex.client.calls if c["path"] == "/v2/metadataSearches/results")
    # direction used_by -> objects that USE the target -> usesObject param
    assert "usesObject" in post["params"]
    assert post["params"]["usesObject"] == f"{CUBE};3"
    assert out["dependents"][0]["name"] == "Monthly P&L Report"


def test_kill_job_deletes():
    ex = _executor({"DELETE /monitors/jobs/4822": (204, None)})
    out = ex.execute("kill_job", {"job_id": 4822})
    assert ex.client.calls[-1]["method"] == "DELETE"
    assert ex.client.calls[-1]["path"] == "/monitors/jobs/4822"
    assert out["cancelled"] is True


def test_delete_object_sends_type_param():
    ex = _executor({f"DELETE /objects/{CUBE}": (204, None)})
    out = ex.execute("delete_object", {
        "project_id": PROJ, "object_id": CUBE, "object_type": "cube"})
    call = ex.client.calls[-1]
    assert call["path"] == f"/objects/{CUBE}"
    assert call["params"] == {"type": 3}          # cube/report object type
    assert call["project_id"] == PROJ
    assert out["deleted"] is True


def test_delete_schedule_deletes():
    ex = _executor({"DELETE /schedules/SCH1": (204, None)})
    out = ex.execute("delete_schedule", {"schedule_id": "SCH1"})
    assert ex.client.calls[-1]["path"] == "/schedules/SCH1"
    assert out["deleted"] is True
