"""Real MSTR executor: tool name -> verified REST call.

Every endpoint here was verified against mstrio-py (the vendor's own SDK)
and the "Strategy REST 2026" OpenAPI spec — details and gotchas in
backend/MSTR_API_NOTES.md. The important non-obvious facts:

- Publish and refresh are the SAME endpoint: POST /v2/cubes/{cubeId}
  (202 + jobId). There is no /cubes/{id}/refresh.
- Pause/resume is PATCH /subscriptions/{id} {"softDisabled": bool} (204);
  the current state reads from delivery.softDisabled on GET.
- Send-now is POST /v2/subscriptions/{id}/send (202, empty body).
- Cube search uses subtype codes: 776 = OLAP cube, 779 = super cube.
- /monitors/caches/cubes REQUIRES clusterNode (from /monitors/iServer/nodes).
- Cache unload is PATCH /monitors/caches/cubes/{cacheId} with
  'Prefer: respond-async' and {"state": {"loadedState": "unloaded"}}.

Outputs are normalized to the same shapes MockMstrExecutor returns, so the
LLM, previews, and the UI behave identically against mock and live systems.
"""

from .client import MstrRestClient
from .errors import MstrApiError

_CUBE_SUBTYPES = (776, 779)          # OLAP cube, super cube
_REPORT_SUBTYPES = (768, 769, 774)   # grid, graph, grid&graph

# EnumDSSCubeStates bit flags (X-MSTR-CubeStatus / cubesInfos[].status)
_BIT_PROCESSING = 1
_BIT_LOADED = 32
_BIT_READY = 64
_BIT_DIRTY = 16


def _cube_status_label(bits: int) -> str:
    if bits & _BIT_PROCESSING:
        return "processing"
    if bits & _BIT_DIRTY:
        return "outdated"
    if bits & _BIT_READY and bits & _BIT_LOADED:
        return "ready"
    if bits & _BIT_READY:
        return "ready (not loaded)"
    return f"state({bits})"


class RealMstrExecutor:
    def __init__(self, client: MstrRestClient | None = None) -> None:
        self.client = client or MstrRestClient()
        self._nodes_cache: list[str] | None = None

    def execute(self, tool: str, args: dict):
        handler = getattr(self, f"_{tool}", None)
        if handler is None:
            raise MstrApiError(f"Tool '{tool}' is not implemented")
        return handler(args)

    # ----- read tools ---------------------------------------------------

    def _list_projects(self, args):
        data = self.client.get_json("/projects")
        return [{"id": p["id"], "name": p["name"],
                 "status": "loaded" if p.get("status") == 0 else str(p.get("status"))}
                for p in data]

    def _search_objects(self, args):
        subtypes = _CUBE_SUBTYPES if args.get("object_type", "cube") == "cube" \
            else _REPORT_SUBTYPES
        params = [("name", args["name"]), ("pattern", 4),  # 4 = CONTAINS
                  ("limit", args.get("limit", 20))]
        params += [("type", st) for st in subtypes]
        data = self.client.get_json("/searches/results",
                                    project_id=args["project_id"], params=params)
        return [{"id": o["id"], "name": o["name"],
                 "type": args.get("object_type", "cube"),
                 "project_id": args["project_id"]}
                for o in data.get("result", [])]

    def _list_subscriptions(self, args):
        data = self.client.get_json("/subscriptions",
                                    project_id=args["project_id"])
        subs = [self._sub_summary(s, args["project_id"])
                for s in data.get("subscriptions", [])]
        if args.get("filter_text"):
            f = args["filter_text"].lower()
            subs = [s for s in subs if f in s["name"].lower()]
        return subs

    def _get_subscription(self, args):
        data = self.client.get_json(f"/subscriptions/{args['subscription_id']}",
                                    project_id=args["project_id"])
        return self._sub_summary(data, args["project_id"])

    def _get_subscription_status(self, args):
        sub = self._get_subscription(args)
        data = self.client.get_json(
            f"/subscriptions/{args['subscription_id']}/status",
            project_id=args["project_id"])
        # state: 0=unknown 1=ready 2=executing 3=success 4=error (per spec enum)
        state_map = {1: "ready", 2: "executing", 3: "success", 4: "error"}
        return {
            "id": sub["id"],
            "name": sub["name"],
            "last_run_state": state_map.get(data.get("state"), "unknown"),
            "last_run": data.get("start"),
            "failures": data.get("failure", 0),
            "recipients": sub["recipients"],
        }

    def _list_schedules(self, args):
        data = self.client.get_json("/v2/schedules", params={"limit": 500})
        return [{"id": s["id"], "name": s["name"],
                 "type": s.get("scheduleType", "unknown").replace("_", "-")}
                for s in data.get("schedules", [])]

    def _get_cube_status(self, args):
        info = self._cube_info(args["project_id"], args["cube_id"])
        return info

    def _get_refresh_status(self, args):
        instance_id = args.get("instance_id")
        if instance_id:
            resp = self.client.request(
                "GET", f"/v2/monitors/jobs/{instance_id}",
                project_id=args["project_id"], ok=(200, 400, 404))
            if resp.status_code == 200:
                job = resp.json()
                return {"cube_id": args["cube_id"],
                        "instance_id": instance_id,
                        "status": str(job.get("status", "running")).lower()}
            # job purged => finished; fall through to the cube's own status
        info = self._cube_info(args["project_id"], args["cube_id"])
        status = "running" if info["status"] == "processing" else "done"
        return {"cube_id": args["cube_id"], "name": info["name"],
                "instance_id": instance_id, "status": status}

    def _list_cube_caches(self, args):
        caches = []
        for node in self._nodes():
            params = {"clusterNode": node, "limit": 1000}
            if args.get("project_id"):
                params["projectIds"] = args["project_id"]
            data = self.client.get_json("/monitors/caches/cubes", params=params)
            for c in data.get("cubeCaches", []):
                caches.append({
                    "cache_id": c["id"],
                    "cube_id": c.get("source", {}).get("id", ""),
                    "cube_name": c.get("source", {}).get("name", ""),
                    "project_id": c.get("projectId", ""),
                    "node": node,
                    "size_mb": round(c.get("size", 0) / (1024 * 1024), 1),
                    "hit_count": c.get("hitCount", 0),
                    "status": c.get("state", {}).get("loadedState", "unknown"),
                })
        return caches

    def _get_cube_definition(self, args):
        data = self.client.get_json(f"/v2/cubes/{args['cube_id']}",
                                    project_id=args["project_id"])
        defn = data.get("definition", {}) or {}
        available = defn.get("availableObjects", defn)
        attrs = [a.get("name") for a in available.get("attributes", []) or []]
        metrics = [m.get("name") for m in available.get("metrics", []) or []]
        return {"id": args["cube_id"], "name": data.get("name", ""),
                "attributes": [a for a in attrs if a],
                "metrics": [m for m in metrics if m]}

    def _run_cube(self, args):
        limit = args.get("row_limit", 10)
        resp = self.client.request(
            "POST", f"/v2/cubes/{args['cube_id']}/instances",
            project_id=args["project_id"],
            params={"offset": 0, "limit": limit}, json_body={}, ok=(200, 201))
        body = resp.json() if resp.content else {}
        grid = (body.get("definition", {}) or {}).get("grid", {}) or {}
        columns = [r.get("name") for r in grid.get("rows", []) or [] if r.get("name")]
        for hdr in grid.get("columns", []) or []:
            columns += [e.get("name") for e in hdr.get("elements", []) or []
                        if e.get("name")]
        total = (((body.get("data", {}) or {}).get("paging", {}) or {})
                 .get("total"))
        return {"id": args["cube_id"], "instance_id": body.get("instanceId"),
                "row_count": total, "columns": columns[:25]}

    def _list_all_subscriptions(self, args):
        project_ids = [p["id"] for p in self._list_projects({})]
        resp = self.client.request("POST", "/subscriptions/query",
                                   json_body={"projectIds": project_ids},
                                   ok=(200,))
        body = resp.json() if resp.content else {}
        name_by_id = {p["id"]: p["name"] for p in self._list_projects({})}
        subs = []
        for s in body.get("subscriptions", []) or []:
            pid = s.get("projectId", "")
            subs.append({"id": s.get("id"), "name": s.get("name", ""),
                         "project": name_by_id.get(pid, pid),
                         "owner": (s.get("owner", {}) or {}).get("name", ""),
                         "enabled": not (s.get("delivery", {}) or {})
                         .get("softDisabled", False)})
        if args.get("filter_text"):
            f = args["filter_text"].lower()
            subs = [s for s in subs if f in (s["name"] or "").lower()]
        return subs

    def _get_cube_cache_usage(self, args):
        group_by = args.get("group_by", "project")
        out = []
        for node in self._nodes():
            data = self.client.get_json(
                "/monitors/caches/cubes/aggregatedUsages",
                params={"clusterNode": node, "groupByObject": group_by})
            for u in (data.get("aggregatedUsages")
                      or data.get("usages") or data.get("usage") or []):
                out.append({
                    "group": u.get("name") or u.get("groupName", ""),
                    "size_mb": round(int(u.get("size", 0) or 0) / (1024 * 1024), 1),
                    "cache_count": u.get("count", u.get("cacheCount", 0)),
                })
        return out

    def _list_jobs(self, args):
        jobs = []
        for node in self._nodes():
            data = self.client.get_json("/monitors/jobs",
                                        params={"clusterNode": node})
            for j in data.get("jobs", []) or []:
                if (args.get("project_id") and (j.get("projectId", "") or "").lower()
                        != args["project_id"].lower()):
                    continue
                jobs.append({
                    "job_id": j.get("id") or j.get("jobId"),
                    "type": j.get("jobType") or j.get("type", ""),
                    "status": j.get("status", ""),
                    "user": j.get("userFullName") or j.get("user", ""),
                    "object": j.get("objectName", ""),
                    "project": j.get("projectName", ""),
                })
        return jobs

    def _get_object_dependencies(self, args):
        # MSTR dependency-search param naming is inverted vs. plain English:
        #   usedByObject  -> objects the target USES  (its own dependencies)
        #   usesObject    -> objects that USE the target (things depending on it)
        # Verify against the client's server if the two directions look swapped.
        type_code = {"cube": 3, "report": 3, "document": 55}.get(
            args.get("object_type", "cube"), 3)
        ref = f"{args['object_id']};{type_code}"
        param = "usedByObject" if args["direction"] == "uses" else "usesObject"
        created = self.client.request(
            "POST", "/v2/metadataSearches/results",
            project_id=args["project_id"], params={param: ref},
            ok=(200, 201)).json()
        search_id = created.get("id")
        results = self.client.get_json(
            "/metadataSearches/results", project_id=args["project_id"],
            params={"searchId": search_id, "limit": 100})
        items = results if isinstance(results, list) \
            else results.get("result") or results.get("objects") or []
        return {"object_id": args["object_id"], "direction": args["direction"],
                "dependents": [{"name": o.get("name"),
                                "type": o.get("type"),
                                "subtype": o.get("subtype")} for o in items]}

    # ----- mutating tools ------------------------------------------------

    def _pause_subscription(self, args):
        return self._set_soft_disabled(args, disabled=True)

    def _resume_subscription(self, args):
        return self._set_soft_disabled(args, disabled=False)

    def _set_soft_disabled(self, args, disabled: bool):
        sub = self._get_subscription(args)   # 404s early with a clear message
        self.client.request(
            "PATCH", f"/subscriptions/{args['subscription_id']}",
            project_id=args["project_id"],
            json_body={"softDisabled": disabled}, ok=(200, 204))
        return {"id": sub["id"], "name": sub["name"], "enabled": not disabled}

    def _delete_subscription(self, args):
        sub = self._get_subscription(args)
        self.client.request(
            "DELETE", f"/subscriptions/{args['subscription_id']}",
            project_id=args["project_id"], ok=(200, 204))
        return {"id": sub["id"], "name": sub["name"], "deleted": True}

    def _trigger_subscription_now(self, args):
        sub = self._get_subscription(args)
        self.client.request(
            "POST", f"/v2/subscriptions/{args['subscription_id']}/send",
            project_id=args["project_id"], ok=(200, 202, 204))
        return {"id": sub["id"], "name": sub["name"], "delivered": True,
                "recipients": sub.get("recipients", [])}

    def _publish_cube(self, args):
        return self._start_cube_job(args)

    def _refresh_cube(self, args):
        # Republish IS refresh — same endpoint by design (see module docstring)
        return self._start_cube_job(args)

    def _start_cube_job(self, args):
        info = self._cube_info(args["project_id"], args["cube_id"])
        resp = self.client.request("POST", f"/v2/cubes/{args['cube_id']}",
                                   project_id=args["project_id"],
                                   ok=(200, 201, 202))
        body = resp.json() if resp.content else {}
        job_id = str(body.get("jobId") or body.get("id") or "")
        return {"id": args["cube_id"], "name": info["name"],
                "instance_id": job_id, "status": "processing"}

    def _unload_cube_cache(self, args):
        self.client.request(
            "PATCH", f"/monitors/caches/cubes/{args['cache_id']}",
            headers={"Prefer": "respond-async"},
            json_body={"state": {"loadedState": "unloaded"}},
            ok=(200, 202))
        return {"cache_id": args["cache_id"], "unloaded": True}

    def _kill_job(self, args):
        self.client.request("DELETE", f"/monitors/jobs/{args['job_id']}",
                            ok=(200, 204))
        return {"job_id": args["job_id"], "cancelled": True}

    def _delete_object(self, args):
        type_code = {"cube": 3, "report": 3, "document": 55}.get(
            args.get("object_type", "cube"), 3)
        self.client.request("DELETE", f"/objects/{args['object_id']}",
                            project_id=args["project_id"],
                            params={"type": type_code}, ok=(200, 204))
        return {"object_id": args["object_id"],
                "object_type": args.get("object_type"), "deleted": True}

    def _delete_schedule(self, args):
        self.client.request("DELETE", f"/schedules/{args['schedule_id']}",
                            ok=(200, 204))
        return {"schedule_id": args["schedule_id"], "deleted": True}

    # ----- helpers --------------------------------------------------------

    def _sub_summary(self, s: dict, project_id: str) -> dict:
        delivery = s.get("delivery", {}) or {}
        schedules = s.get("schedules", []) or []
        recipients = [r.get("name") or r.get("id", "")
                      for r in s.get("recipients", []) or []]
        owner = s.get("owner", {}) or {}
        return {
            "id": s["id"],
            "project_id": project_id,
            "name": s.get("name", ""),
            "owner": owner.get("name") or owner.get("abbreviation", ""),
            "schedule": schedules[0].get("name", "") if schedules else "",
            "delivery": delivery.get("mode", ""),
            "recipients": recipients,
            "enabled": not delivery.get("softDisabled", False),
        }

    def _cube_info(self, project_id: str, cube_id: str) -> dict:
        data = self.client.get_json("/cubes/", project_id=project_id,
                                    params={"id": cube_id})
        infos = data.get("cubesInfos") or []
        if not infos:
            raise MstrApiError(f"Cube {cube_id} not found in project", status=404)
        c = infos[0]
        bits = int(c.get("status", 0) or 0)
        return {
            "id": cube_id,
            "project_id": project_id,
            "name": c.get("cubeName", ""),
            "status": _cube_status_label(bits),
            "size_mb": round(int(c.get("size", 0) or 0) / (1024 * 1024), 1),
            "row_count": c.get("rowCount"),
            "last_update": c.get("lastUpdateTime") or c.get("modificationTime", ""),
        }

    def _nodes(self) -> list[str]:
        if self._nodes_cache is None:
            data = self.client.get_json("/monitors/iServer/nodes")
            self._nodes_cache = [n["name"] for n in data.get("nodes", [])
                                 if n.get("name")]
            if not self._nodes_cache:
                raise MstrApiError("No Intelligence Server nodes visible", status=502)
        return self._nodes_cache
