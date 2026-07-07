"""In-memory mock of the MicroStrategy environment.

Implements the same executor interface as the real client
(execute(tool_name, args) -> data, raising MstrApiError on failures) with
stateful seeded data, so the full agent loop — including mutations — runs
offline and in tests.
"""

import itertools

from .errors import MstrApiError

P_FIN = "B7CA92F04B9FAE8D941C3E9B7E0CD754"
P_SALES = "A2FD14E6423F1AB6C4B2E4914B65CD21"

_SUBS = [
    # (id, project, name, owner, schedule, delivery, recipients, enabled)
    ("1A111111111111111111111111111111", P_FIN, "Daily Sales Email", "j.smith",
     "Daily 6:00 AM", "EMAIL", ["sales-leads@corp.example"], True),
    ("2A222222222222222222222222222222", P_FIN, "Weekly P&L Distribution", "m.jones",
     "Monday 7:00 AM", "EMAIL", ["finance-team@corp.example"], True),
    ("3A333333333333333333333333333333", P_FIN, "Month-End Close Package", "m.jones",
     "Last day of month", "FILE", ["\\\\fileshare\\close"], True),
    ("4A444444444444444444444444444444", P_FIN, "Quarterly Board Pack", "c.wu",
     "Quarterly", "EMAIL", ["board-office@corp.example"], False),
    ("5A555555555555555555555555555555", P_SALES, "Pipeline Snapshot", "r.patel",
     "Daily 7:30 AM", "EMAIL", ["sales-ops@corp.example"], True),
    ("6A666666666666666666666666666666", P_SALES, "Territory Performance", "r.patel",
     "Weekly Friday", "EMAIL", ["region-vps@corp.example"], True),
    ("7A777777777777777777777777777777", P_SALES, "Churn Watchlist", "d.kim",
     "Daily 8:00 AM", "EMAIL", ["cs-managers@corp.example"], False),
    ("8A888888888888888888888888888888", P_SALES, "Comp Plan Statements", "d.kim",
     "Monthly 1st", "FILE", ["\\\\fileshare\\comp"], True),
]

_CUBES = [
    # (id, project, name, status, rows, size_mb, last_update)
    ("C111111111111111111111111111C111", P_FIN, "Finance Master Cube",
     "ready", 12_400_000, 812, "2026-07-06 04:12"),
    ("C222222222222222222222222222C222", P_FIN, "GL Transactions Cube",
     "ready", 48_900_000, 2_140, "2026-07-06 03:45"),
    ("C333333333333333333333333333C333", P_FIN, "Budget vs Actuals Cube",
     "outdated", 3_800_000, 260, "2026-06-30 22:10"),
    ("C444444444444444444444444444C444", P_SALES, "Sales Pipeline Cube",
     "ready", 9_100_000, 540, "2026-07-06 05:02"),
    ("C555555555555555555555555555C555", P_SALES, "Customer 360 Cube",
     "ready", 27_300_000, 1_675, "2026-07-05 23:58"),
]

_SCHEDULES = [
    ("SCH1111111111111111111111111111A", "Daily 6:00 AM", "time-based"),
    ("SCH2222222222222222222222222222B", "Daily 7:30 AM", "time-based"),
    ("SCH3333333333333333333333333333C", "Monday 7:00 AM", "time-based"),
    ("SCH4444444444444444444444444444D", "Database Load Complete", "event-based"),
]


class MockMstrExecutor:
    """Stateful mock. Mutations actually change the in-memory data."""

    def __init__(self) -> None:
        self._instance_seq = itertools.count(1)
        self.projects = [
            {"id": P_FIN, "name": "Finance Analytics", "status": "loaded"},
            {"id": P_SALES, "name": "Sales Operations", "status": "loaded"},
        ]
        self.subscriptions = [
            {"id": s[0], "project_id": s[1], "name": s[2], "owner": s[3],
             "schedule": s[4], "delivery": s[5], "recipients": s[6],
             "enabled": s[7]} for s in _SUBS
        ]
        self.cubes = [
            {"id": c[0], "project_id": c[1], "name": c[2], "status": c[3],
             "row_count": c[4], "size_mb": c[5], "last_update": c[6]}
            for c in _CUBES
        ]
        self.caches = [
            {"cache_id": f"cache-{c['id'][:8].lower()}", "cube_id": c["id"],
             "cube_name": c["name"], "project_id": c["project_id"],
             "node": "node-1", "size_mb": c["size_mb"],
             "hit_count": 40 + i * 17, "status": "loaded"}
            for i, c in enumerate(self.cubes) if c["status"] == "ready"
        ]
        self.schedules = [
            {"id": s[0], "name": s[1], "type": s[2]} for s in _SCHEDULES
        ]
        self.refresh_jobs: dict[str, int] = {}   # instance_id -> polls until done
        self.deliveries: list[dict] = []

    # ------------------------------------------------------------------

    def execute(self, tool: str, args: dict):
        handler = getattr(self, f"_{tool}", None)
        if handler is None:
            raise MstrApiError(f"Tool '{tool}' is not implemented")
        return handler(args)

    # ----- lookups -----------------------------------------------------

    def _project(self, project_id: str) -> dict:
        for p in self.projects:
            if p["id"].lower() == project_id.lower():
                return p
        raise MstrApiError(f"Project {project_id} not found", status=404)

    def _subscription(self, project_id: str, subscription_id: str) -> dict:
        self._project(project_id)
        for s in self.subscriptions:
            if (s["id"].lower() == subscription_id.lower()
                    and s["project_id"].lower() == project_id.lower()):
                return s
        raise MstrApiError(f"Subscription {subscription_id} not found in project",
                           status=404)

    def _cube(self, project_id: str, cube_id: str) -> dict:
        self._project(project_id)
        for c in self.cubes:
            if (c["id"].lower() == cube_id.lower()
                    and c["project_id"].lower() == project_id.lower()):
                return c
        raise MstrApiError(f"Cube {cube_id} not found in project", status=404)

    # ----- read tools ---------------------------------------------------

    def _list_projects(self, args):
        return self.projects

    def _search_objects(self, args):
        self._project(args["project_id"])
        term = args["name"].lower()
        limit = args.get("limit", 20)
        pool = self.cubes if args.get("object_type", "cube") == "cube" else []
        hits = [{"id": c["id"], "name": c["name"], "type": "cube",
                 "project_id": c["project_id"]}
                for c in pool
                if c["project_id"].lower() == args["project_id"].lower()
                and term in c["name"].lower()]
        return hits[:limit]

    def _list_subscriptions(self, args):
        self._project(args["project_id"])
        subs = [s for s in self.subscriptions
                if s["project_id"].lower() == args["project_id"].lower()]
        if args.get("filter_text"):
            f = args["filter_text"].lower()
            subs = [s for s in subs if f in s["name"].lower()]
        return subs

    def _get_subscription(self, args):
        return self._subscription(args["project_id"], args["subscription_id"])

    def _get_subscription_status(self, args):
        sub = self._subscription(args["project_id"], args["subscription_id"])
        delivered = any(d["subscription_id"] == sub["id"] for d in self.deliveries)
        return {
            "id": sub["id"],
            "name": sub["name"],
            "last_run_state": "success" if sub["enabled"] else "not_scheduled",
            "last_run": "2026-07-06 06:00" if sub["enabled"] else None,
            "delivered_this_session": delivered,
            "recipients": sub["recipients"],
            "failures": 0,
        }

    def _list_schedules(self, args):
        return self.schedules

    def _get_cube_status(self, args):
        return self._cube(args["project_id"], args["cube_id"])

    def _get_refresh_status(self, args):
        cube = self._cube(args["project_id"], args["cube_id"])
        instance_id = args.get("instance_id")
        if instance_id and instance_id in self.refresh_jobs:
            remaining = self.refresh_jobs[instance_id]
            if remaining > 0:
                self.refresh_jobs[instance_id] = remaining - 1
                return {"cube_id": cube["id"], "name": cube["name"],
                        "instance_id": instance_id, "status": "running"}
            cube["status"] = "ready"
            cube["last_update"] = "2026-07-06 12:00"
            return {"cube_id": cube["id"], "name": cube["name"],
                    "instance_id": instance_id, "status": "done"}
        return {"cube_id": cube["id"], "name": cube["name"],
                "instance_id": instance_id, "status": cube["status"]}

    def _list_cube_caches(self, args):
        caches = self.caches
        if args.get("project_id"):
            caches = [c for c in caches
                      if c["project_id"].lower() == args["project_id"].lower()]
        return caches

    # ----- mutating tools ------------------------------------------------

    def _pause_subscription(self, args):
        sub = self._subscription(args["project_id"], args["subscription_id"])
        if not sub["enabled"]:
            raise MstrApiError(f"Subscription “{sub['name']}” is already paused")
        sub["enabled"] = False
        return {"id": sub["id"], "name": sub["name"], "enabled": False}

    def _resume_subscription(self, args):
        sub = self._subscription(args["project_id"], args["subscription_id"])
        if sub["enabled"]:
            raise MstrApiError(f"Subscription “{sub['name']}” is already enabled")
        sub["enabled"] = True
        return {"id": sub["id"], "name": sub["name"], "enabled": True}

    def _delete_subscription(self, args):
        sub = self._subscription(args["project_id"], args["subscription_id"])
        self.subscriptions.remove(sub)
        return {"id": sub["id"], "name": sub["name"], "deleted": True}

    def _trigger_subscription_now(self, args):
        sub = self._subscription(args["project_id"], args["subscription_id"])
        self.deliveries.append({"subscription_id": sub["id"], "name": sub["name"]})
        return {"id": sub["id"], "name": sub["name"], "delivered": True,
                "recipients": sub["recipients"]}

    def _publish_cube(self, args):
        return self._start_cube_job(args)

    def _refresh_cube(self, args):
        return self._start_cube_job(args)

    def _start_cube_job(self, args):
        cube = self._cube(args["project_id"], args["cube_id"])
        instance_id = f"inst-{next(self._instance_seq):06d}"
        self.refresh_jobs[instance_id] = 1   # one 'running' poll, then done
        cube["status"] = "processing"
        return {"id": cube["id"], "name": cube["name"],
                "instance_id": instance_id, "status": "processing"}

    def _unload_cube_cache(self, args):
        for c in self.caches:
            if c["cache_id"] == args["cache_id"]:
                self.caches.remove(c)
                return {"cache_id": c["cache_id"], "cube_name": c["cube_name"],
                        "unloaded": True}
        raise MstrApiError(f"Cache {args['cache_id']} not found", status=404)
