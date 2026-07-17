"""Live MSTR connectivity + endpoint validator.

READ-ONLY and safe to run against production — it never calls a mutating tool
(no pause/resume/delete/publish/refresh/kill). It logs in with the credentials
in your .env and exercises every read endpoint the agent uses, printing
PASS/FAIL per endpoint with the real response summary or the exact MSTR error.

Usage:
    1. Fill .env with real values and disable mock:
         STRATEGYAI_MOCK_MSTR=false
         MSTR_BASE_URL=https://YOUR-SERVER/MicroStrategyLibrary/api
         MSTR_USERNAME=...        MSTR_PASSWORD=...
         MSTR_VERIFY_SSL=false    # true if the server has a valid cert
    2. Run:
         venv\\Scripts\\python backend\\validate_live.py
    3. Optional targeting (else it auto-discovers):
         MSTR_VALIDATE_PROJECT_ID=<32-hex>   MSTR_VALIDATE_CUBE_ID=<32-hex>

Exit code 0 = every attempted endpoint passed.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import settings          # noqa: E402
from app.main import build_executor      # noqa: E402

# ANSI colors only when writing to a real terminal that wants them.
_COLOR = sys.stdout.isatty() and os.getenv("NO_COLOR") is None
GREEN = "\033[32m" if _COLOR else ""
RED = "\033[31m" if _COLOR else ""
YELLOW = "\033[33m" if _COLOR else ""
DIM = "\033[2m" if _COLOR else ""
RESET = "\033[0m" if _COLOR else ""


class Runner:
    def __init__(self, executor):
        self.ex = executor
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def check(self, label, tool, args):
        try:
            data = self.ex.execute(tool, args)
        except Exception as exc:
            self.failed += 1
            print(f"  {RED}[FAIL]{RESET} {label}")
            print(f"         {DIM}{tool}{RESET}  ->  {exc}")
            return None
        n = len(data) if isinstance(data, list) else 1
        summary = self._summarize(data)
        self.passed += 1
        print(f"  {GREEN}[PASS]{RESET} {label}  {DIM}({n} result(s)){RESET}"
              + (f"  {DIM}{summary}{RESET}" if summary else ""))
        return data

    def skip(self, label, why):
        self.skipped += 1
        print(f"  {YELLOW}[SKIP]{RESET} {label}  {DIM}{why}{RESET}")

    @staticmethod
    def _summarize(data):
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                for key in ("name", "cube_name", "group", "id"):
                    if key in first:
                        extra = "" if len(data) == 1 else " ..."
                        return f"e.g. {first[key]}{extra}"
        elif isinstance(data, dict):
            return data.get("name") or data.get("status") or ""
        return ""


def main():
    mode = "MOCK" if settings.mock_mstr else "LIVE"
    print("=" * 66)
    print(f" StrategyAI — MSTR endpoint validation   [{mode} MODE]")
    print(f" server: {settings.mstr_base_url or '(mock — set STRATEGYAI_MOCK_MSTR=false)'}")
    print("=" * 66)

    if not settings.mock_mstr and not settings.mstr_base_url:
        print(f"{RED}MSTR_BASE_URL is not set.{RESET} Add it to .env "
              "(must end in /MicroStrategyLibrary/api).")
        return 2

    try:
        ex = build_executor()
    except Exception as exc:
        print(f"{RED}Could not build the executor:{RESET} {exc}")
        return 2

    r = Runner(ex)

    print("\nAuth + projects")
    projects = r.check("list_projects", "list_projects", {})
    if not projects:
        print(f"\n{RED}Login or project listing failed — stopping.{RESET}")
        print("Check: base URL ends in /MicroStrategyLibrary/api, credentials, "
              "and that the account can reach the Library server.")
        return 1

    project_id = os.getenv("MSTR_VALIDATE_PROJECT_ID") or projects[0]["id"]
    pname = next((p["name"] for p in projects if p["id"] == project_id),
                 project_id)
    print(f"\nUsing project: {pname}  ({project_id})")

    print("\nSubscriptions + schedules")
    subs = r.check("list_subscriptions", "list_subscriptions",
                   {"project_id": project_id})
    r.check("list_all_subscriptions (cross-project)", "list_all_subscriptions", {})
    r.check("list_schedules", "list_schedules", {})
    if subs:
        sid = subs[0]["id"]
        r.check("get_subscription", "get_subscription",
                {"project_id": project_id, "subscription_id": sid})
        r.check("get_subscription_status", "get_subscription_status",
                {"project_id": project_id, "subscription_id": sid})
    else:
        r.skip("get_subscription / status", "no subscriptions in this project")

    print("\nCubes + caches + jobs")
    caches = r.check("list_cube_caches", "list_cube_caches",
                     {"project_id": project_id})
    r.check("get_cube_cache_usage", "get_cube_cache_usage", {})
    r.check("list_jobs", "list_jobs", {})

    cube_id = os.getenv("MSTR_VALIDATE_CUBE_ID")
    if not cube_id and caches:
        cube_id = caches[0].get("cube_id")
    if cube_id:
        r.check("get_cube_status", "get_cube_status",
                {"project_id": project_id, "cube_id": cube_id})
        r.check("get_cube_definition", "get_cube_definition",
                {"project_id": project_id, "cube_id": cube_id})
        r.check("get_object_dependencies (used_by)", "get_object_dependencies",
                {"project_id": project_id, "object_id": cube_id,
                 "direction": "used_by"})
    else:
        r.skip("cube status / definition / dependencies",
               "no cube found — set MSTR_VALIDATE_CUBE_ID=<32-hex> to test")

    search_term = os.getenv("MSTR_VALIDATE_SEARCH")
    if search_term:
        r.check(f"search_objects('{search_term}')", "search_objects",
                {"project_id": project_id, "name": search_term})
    else:
        r.skip("search_objects", "set MSTR_VALIDATE_SEARCH=<name> to test")

    if hasattr(ex, "client"):
        try:
            ex.client.logout()
        except Exception:
            pass

    print("\n" + "=" * 66)
    total = r.passed + r.failed
    color = GREEN if r.failed == 0 else RED
    print(f" {color}{r.passed}/{total} endpoints OK{RESET}"
          f"   ({r.skipped} skipped)")
    print("=" * 66)
    if r.failed:
        print("For each FAIL above, the tool name + MSTR error is shown — send "
              "me that line and I'll correct the exact call/payload.")
    return 0 if r.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
