"""The v1 tool registry: subscription management + cube operations.

Each tool is a typed contract between the LLM and the executor:
- input_schema is enforced with jsonschema BEFORE anything executes
- mutating tools NEVER execute directly — the loop turns them into a
  PendingAction that the user must confirm in the UI
- preview() renders the human-readable confirmation card text
"""

from dataclasses import dataclass, field
from typing import Callable

_OBJECT_ID = {"type": "string", "pattern": "^[A-Fa-f0-9]{32}$",
              "description": "32-character hex MicroStrategy object ID"}


def _schema(props: dict, required: list[str]) -> dict:
    return {
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": False,
    }


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict
    mutating: bool = False
    # (arguments, resolved_names) -> confirmation text; mutating tools only
    preview: Callable[[dict, dict], str] | None = field(default=None)


def _sub_label(args: dict, names: dict) -> str:
    sub = names.get("subscription_name") or args.get("subscription_id", "?")
    proj = names.get("project_name") or args.get("project_id", "?")
    return f"subscription “{sub}” in project “{proj}”"


def _cube_label(args: dict, names: dict) -> str:
    cube = names.get("cube_name") or args.get("cube_id", "?")
    proj = names.get("project_name") or args.get("project_id", "?")
    return f"cube “{cube}” in project “{proj}”"


TOOLS: dict[str, ToolSpec] = {}


def _register(spec: ToolSpec) -> None:
    TOOLS[spec.name] = spec


# ---------------------------------------------------------------- read-only

_register(ToolSpec(
    name="list_projects",
    description=(
        "List the MicroStrategy projects available to the service account. "
        "Call this first whenever the user names a project and you don't yet "
        "know its ID — never guess project IDs."),
    input_schema=_schema({}, []),
))

_register(ToolSpec(
    name="search_objects",
    description=(
        "Find objects (intelligent cubes or reports) by name within a project. "
        "Call this to resolve a cube name the user mentioned into its object ID "
        "before any cube operation. Supports partial names."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "name": {"type": "string", "minLength": 1,
                 "description": "Full or partial object name to search for"},
        "object_type": {"type": "string", "enum": ["cube", "report"],
                        "description": "Type of object to search (default cube)"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
    }, ["project_id", "name"]),
))

_register(ToolSpec(
    name="list_subscriptions",
    description=(
        "List all distribution-services subscriptions in a project, including "
        "name, owner, enabled state, schedule and recipients. Call this before "
        "acting on a subscription the user referenced by name."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "filter_text": {"type": "string",
                        "description": "Optional case-insensitive name filter"},
    }, ["project_id"]),
))

_register(ToolSpec(
    name="get_subscription",
    description="Get the full definition of one subscription by its ID.",
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
))

_register(ToolSpec(
    name="get_subscription_status",
    description=("Get the latest delivery status of a subscription: whether the "
                "last run succeeded or failed, and per-recipient detail. Use for "
                "questions like 'did the daily email go out?' or 'why did it fail?'."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
))

_register(ToolSpec(
    name="list_schedules",
    description=("List the schedules defined on the environment (used by "
                 "subscriptions). Read-only helper for schedule questions."),
    input_schema=_schema({}, []),
))

_register(ToolSpec(
    name="get_cube_status",
    description=("Get an intelligent cube's status, size, row count and last "
                 "update time. Use after resolving the cube ID via search_objects."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "cube_id": _OBJECT_ID,
    }, ["project_id", "cube_id"]),
))

_register(ToolSpec(
    name="get_refresh_status",
    description=("Check the progress of a cube publish/refresh started earlier "
                 "(202-accepted async operation)."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "cube_id": _OBJECT_ID,
        "instance_id": {"type": "string", "minLength": 1,
                        "description": "Instance ID returned when the refresh started"},
    }, ["project_id", "cube_id"]),
))

_register(ToolSpec(
    name="list_cube_caches",
    description=("List intelligent-cube caches on the Intelligence Server "
                 "cluster: cube name, size, hit count, status, node."),
    input_schema=_schema({
        "project_id": {**_OBJECT_ID,
                       "description": "Optional project filter (32-char hex ID)"},
    }, []),
))

# ----------------------------------------------------------------- mutating

_register(ToolSpec(
    name="pause_subscription",
    description=("Disable (pause) a subscription so it stops delivering. "
                 "Requires user confirmation before execution."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
    mutating=True,
    preview=lambda a, n: (f"Pause {_sub_label(a, n)}. Deliveries stop until it "
                          f"is resumed. No content is deleted."),
))

_register(ToolSpec(
    name="resume_subscription",
    description=("Re-enable (resume) a paused subscription so deliveries "
                 "restart on its schedule. Requires user confirmation."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
    mutating=True,
    preview=lambda a, n: (f"Resume {_sub_label(a, n)}. Deliveries restart on "
                          f"its existing schedule."),
))

_register(ToolSpec(
    name="delete_subscription",
    description=("Permanently delete a subscription. Irreversible. Requires "
                 "user confirmation."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
    mutating=True,
    preview=lambda a, n: (f"PERMANENTLY DELETE {_sub_label(a, n)}. This cannot "
                          f"be undone — recipients stop receiving it forever."),
))

_register(ToolSpec(
    name="trigger_subscription_now",
    description=("Send/deliver a subscription immediately, outside its "
                 "schedule. Requires user confirmation."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "subscription_id": _OBJECT_ID,
    }, ["project_id", "subscription_id"]),
    mutating=True,
    preview=lambda a, n: (f"Deliver {_sub_label(a, n)} to all its recipients "
                          f"right now, outside the normal schedule."),
))

_register(ToolSpec(
    name="publish_cube",
    description=("Publish an intelligent cube (first load into memory, or "
                 "full re-publish). Runs a full data pull against the "
                 "warehouse. Requires user confirmation."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "cube_id": _OBJECT_ID,
    }, ["project_id", "cube_id"]),
    mutating=True,
    preview=lambda a, n: (f"Publish {_cube_label(a, n)}. This executes the "
                          f"cube's SQL against the warehouse and loads the "
                          f"result into Intelligence Server memory."),
))

_register(ToolSpec(
    name="refresh_cube",
    description=("Refresh (republish) an already-published intelligent cube "
                 "with current warehouse data. Requires user confirmation."),
    input_schema=_schema({
        "project_id": _OBJECT_ID,
        "cube_id": _OBJECT_ID,
    }, ["project_id", "cube_id"]),
    mutating=True,
    preview=lambda a, n: (f"Refresh {_cube_label(a, n)} with current warehouse "
                          f"data. Reports using this cube may respond slower "
                          f"while the refresh runs."),
))

_register(ToolSpec(
    name="unload_cube_cache",
    description=("Unload (remove) an intelligent-cube cache from Intelligence "
                 "Server memory. The next request repopulates it. Requires "
                 "user confirmation."),
    input_schema=_schema({
        "cache_id": {"type": "string", "minLength": 1,
                     "description": "Cache ID from list_cube_caches"},
        "node_name": {"type": "string", "minLength": 1,
                      "description": "Cluster node holding the cache"},
    }, ["cache_id"]),
    mutating=True,
    preview=lambda a, n: (f"Unload cube cache "
                          f"“{n.get('cache_name', a.get('cache_id'))}” "
                          f"from Intelligence Server memory. The first report "
                          f"that needs it afterwards will be slower while the "
                          f"cache rebuilds."),
))


def anthropic_tools() -> list[dict]:
    """Registry in Anthropic Messages API tool format."""
    return [
        {"name": t.name, "description": t.description, "input_schema": t.input_schema}
        for t in TOOLS.values()
    ]
