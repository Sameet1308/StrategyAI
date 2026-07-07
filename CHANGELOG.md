# Changelog

## Session 2 — 2026-07-07

### StrategyAI BOT — full end-to-end build (Phase 3 v1)
Built the AI admin copilot end to end: React chat UI + FastAPI agent backend +
Claude (Bedrock/Anthropic) tool-use loop + live MicroStrategy REST executor.

**Backend (`backend/`)**
- Agent loop (`app/agent/loop.py`): intent → converse → capability check →
  JSON-Schema payload validation → **code-enforced confirmation gate** →
  execute → audit. Mutating tools cannot run without an explicit user confirm.
- Tool registry (`app/agent/registry.py`): 16 tools — subscriptions
  (list/get/status/pause/resume/delete/trigger), cubes
  (search/status/publish/refresh/refresh-status/list-caches/unload-cache),
  projects, schedules. Strict schemas (`additionalProperties: false`).
- LLM providers (`app/agent/llm.py`): Bedrock (`anthropic[bedrock]` Mantle),
  direct Anthropic API (local live with just an API key), and a deterministic
  MockLLM for offline dev/tests. Selected by `STRATEGYAI_LLM_PROVIDER`.
- MSTR executor (`app/mstr/executors.py` + `client.py`): endpoints verified
  against mstrio-py + the Strategy REST 2026 OpenAPI spec (see
  `backend/MSTR_API_NOTES.md`). Publish/refresh = `POST /v2/cubes/{id}`;
  pause/resume = `PATCH /subscriptions/{id} {"softDisabled": bool}`; send-now =
  `POST /v2/subscriptions/{id}/send`; caches need clusterNode resolution.
- Persistence (`app/models.py`): conversations, messages (raw Anthropic content
  blocks), pending actions with TTL, audit log keyed to the real user.
  SQLite locally, RDS Postgres via `STRATEGYAI_DATABASE_URL`.
- Identity (`app/identity.py`): ALB + Okta OIDC header in prod, `X-Dev-User`
  locally. FastAPI serves the built React SPA (no CloudFront/S3).

**Frontend (`frontend/`)**: Vite + React chat with confirmation cards, result
tables, superseded-action handling; built into `backend/static/`.

**Tests**: 56 pytest tests (agent loop, confirm gate, cross-user security,
wire-level MSTR fidelity, live-provider wiring) — all passing.

**Verified in-browser** (mock mode): pause-subscription confirm→execute,
cube-status, delivery-status, decline-keeps-data, missing-payload→asks.

### Roadmap update
- Phase 1 (scripts) and Phase 2 (REST framework) complete; Phase 3 (agent) v1
  shipped with scope = subscriptions + cubes. Adding a capability = one registry
  entry + one executor method.

## Session 1 — 2026-03-29

### Project Initialization
- Created project structure for **StrategyAI** — MicroStrategy (Strategy) Sep 2025 admin automation
- Set up `core/auth.py` — reusable authenticated session with Strategy REST API (login/logout, GET/POST/PUT/DELETE)
- Set up `core/config.py` — .env-based configuration loader
- Created `scripts/` directory for standalone admin scripts
- Created `notes/` directory for daily learning notes (.docx)
- Created `.env.example`, `.gitignore`, `requirements.txt`
- Created `CLAUDE.md` with project architecture and rules

### Current Phase
- **Phase 1**: Standalone Python scripts for daily admin tasks
- Next: Build first admin scripts (user management, cache clearing, server status, etc.)
