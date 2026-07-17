# StrategyAI — MicroStrategy (Strategy) Admin Automation

## Overview
AI-powered admin copilot and REST API framework for **MicroStrategy (Strategy One), March 2026** release.
Two workstreams: standalone Python scripts (`scripts/`) and the **StrategyAI BOT** (`backend/` + `frontend/`) —
a chat agent that turns natural language into confirmed MicroStrategy admin actions.

## Roadmap
1. **Phase 1 (done)**: Standalone Python scripts for admin daily tasks
2. **Phase 2 (done)**: REST API framework wrapping Strategy REST API (`backend/app/mstr/`)
3. **Phase 3 (current)**: AI-powered agent — v1 scope: subscriptions + cube operations

## StrategyAI BOT (backend/ + frontend/)
- **Agent loop**: intent → converse → capability check → JSON-Schema payload
  validation → **code-enforced confirmation gate** → execute → audit.
  Mutating tools NEVER run without an explicit user confirm (`backend/app/agent/loop.py`).
- **LLM**: Claude on Amazon Bedrock (`anthropic[bedrock]` Mantle client,
  `BEDROCK_MODEL_ID`, default `anthropic.claude-sonnet-5`). `STRATEGYAI_MOCK_LLM=true`
  swaps in a deterministic mock so everything runs offline.
- **MSTR executor**: `backend/app/mstr/executors.py` — endpoints verified against
  mstrio-py + the Strategy REST 2026 OpenAPI spec; see `backend/MSTR_API_NOTES.md`.
  Notable: publish/refresh cube are BOTH `POST /v2/cubes/{id}`; pause/resume is
  `PATCH /subscriptions/{id} {"softDisabled": bool}`.
- **Persistence**: SQLAlchemy — SQLite locally, RDS Postgres in the client account
  (`STRATEGYAI_DATABASE_URL`). Audit log keyed to the real user.
- **Identity**: internal ALB + Okta OIDC header (`x-amzn-oidc-data`) in prod;
  `X-Dev-User` header / `STRATEGYAI_DEV_USER` locally.
- **UI**: React (Vite) chat with confirmation cards, built into `backend/static/`
  and served by FastAPI (client constraint: no CloudFront/S3 hosting).

### Run the BOT locally (mock mode — no credentials needed)
```bash
venv\Scripts\python -m pip install -r backend\requirements.txt
cd frontend && npm install && npm run build && cd ..
venv\Scripts\python -m uvicorn app.main:app --app-dir backend --port 8000
# open http://localhost:8000
```

### Test
```bash
venv\Scripts\python -m pytest backend\tests -q   # 74 tests, all must pass
```

### Go live against a real MicroStrategy server
1. In `.env`: `STRATEGYAI_MOCK_MSTR=false`, set `MSTR_BASE_URL` (must end in
   `/MicroStrategyLibrary/api`), `MSTR_USERNAME`, `MSTR_PASSWORD`, and choose an
   LLM provider (`STRATEGYAI_LLM_PROVIDER=anthropic` + `ANTHROPIC_API_KEY` is the
   easiest local live path; `bedrock` in the AWS deployment).
2. **Validate the live API calls before running the app** (read-only, safe on
   prod — never calls a mutating endpoint):
   ```bash
   venv\Scripts\python backend\validate_live.py
   ```
   It logs in and exercises every read endpoint, printing PASS/FAIL per call
   with the real MSTR error on failure. Endpoint shapes are verified against
   `backend/MSTR_API_NOTES.md`; this proves them against *your* server.
3. Start the app: `venv\Scripts\python -m uvicorn app.main:app --app-dir backend --port 8000`

## Strategy REST API
- Base URL pattern: `https://<server>/MicroStrategyLibrary/api`
- Auth: POST `/api/auth/login` — returns auth token in `X-MSTR-AuthToken` header
- API version: Strategy Sep 2025 (v2)
- Docs: Available at `https://<server>/MicroStrategyLibrary/api-docs/`

## Tech Stack
- **Python 3.11+**
- **requests** — HTTP calls to Strategy REST API
- **python-dotenv** — Environment variable management
- **.env** — All credentials and server URLs (NEVER committed)

## How to Run
```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy env template and fill in your values
cp .env.example .env

# 4. Run any script
python scripts/<script_name>.py
```

## Project Structure
```
StrategyAI/
├── scripts/           # Standalone admin scripts
│   └── ...
├── core/              # Shared utilities (auth, config, helpers)
│   ├── __init__.py
│   ├── auth.py        # Strategy REST API authentication
│   └── config.py      # Environment config loader
├── notes/             # Learning notes (.docx files)
├── .env.example       # Environment template
├── .gitignore
├── requirements.txt
├── CLAUDE.md          # This file
└── CHANGELOG.md       # Session tracking
```

## Rules
- All scripts must use `core/auth.py` for authentication — no inline auth
- All server URLs and credentials come from `.env`
- Each script should be self-contained and runnable independently
- Add docstring at top of each script explaining what it does
- Test scripts against a real Strategy environment before marking done
