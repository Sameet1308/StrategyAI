# StrategyAI — MicroStrategy (Strategy) Admin Automation

## Overview
AI-powered workflow orchestrator and REST API framework for **MicroStrategy (Strategy) Sep 2025** version.
Currently focused on **standalone Python scripts** for daily admin tasks.

## Roadmap
1. **Phase 1 (Current)**: Standalone Python scripts for admin daily tasks
2. **Phase 2**: REST API framework wrapping Strategy REST API
3. **Phase 3**: AI-powered workflow orchestrator

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
