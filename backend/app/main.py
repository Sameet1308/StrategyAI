"""FastAPI application factory.

Serves the API and the built React bundle (backend/static). In the client
deployment the same container runs behind the internal ALB — no separate
static hosting (client constraint: no CloudFront/S3 website).
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .agent.loop import AgentLoop
from .api.routes import router, set_agent_loop
from .config import settings
from .db import init_db

STATIC_DIR = Path(__file__).resolve().parents[1] / "static"


def build_executor():
    if settings.mock_mstr:
        from .mstr.mock import MockMstrExecutor
        return MockMstrExecutor()
    from .mstr.executors import RealMstrExecutor
    return RealMstrExecutor()


def create_app(executor=None, llm=None) -> FastAPI:
    init_db()
    app = FastAPI(title="StrategyAI Admin Copilot", docs_url="/api/docs",
                  openapi_url="/api/openapi.json")

    # Dev-only convenience for `npm run dev` on :5173; same-origin in prod.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"], allow_headers=["*"],
    )

    set_agent_loop(AgentLoop(executor or build_executor(), llm=llm))
    app.include_router(router)

    if STATIC_DIR.is_dir():
        assets = STATIC_DIR / "assets"
        if assets.is_dir():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            candidate = (STATIC_DIR / path).resolve()
            if (path and candidate.is_file()
                    and candidate.is_relative_to(STATIC_DIR.resolve())):
                return FileResponse(candidate)
            return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
