"""Test fixtures: fresh in-memory DB + mock executor + mock LLM per test."""

import os
import sys
from pathlib import Path

# Ensure `app` package resolves when pytest runs from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("STRATEGYAI_MOCK_MSTR", "true")
os.environ.setdefault("STRATEGYAI_MOCK_LLM", "true")
os.environ.setdefault("STRATEGYAI_DATABASE_URL", "sqlite://")  # in-memory

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import app.db as db_module
from app.db import Base


@pytest.fixture()
def db_session(monkeypatch):
    engine = create_engine("sqlite://",
                           connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    TestSession = sessionmaker(bind=engine, autoflush=False,
                               expire_on_commit=False)
    monkeypatch.setattr(db_module, "engine", engine)
    monkeypatch.setattr(db_module, "SessionLocal", TestSession)
    # routes.py imported SessionLocal by name — patch there too
    import app.api.routes as routes_module
    monkeypatch.setattr(routes_module, "SessionLocal", TestSession)
    Base.metadata.create_all(engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture()
def executor():
    from app.mstr.mock import MockMstrExecutor
    return MockMstrExecutor()


@pytest.fixture()
def agent(db_session, executor):
    from app.agent.llm import MockLLM
    from app.agent.loop import AgentLoop
    return AgentLoop(executor, llm=MockLLM())


@pytest.fixture()
def client(db_session, executor):
    from app.agent.llm import MockLLM
    from app.main import create_app
    app = create_app(executor=executor, llm=MockLLM())
    return TestClient(app)
