"""SQLAlchemy engine/session setup.

SQLite locally (zero setup); RDS Postgres in the client deployment via
STRATEGYAI_DATABASE_URL — the models use only portable column types.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import settings


class Base(DeclarativeBase):
    pass


_connect_args = {}
if settings.database_url.startswith("sqlite"):
    # FastAPI handlers may touch the session from the threadpool.
    _connect_args["check_same_thread"] = False

engine = create_engine(settings.database_url, connect_args=_connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_db() -> None:
    from . import models  # noqa: F401  (register mappings)
    Base.metadata.create_all(engine)
