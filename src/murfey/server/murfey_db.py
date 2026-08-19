from __future__ import annotations

import sqlite3
from functools import partial

import yaml
from cryptography.fernet import Fernet
from fastapi import Depends
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine

from murfey.util.config import Security, get_security_config


@event.listens_for(Engine, "connect")
def _configure_sqlite_connection(dbapi_connection, connection_record):
    """Tune every SQLite connection; a no-op for Postgres.

    Doppio's feedback thread and micrograph watcher write concurrently, so the
    SQLite defaults (``busy_timeout=0``, ``DELETE`` journal) make the second
    writer fail instantly with "database is locked". WAL lets readers run
    alongside a single writer, ``busy_timeout`` makes writers wait for the lock
    instead of erroring, and ``synchronous=NORMAL`` (safe under WAL) skips an
    fsync per commit.
    """
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()


def url(security_config: Security | None = None) -> str:
    security_config = security_config or get_security_config()
    with open(security_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
    if security_config.db == "sqlite":
        return f"sqlite:///{creds['database']}"
    f = Fernet(security_config.crypto_key.encode("ascii"))
    p = f.decrypt(creds["password"].encode("ascii"))
    return f"postgresql+psycopg2://{creds['username']}:{p.decode()}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_murfey_db_session(
    security_config: Security | None = None,
) -> Session:  # type: ignore
    _url = url(security_config)
    if security_config and not security_config.sqlalchemy_pooling:
        engine = create_engine(_url, poolclass=NullPool)
    else:
        engine = create_engine(_url)
    with Session(engine) as session:
        try:
            yield session
        finally:
            session.close()


murfey_db_session = partial(
    get_murfey_db_session,
    get_security_config(),
)

murfey_db: Session = Depends(murfey_db_session)
