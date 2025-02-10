from __future__ import annotations

from functools import partial

import yaml
from cryptography.fernet import Fernet
from fastapi import Depends
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine

from murfey.util.config import Security, get_security_config


def url(security_config: Security | None = None) -> str:
    security_config = security_config or get_security_config()
    with open(security_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
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
