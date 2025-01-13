from __future__ import annotations

from functools import partial

import yaml
from cryptography.fernet import Fernet
from fastapi import Depends
from sqlalchemy.pool import NullPool
from sqlmodel import Session, create_engine

from murfey.util.config import GlobalConfig, get_global_config


def url(global_config: GlobalConfig | None = None) -> str:
    global_config = global_config or get_global_config()
    if global_config.murfey_db_credentials is None:
        raise ValueError(
            "No database credentials file was provided for this instance of Murfey"
        )
    with open(global_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
    f = Fernet(global_config.crypto_key.encode("ascii"))
    p = f.decrypt(creds["password"].encode("ascii"))
    return f"postgresql+psycopg2://{creds['username']}:{p.decode()}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_murfey_db_session(
    global_config: GlobalConfig | None = None,
) -> Session:  # type: ignore
    _url = url(global_config)
    if global_config and not global_config.sqlalchemy_pooling:
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
    get_global_config(),
)

murfey_db: Session = Depends(murfey_db_session)
