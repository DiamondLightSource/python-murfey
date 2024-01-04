from __future__ import annotations

from functools import partial

import yaml
from cryptography.fernet import Fernet
from fastapi import Depends
from sqlmodel import Session, create_engine

from murfey.server.config import MachineConfig, get_machine_config


def url(machine_config: MachineConfig | None = None) -> str:
    machine_config = machine_config or get_machine_config()
    with open(machine_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
    f = Fernet(machine_config.crypto_key.encode("ascii"))
    p = f.decrypt(creds["password"].encode("ascii"))
    return f"postgresql+psycopg2://{creds['username']}:{p.decode()}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_murfey_db_session(
    machine_config: MachineConfig | None = None,
) -> Session:
    _url = url(machine_config)
    engine = create_engine(_url)
    with Session(engine) as session:
        try:
            yield session
        finally:
            session.close()


murfey_db = Depends(partial(get_murfey_db_session, get_machine_config()))
