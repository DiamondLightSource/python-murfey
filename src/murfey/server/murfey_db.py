from __future__ import annotations

from functools import partial

import yaml
from fastapi import Depends
from sqlmodel import Session, create_engine

from murfey.server.config import MachineConfig, get_machine_config


def url(machine_config: MachineConfig | None = None) -> str:
    machine_config = machine_config or get_machine_config()
    with open(machine_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
    return f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"


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
