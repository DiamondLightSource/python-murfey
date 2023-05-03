from __future__ import annotations

import sqlalchemy
import yaml

from murfey.server.config import MachineConfig, get_machine_config


def url(machine_config: MachineConfig | None = None) -> str:
    machine_config = machine_config or get_machine_config()
    with open(machine_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)
    return f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"


def get_murfey_db_session(
    machine_config: MachineConfig | None = None,
) -> sqlalchemy.orm.Session:
    url = url(machine_config)
    Session = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(url, connect_args={"use_pure": True})
    )
    db = Session()
    try:
        yield db
    finally:
        db.close()
