import argparse
import os

import yaml
from sqlalchemy.schema import CreateTable
from sqlmodel import SQLModel, create_engine

from murfey.server.config import get_machine_config
from murfey.server.murfey_db import url


def run():
    parser = argparse.ArgumentParser(
        description="Write SQL required to create the Murfey database"
    )
    parser.add_argument(
        "--out-file",
        type=str,
        help="Output file for SQL writing",
        default="./create_murfey_db.sql",
    )
    args = parser.parse_args()

    engine = create_engine(url())

    machine_config = get_machine_config()
    with open(machine_config.murfey_db_credentials, "r") as stream:
        creds = yaml.safe_load(stream)

    with open(args.out_file, "w") as f:
        try:
            mic = os.environ["BEAMLINE"]
        except KeyError:
            mic = "m01"
        f.write(f"CREATE USER murfey WITH PASSWORD '{creds['password']}'; \n")
        f.write(f"CREATE DATABASE murfey_{mic}_db OWNER murfey; \n")
        for table in SQLModel.metadata.tables.values():
            f.write(f"{str(CreateTable(table).compile(engine))[:-2]};")
