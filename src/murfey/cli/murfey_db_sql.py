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
        ordered_tables = []
        ordered_table_names = []
        waiting = {}
        for tn, t in SQLModel.metadata.tables.items():
            for c in t.columns:
                if c.foreign_keys:
                    constraints = [fk._column_tokens[1] for fk in c.foreign_keys]
                    if not all(co in ordered_table_names for co in constraints):
                        if waiting.get(constraints[0]):
                            waiting[constraints[0]].append(t)
                        else:
                            waiting[constraints[0]] = [t]
                        break
            else:
                ordered_tables.append(t)
                ordered_table_names.append(tn)
                for wt in waiting.get(tn, []):
                    ordered_tables.append(wt)
                    ordered_table_names.append(wt.name)
                waiting[tn] = []
        for table in ordered_tables:
            f.write(f"{str(CreateTable(table).compile(engine))[:-2]};")
