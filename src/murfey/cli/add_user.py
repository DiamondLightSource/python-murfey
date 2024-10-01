import argparse

from sqlmodel import Session, create_engine

from murfey.server.api.auth import hash_password
from murfey.server.murfey_db import url
from murfey.util.config import get_security_config
from murfey.util.db import MurfeyUser as User


def run():
    parser = argparse.ArgumentParser(
        description="Generate the necessary tables for the Murfey database"
    )

    parser.add_argument("-u", "--username", type=str, help="User name for new user")
    parser.add_argument("-p", "--password", type=str, help="Password for new user")

    args = parser.parse_args()

    new_user = User(
        username=args.username, hashed_password=hash_password(args.password)
    )
    _url = url(get_security_config())
    engine = create_engine(_url)
    with Session(engine) as murfey_db:
        murfey_db.add(new_user)
        murfey_db.commit()
        murfey_db.close()
