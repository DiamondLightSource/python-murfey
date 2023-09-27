import argparse

from sqlmodel import Session, create_engine, select

from murfey.server.config import get_machine_config
from murfey.server.murfey_db import url
from murfey.util.db import Session as MurfeySession


def run():
    parser = argparse.ArgumentParser(
        description="See and remove Murfey client sessions from Murfey DB"
    )

    parser.add_argument("-d", "--delete", dest="sessions_to_remove", nargs="+")

    args = parser.parse_args()

    _url = url(get_machine_config())
    engine = create_engine(_url)
    murfey_db = Session(engine)
    if args.sessions_to_remove:
        for sess_id in args.sessions_to_remove:
            sess = murfey_db.exec(
                select(MurfeySession).where(MurfeySession.id == sess_id)
            ).one()
            murfey_db.delete(sess)
        murfey_db.commit()
    else:
        sessions = murfey_db.exec(select(MurfeySession)).all()
        print(sessions)
    murfey_db.close()
