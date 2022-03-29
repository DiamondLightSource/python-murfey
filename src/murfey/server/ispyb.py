from __future__ import annotations

import datetime
import logging

import ispyb.sqlalchemy
import sqlalchemy.orm
from fastapi import Depends

from murfey.util.models import Visit

_BLSession = ispyb.sqlalchemy.BLSession
_Proposal = ispyb.sqlalchemy.Proposal


log = logging.getLogger("murfey.server.ispyb")

Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)


def _get_session() -> sqlalchemy.orm.Session:
    db = Session()
    try:
        yield db
    finally:
        db.close()


DB = Depends(_get_session)
# Shortcut to access the database in a FastAPI endpoint


def get_all_ongoing_visits(microscope: str, db: sqlalchemy.orm.Session) -> list[Visit]:
    query = (
        db.query(_BLSession)
        .join(_Proposal)
        .filter(
            _BLSession.proposalId == _Proposal.proposalId,
            _BLSession.beamLineName == microscope,
            _BLSession.endDate > datetime.datetime.now(),
            _BLSession.startDate < datetime.datetime.now(),
        )
        .add_columns(
            _BLSession.startDate,
            _BLSession.endDate,
            _Proposal.proposalCode,
            _Proposal.proposalNumber,
            _BLSession.visit_number,
            _Proposal.title,
        )
        .all()
    )
    return [
        Visit(
            start=row.startDate,
            end=row.endDate,
            name=f"{row.proposalCode}{row.proposalNumber}-{row.visit_number}",
            proposal_title=row.title,
            beamline=microscope,
        )
        for row in query
    ]
