from __future__ import annotations

import datetime
import logging

import ispyb.sqlalchemy
import sqlalchemy.orm
import workflows.transport
from fastapi import Depends
from sqlalchemy.orm import Load

from murfey.util.models import Visit

_BLSession = ispyb.sqlalchemy.BLSession
_Proposal = ispyb.sqlalchemy.Proposal
_DataCollection = ispyb.sqlalchemy.DataCollection
_ProcessingJob = ispyb.sqlalchemy.ProcessingJob

log = logging.getLogger("murfey.server.ispyb")

Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)


class TransportManager:
    def __init__(self, transport_type):
        self.transport = workflows.transport.lookup(transport_type)()
        self.transport.connect()

    def start_dc(self, message):
        message["ispyb_command"] = "insert_data_collection"
        visit = message["visit"]
        session = (
            Session()
            .query(_BLSession, _Proposal)
            .join(_Proposal, _Proposal.proposalId == _BLSession.proposalId)
            .options(
                Load(_BLSession).load_only("sessionId", "visit_number", "proposalId"),
                Load(_Proposal).load_only(
                    "proposalId", "proposalCode", "proposalNumber"
                ),
            )
            .filter(
                sqlalchemy.func.concat(
                    _Proposal.proposalCode,
                    _Proposal.proposalNumber,
                    "-",
                    _BLSession.visit_number,
                )
                == visit
            )
        )
        message["session_id"] = session.first()[0].sessionId
        ispyb_message = {"content": "Murfey DC insert", "parameters": message}
        self.transport.send("ispyb_connector", ispyb_message)


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


def start_data_collection(db: sqlalchemy.orm.Session):
    comment = "Test Murfey DC insert"
    insert = _DataCollection(comments=comment)
    # insert = _ProcessingJob(comments=comment)
    db.add(insert)
    db.commit()


# start_data_collection(Session())
