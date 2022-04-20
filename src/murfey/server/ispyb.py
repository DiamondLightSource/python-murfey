from __future__ import annotations

import datetime
import logging

import ispyb.sqlalchemy
import sqlalchemy.orm
import workflows.transport
from fastapi import Depends

import murfey.server
from murfey.util.models import Visit

_BLSession = ispyb.sqlalchemy.BLSession
_Proposal = ispyb.sqlalchemy.Proposal
_DataCollection = ispyb.sqlalchemy.DataCollection
_ProcessingJob = ispyb.sqlalchemy.ProcessingJob
_DataCollectionGroup = ispyb.sqlalchemy.DataCollectionGroup

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
        message["start_time"] = str(datetime.datetime.now())
        visits = get_all_ongoing_visits(murfey.server.get_microscope(), Session())
        current_visit_object = [
            visit for visit in visits if visit.name == message["visit"]
        ]
        if current_visit_object:
            message["session_id"] = current_visit_object[0].session_id
        self.transport.send(
            "processing_recipe", {"recipes": ["ispyb-murfey"], "parameters": message}
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
            _BLSession.sessionId,
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
            session_id=row.sessionId,
            name=f"{row.proposalCode}{row.proposalNumber}-{row.visit_number}",
            proposal_title=row.title,
            beamline=microscope,
        )
        for row in query
    ]


def start_data_collection(db: sqlalchemy.orm.Session):
    comment = "Test Murfey DC insert"
    insert = _DataCollection(comments=comment)
    db.add(insert)
    db.commit()


def get_data_collection_group_ids(session_id):
    query = (
        Session()
        .query(_DataCollectionGroup)
        .filter(
            _DataCollectionGroup.sessionId == session_id,
        )
        .all()
    )
    dcgids = [row.dataCollectionGroupId for row in query]
    return dcgids
