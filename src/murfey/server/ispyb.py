from __future__ import annotations

import datetime
import logging
from typing import Callable, List

import ispyb

# import ispyb.sqlalchemy
import sqlalchemy.orm
import workflows.transport
from fastapi import Depends
from ispyb.sqlalchemy import (
    AutoProcProgram,
    BLSession,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
    Proposal,
    url,
)

from murfey.util.models import Visit

log = logging.getLogger("murfey.server.ispyb")

Session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(url(), connect_args={"use_pure": True})
)


class TransportManager:
    def __init__(self, transport_type):
        self.transport = workflows.transport.lookup(transport_type)()
        self.transport.connect()
        self.feedback_queue = ""
        self.ispyb = ispyb.open()
        self._connection_callback: Callable | None = None

    def do_insert_data_collection_group(
        self,
        record: DataCollectionGroup,
        message=None,
        **kwargs,
    ):
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created DataCollectionGroup {record.dataCollectionGroupId}")
                return {"success": True, "return_value": record.dataCollectionGroupId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Data Collection Group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return False

    def send(self, queue: str, message: dict):
        if self.transport:
            if not self.transport.is_connected():
                self.transport.connect()
                if self._connection_callback:
                    self._connection_callback()
            self.transport.send(queue, message)

    def do_insert_data_collection(self, record: DataCollection, message=None, **kwargs):
        comment = (
            f"Tilt series: {kwargs['tag']}"
            if kwargs.get("tag")
            else "Created for Murfey"
        )
        try:
            with Session() as db:
                record.comments = comment
                db.add(record)
                db.commit()
                log.info(f"Created DataCollection {record.dataCollectionId}")
                return {"success": True, "return_value": record.dataCollectionId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Data Collection entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}

    def do_create_ispyb_job(
        self,
        record: ProcessingJob,
        params: List[ProcessingJobParameter] | None = None,
        rw=None,
        **kwargs,
    ):
        params = params or []
        dcid = record.dataCollectionId
        if not dcid:
            log.error("Can not create job: DCID not specified")
            return False

        jp = self.ispyb.mx_processing.get_job_params()
        jp["automatic"] = record.automatic
        jp["comments"] = record.comments
        jp["datacollectionid"] = dcid
        jp["display_name"] = record.displayName
        jp["recipe"] = record.recipe
        log.info("Creating database entries...")
        try:
            jobid = self.ispyb.mx_processing.upsert_job(list(jp.values()))
            for p in params:
                pp = self.ispyb.mx_processing.get_job_parameter_params()
                pp["jobid"] = jobid
                pp["parameterkey"] = p.parameterKey
                pp["parametervalue"] = p.parametervalue
                self.ispyb.mx_processing.upsert_job_parameter(list(pp.values()))
            log.info(f"All done. Processing job {jobid} created")
            return {"success": True, "return_value": jobid}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Processing Job entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}

    def do_update_processing_status(self, record: AutoProcProgram, **kwargs):
        ppid = record.autoProcProgramId
        message = record.processingMessage
        status = record.processingStatus
        try:
            result = self.ispyb.mx_processing.upsert_program_ex(
                program_id=ppid,
                status={"success": 1, "failure": 0}.get(status),
                time_start=record.processingStartTime,
                time_update=record.processingEndTime,
                message=message,
                job_id=record.processingJobId,
            )
            log.info(
                f"Updating program {result} with status {status!r}",
            )
            return {"success": True, "return_value": result}
        except ispyb.ISPyBException as e:
            log.error(
                "Updating program %s status: '%s' caused exception '%s'.",
                ppid,
                message,
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}


def _get_session() -> sqlalchemy.orm.Session:
    db = Session()
    try:
        yield db
    finally:
        db.close()


DB = Depends(_get_session)
# Shortcut to access the database in a FastAPI endpoint


def get_session_id(
    microscope: str,
    proposal_code: str,
    proposal_number: str,
    visit_number: str,
    db: sqlalchemy.orm.Session,
) -> int:
    query = (
        db.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == microscope,
            Proposal.proposalCode == proposal_code,
            Proposal.proposalNumber == proposal_number,
            BLSession.visit_number == visit_number,
        )
        .add_columns(BLSession.sessionId)
        .all()
    )
    return query[0][1]


def get_all_ongoing_visits(microscope: str, db: sqlalchemy.orm.Session) -> list[Visit]:
    query = (
        db.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == microscope,
            BLSession.endDate > datetime.datetime.now(),
            BLSession.startDate < datetime.datetime.now(),
        )
        .add_columns(
            BLSession.startDate,
            BLSession.endDate,
            BLSession.sessionId,
            Proposal.proposalCode,
            Proposal.proposalNumber,
            BLSession.visit_number,
            Proposal.title,
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


def get_data_collection_group_ids(session_id):
    query = (
        Session()
        .query(DataCollectionGroup)
        .filter(
            DataCollectionGroup.sessionId == session_id,
        )
        .all()
    )
    dcgids = [row.dataCollectionGroupId for row in query]
    return dcgids
