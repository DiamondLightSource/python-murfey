from __future__ import annotations

import datetime
import logging
from typing import Callable, List, Optional

import ispyb

# import ispyb.sqlalchemy
import sqlalchemy.orm
import workflows.transport
from fastapi import Depends
from ispyb.sqlalchemy import (
    AutoProcProgram,
    BLSample,
    BLSampleGroup,
    BLSampleGroupHasBLSample,
    BLSampleImage,
    BLSession,
    BLSubSample,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
    Proposal,
    ZcZocaloBuffer,
    url,
)

from murfey.util.models import Sample, Visit

log = logging.getLogger("murfey.server.ispyb")

try:
    Session = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(url(), connect_args={"use_pure": True})
    )
except AttributeError:
    Session = None


def _send_using_new_connection(transport_type: str, queue: str, message: dict) -> None:
    transport = workflows.transport.lookup(transport_type)()
    transport.connect()
    send_call = transport.send(queue, message)
    # send_call may be a concurrent.futures.Future object
    if send_call:
        send_call.result()
    transport.disconnect()
    return None


class TransportManager:
    def __init__(self, transport_type):
        self._transport_type = transport_type
        self.transport = workflows.transport.lookup(transport_type)()
        self.transport.connect()
        self.feedback_queue = ""
        self.ispyb = ispyb.open()
        self._connection_callback: Callable | None = None

    def reconnect(self):
        try:
            self.transport.disconnect()
        except Exception:
            log.warning(
                "Disconnection of old transport failed when reconnecting",
                exc_info=True,
            )
        self.transport = workflows.transport.lookup(self._transport_type)()
        self.transport.connect()

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
        return {"success": False, "return_value": None}

    def send(self, queue: str, message: dict, new_connection: bool = False):
        if self.transport:
            if not self.transport.is_connected():
                self.reconnect()
                if self._connection_callback:
                    self._connection_callback()
            if new_connection:
                _send_using_new_connection(self._transport_type, queue, message)
            else:
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

    def do_insert_sample_group(self, record: BLSampleGroup) -> dict:
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created BLSampleGroup {record.blSampleGroupId}")
                return {"success": True, "return_value": record.blSampleGroupId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Sample Group entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}

    def do_insert_sample(self, record: BLSample, sample_group_id: int) -> dict:
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created BLSample {record.blSampleId}")
                linking_record = BLSampleGroupHasBLSample(
                    blSampleId=record.blSampleId, blSampleGroupId=sample_group_id
                )
                db.add(linking_record)
                db.commit()
                log.info(
                    f"Linked BLSample {record.blSampleId} to BLSampleGroup {sample_group_id}"
                )
                return {"success": True, "return_value": record.blSampleId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Sample entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}

    def do_insert_subsample(self, record: BLSubSample) -> dict:
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created BLSubSample {record.blSubSampleId}")
                return {"success": True, "return_value": record.blSubSampleId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting SubSample entry caused exception '%s'.",
                e,
                exc_info=True,
            )
            return {"success": False, "return_value": None}

    def do_insert_sample_image(self, record: BLSampleImage) -> dict:
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created BLSampleImage {record.blSampleImageId}")
                return {"success": True, "return_value": record.blSampleImageId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Sample Image entry caused exception '%s'.",
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
                pp["job_id"] = jobid
                pp["parameter_key"] = p.parameterKey
                pp["parameter_value"] = p.parameterValue
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
        status = (
            "success"
            if record.processingStatus
            else ("none" if record.processingStatus is None else "failure")
        )
        try:
            result = self.ispyb.mx_processing.upsert_program_ex(
                program_id=ppid,
                status={"success": 1, "failure": 0, "none": None}.get(status),
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

    def do_buffer_lookup(self, app_id: int, uuid: int) -> Optional[int]:
        with Session() as db:
            buffer_objects = (
                db.query(ZcZocaloBuffer)
                .filter_by(AutoProcProgramID=app_id, UUID=uuid)
                .all()
            )
            reference = buffer_objects[0].Reference if buffer_objects else None
        log.info(f"Buffer lookup {uuid} returned {reference}")
        return reference


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
    res = query[0][1]
    db.close()
    return res


def get_proposal_id(
    proposal_code: str, proposal_number: str, db: sqlalchemy.orm.Session
) -> int:
    query = (
        db.query(Proposal)
        .filter(
            Proposal.proposalCode == proposal_code,
            Proposal.proposalNumber == proposal_number,
        )
        .all()
    )
    return query[0].proposalId


def get_sub_samples_from_visit(visit: str, db: sqlalchemy.orm.Session) -> List[Sample]:
    proposal_id = get_proposal_id(visit[:2], visit.split("-")[0][2:], db)
    samples = (
        db.query(BLSampleGroup, BLSampleGroupHasBLSample, BLSample, BLSubSample)
        .join(BLSample, BLSample.blSampleId == BLSampleGroupHasBLSample.blSampleId)
        .join(
            BLSampleGroup,
            BLSampleGroup.blSampleGroupId == BLSampleGroupHasBLSample.blSampleGroupId,
        )
        .join(BLSubSample, BLSubSample.blSampleId == BLSample.blSampleId)
        .filter(BLSampleGroup.proposalId == proposal_id)
        .all()
    )
    res = [
        Sample(
            sample_group_id=s[1].blSampleGroupId,
            sample_id=s[2].blSampleId,
            subsample_id=s[3].blSubSampleId,
            image_path=s[3].imgFilePath,
        )
        for s in samples
    ]
    return res


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
