from __future__ import annotations

import datetime
import logging
from typing import Callable, List, Literal, Optional

import ispyb

# import ispyb.sqlalchemy
import sqlalchemy.orm
import workflows.transport
from fastapi import Depends
from ispyb.sqlalchemy import (
    Atlas,
    AutoProcProgram,
    BLSample,
    BLSampleGroup,
    BLSampleGroupHasBLSample,
    BLSampleImage,
    BLSession,
    BLSubSample,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    ProcessingJob,
    ProcessingJobParameter,
    Proposal,
    ZcZocaloBuffer,
    url,
)

from murfey.util.config import get_security_config
from murfey.util.models import FoilHoleParameters, GridSquareParameters, Sample, Visit

log = logging.getLogger("murfey.server.ispyb")
security_config = get_security_config()

try:
    Session = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(url(), connect_args={"use_pure": True})
    )
except AttributeError:
    Session = lambda: None


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
    def __init__(self, transport_type: Literal["PikaTransport"]):
        self._transport_type = transport_type
        self.transport = workflows.transport.lookup(transport_type)()
        self.transport.connect()
        self.feedback_queue = ""
        self.ispyb = (
            ispyb.open(credentials=security_config.ispyb_credentials)
            if security_config.ispyb_credentials
            else None
        )
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

    def do_insert_atlas(self, record: Atlas):
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created Atlas {record.atlasId}")
                return {"success": True, "return_value": record.atlasId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting Atlas entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return {"success": False, "return_value": None}

    def do_update_atlas(
        self, atlas_id: int, atlas_image: str, pixel_size: float, slot: int
    ):
        try:
            with Session() as db:
                atlas = db.query(Atlas).filter(Atlas.atlasId == atlas_id).one()
                atlas.atlasImage = atlas_image or atlas.atlasImage
                atlas.pixelSize = pixel_size or atlas.pixelSize
                atlas.cassetteSlot = slot or atlas.cassetteSlot
                db.add(atlas)
                db.commit()
                return {"success": True, "return_value": atlas.atlasId}
        except ispyb.ISPyBException as e:
            log.error(
                "Updating Atlas entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return {"success": False, "return_value": None}

    def do_insert_grid_square(
        self,
        atlas_id: int,
        grid_square_id: int,
        grid_square_parameters: GridSquareParameters,
    ):
        # most of this is for mypy
        if (
            grid_square_parameters.pixel_size is not None
            and grid_square_parameters.thumbnail_size_x is not None
            and grid_square_parameters.readout_area_x is not None
        ):
            # currently hard coding the scale factor because of difficulties with
            # guaranteeing we have the atlas jpg and mrc sizes
            grid_square_parameters.pixel_size *= (
                grid_square_parameters.readout_area_x
                / grid_square_parameters.thumbnail_size_x
            )
        grid_square_parameters.height = (
            int(grid_square_parameters.height / 7.8)
            if grid_square_parameters.height
            else None
        )
        grid_square_parameters.width = (
            int(grid_square_parameters.width / 7.8)
            if grid_square_parameters.width
            else None
        )
        grid_square_parameters.x_location = (
            int(grid_square_parameters.x_location / 7.8)
            if grid_square_parameters.x_location
            else None
        )
        grid_square_parameters.y_location = (
            int(grid_square_parameters.y_location / 7.8)
            if grid_square_parameters.y_location
            else None
        )
        record = GridSquare(
            atlasId=atlas_id,
            gridSquareLabel=grid_square_id,
            gridSquareImage=grid_square_parameters.image,
            pixelLocationX=grid_square_parameters.x_location,
            pixelLocationY=grid_square_parameters.y_location,
            height=grid_square_parameters.height,
            width=grid_square_parameters.width,
            angle=grid_square_parameters.angle,
            stageLocationX=grid_square_parameters.x_stage_position,
            stageLocationY=grid_square_parameters.y_stage_position,
            pixelSize=grid_square_parameters.pixel_size,
        )
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created GridSquare {record.gridSquareId}")
                return {"success": True, "return_value": record.gridSquareId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting GridSquare entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return {"success": False, "return_value": None}

    def do_update_grid_square(
        self, grid_square_id: int, grid_square_parameters: GridSquareParameters
    ):
        try:
            with Session() as db:
                grid_square = (
                    db.query(GridSquare)
                    .filter(GridSquare.gridSquareId == grid_square_id)
                    .one()
                )
                if (
                    grid_square_parameters.pixel_size is not None
                    and grid_square_parameters.readout_area_x is not None
                    and grid_square_parameters.thumbnail_size_x is not None
                ):
                    grid_square_parameters.pixel_size *= (
                        grid_square_parameters.readout_area_x
                        / grid_square_parameters.thumbnail_size_x
                    )
                if grid_square_parameters.image:
                    grid_square.gridSquareImage = grid_square_parameters.image
                if grid_square_parameters.x_location:
                    grid_square.pixelLocationX = int(
                        grid_square_parameters.x_location / 7.8
                    )
                if grid_square_parameters.y_location:
                    grid_square.pixelLocationY = int(
                        grid_square_parameters.y_location / 7.8
                    )
                if grid_square_parameters.height is not None:
                    grid_square.height = int(grid_square_parameters.height / 7.8)
                if grid_square_parameters.width is not None:
                    grid_square.width = int(grid_square_parameters.width / 7.8)
                if grid_square_parameters.angle:
                    grid_square.angle = grid_square_parameters.angle
                if grid_square_parameters.x_stage_position:
                    grid_square.stageLocationX = grid_square_parameters.x_stage_position
                if grid_square_parameters.y_stage_position:
                    grid_square.stageLocationY = grid_square_parameters.y_stage_position
                if grid_square_parameters.pixel_size:
                    grid_square.pixelSize = grid_square_parameters.pixel_size
                db.add(grid_square)
                db.commit()
                return {"success": True, "return_value": grid_square.gridSquareId}
        except ispyb.ISPyBException as e:
            log.error(
                "Updating GridSquare entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return {"success": False, "return_value": None}

    def do_insert_foil_hole(
        self,
        grid_square_id: int,
        scale_factor: Optional[float],
        foil_hole_parameters: FoilHoleParameters,
    ):
        if (
            foil_hole_parameters.thumbnail_size_x is not None
            and foil_hole_parameters.readout_area_x is not None
            and foil_hole_parameters.pixel_size is not None
        ):
            foil_hole_parameters.pixel_size *= (
                foil_hole_parameters.readout_area_x
                / foil_hole_parameters.thumbnail_size_x
            )
        if scale_factor:
            foil_hole_parameters.diameter = (
                int(foil_hole_parameters.diameter * scale_factor)
                if foil_hole_parameters.diameter
                else None
            )
            foil_hole_parameters.x_location = (
                int(foil_hole_parameters.x_location * scale_factor)
                if foil_hole_parameters.x_location
                else None
            )
            foil_hole_parameters.y_location = (
                int(foil_hole_parameters.y_location * scale_factor)
                if foil_hole_parameters.y_location
                else None
            )
        record = FoilHole(
            gridSquareId=grid_square_id,
            foilHoleLabel=foil_hole_parameters.name,
            foilHoleImage=foil_hole_parameters.image,
            pixelLocationX=foil_hole_parameters.x_location,
            pixelLocationY=foil_hole_parameters.y_location,
            diameter=foil_hole_parameters.diameter,
            stageLocationX=foil_hole_parameters.x_stage_position,
            stageLocationY=foil_hole_parameters.y_stage_position,
            pixelSize=foil_hole_parameters.pixel_size,
        )
        try:
            with Session() as db:
                db.add(record)
                db.commit()
                log.info(f"Created FoilHole {record.foilHoleId}")
                return {"success": True, "return_value": record.foilHoleId}
        except ispyb.ISPyBException as e:
            log.error(
                "Inserting FoilHole entry caused exception '%s'.",
                e,
                exc_info=True,
            )
        return {"success": False, "return_value": None}

    def do_update_foil_hole(
        self,
        foil_hole_id: int,
        scale_factor: float,
        foil_hole_parameters: FoilHoleParameters,
    ):
        try:
            with Session() as db:
                foil_hole = (
                    db.query(FoilHole).filter(FoilHole.foilHoleId == foil_hole_id).one()
                )
                if foil_hole_parameters.image:
                    foil_hole.foilHoleImage = foil_hole_parameters.image
                if foil_hole_parameters.x_location:
                    foil_hole.pixelLocationX = int(
                        foil_hole_parameters.x_location * scale_factor
                    )
                if foil_hole_parameters.y_location:
                    foil_hole.pixelLocationY = int(
                        foil_hole_parameters.y_location * scale_factor
                    )
                if foil_hole_parameters.diameter is not None:
                    foil_hole.diameter = foil_hole_parameters.diameter * scale_factor
                if foil_hole_parameters.x_stage_position:
                    foil_hole.stageLocationX = foil_hole_parameters.x_stage_position
                if foil_hole_parameters.y_stage_position:
                    foil_hole.stageLocationY = foil_hole_parameters.y_stage_position
                if (
                    foil_hole_parameters.readout_area_x is not None
                    and foil_hole_parameters.thumbnail_size_x is not None
                    and foil_hole_parameters.pixel_size is not None
                ):
                    foil_hole.pixelSize = foil_hole_parameters.pixel_size * (
                        foil_hole_parameters.readout_area_x
                        / foil_hole_parameters.thumbnail_size_x
                    )
                db.add(foil_hole)
                db.commit()
                return {"success": True, "return_value": foil_hole.foilHoleId}
        except ispyb.ISPyBException as e:
            log.error(
                "Updating FoilHole entry caused exception '%s'.",
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
    if db is None:
        yield None
        return
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
    db: sqlalchemy.orm.Session | None,
) -> int | None:
    if db is None:
        return None
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


def get_all_ongoing_visits(
    microscope: str, db: sqlalchemy.orm.Session | None
) -> list[Visit]:
    if db is None:
        return []
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
