from datetime import datetime
from logging import getLogger
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends
from ispyb.sqlalchemy import Atlas
from pydantic import BaseModel
from sqlmodel import select

import murfey.server.prometheus as prom
from murfey.server import sanitise
from murfey.server.api import _transport_object
from murfey.server.api.auth import MurfeySessionID, validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, Session, SessionProcessingParameters

logger = getLogger("murfey.server.api.workflow")

router = APIRouter(
    prefix="/workflow", dependencies=[Depends(validate_token)], tags=["workflow"]
)


class DCGroupParameters(BaseModel):
    # DC = Data collection
    experiment_type: str
    experiment_type_id: int
    tag: str
    atlas: str = ""
    sample: Optional[int] = None
    atlas_pixel_size: int = 0


@router.post("/visits/{visit_name}/{session_id}/register_data_collection_group")
def register_dc_group(
    visit_name, session_id: MurfeySessionID, dcg_params: DCGroupParameters, db=murfey_db
):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    logger.info(f"Registering data collection group on microscope {instrument_name}")
    if dcg_murfey := db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == dcg_params.tag)
    ).all():
        dcg_murfey[0].atlas = dcg_params.atlas
        dcg_murfey[0].sample = dcg_params.sample
        dcg_murfey[0].atlas_pixel_size = dcg_params.atlas_pixel_size

        if _transport_object:
            if dcg_murfey[0].atlas_id is not None:
                _transport_object.send(
                    _transport_object.feedback_queue,
                    {
                        "register": "atlas_update",
                        "atlas_id": dcg_murfey[0].atlas_id,
                        "atlas": dcg_params.atlas,
                        "sample": dcg_params.sample,
                        "atlas_pixel_size": dcg_params.atlas_pixel_size,
                    },
                )
            else:
                atlas_id_response = _transport_object.do_insert_atlas(
                    Atlas(
                        dataCollectionGroupId=dcg_murfey[0].id,
                        atlasImage=dcg_params.atlas,
                        pixelSize=dcg_params.atlas_pixel_size,
                        cassetteSlot=dcg_params.sample,
                    )
                )
                dcg_murfey[0].atlas_id = atlas_id_response["return_value"]
        db.add(dcg_murfey[0])
        db.commit()
    else:
        dcg_parameters = {
            "start_time": str(datetime.now()),
            "experiment_type": dcg_params.experiment_type,
            "experiment_type_id": dcg_params.experiment_type_id,
            "tag": dcg_params.tag,
            "session_id": session_id,
            "atlas": dcg_params.atlas,
            "sample": dcg_params.sample,
            "atlas_pixel_size": dcg_params.atlas_pixel_size,
        }

        if _transport_object:
            _transport_object.send(
                _transport_object.feedback_queue,
                {
                    "register": "data_collection_group",
                    **dcg_parameters,
                    "microscope": instrument_name,
                    "proposal_code": ispyb_proposal_code,
                    "proposal_number": ispyb_proposal_number,
                    "visit_number": ispyb_visit_number,
                },
            )
    return dcg_params


class DCParameters(BaseModel):
    voltage: float
    pixel_size_on_image: str
    experiment_type: str
    image_size_x: int
    image_size_y: int
    file_extension: str
    acquisition_software: str
    image_directory: str
    tag: str
    source: str
    magnification: float
    total_exposed_dose: Optional[float] = None
    c2aperture: Optional[float] = None
    exposure_time: Optional[float] = None
    slit_width: Optional[float] = None
    phase_plate: bool = False
    data_collection_tag: str = ""


@router.post("/visits/{visit_name}/{session_id}/start_data_collection")
def start_dc(
    visit_name, session_id: MurfeySessionID, dc_params: DCParameters, db=murfey_db
):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    logger.info(
        f"Starting data collection on microscope {instrument_name!r} "
        f"with basepath {sanitise(str(machine_config.rsync_basepath))} and directory {sanitise(dc_params.image_directory)}"
    )
    dc_parameters = {
        "visit": visit_name,
        "image_directory": str(
            machine_config.rsync_basepath / dc_params.image_directory
        ),
        "start_time": str(datetime.now()),
        "voltage": dc_params.voltage,
        "pixel_size": str(float(dc_params.pixel_size_on_image) * 1e9),
        "image_suffix": dc_params.file_extension,
        "experiment_type": dc_params.experiment_type,
        "image_size_x": dc_params.image_size_x,
        "image_size_y": dc_params.image_size_y,
        "acquisition_software": dc_params.acquisition_software,
        "tag": dc_params.tag,
        "source": dc_params.source,
        "magnification": dc_params.magnification,
        "total_exposed_dose": dc_params.total_exposed_dose,
        "c2aperture": dc_params.c2aperture,
        "exposure_time": dc_params.exposure_time,
        "slit_width": dc_params.slit_width,
        "phase_plate": dc_params.phase_plate,
        "session_id": session_id,
    }

    if _transport_object:
        _transport_object.send(
            _transport_object.feedback_queue,
            {
                "register": "data_collection",
                **dc_parameters,
                "microscope": instrument_name,
                "proposal_code": ispyb_proposal_code,
                "proposal_number": ispyb_proposal_number,
                "visit_number": ispyb_visit_number,
            },
        )
    if dc_params.exposure_time:
        prom.exposure_time.set(dc_params.exposure_time)
    return dc_params


class ProcessingJobParameters(BaseModel):
    tag: str
    source: str
    recipe: str
    parameters: Dict[str, Any] = {}
    experiment_type: str = "spa"


@router.post("/visits/{visit_name}/{session_id}/register_processing_job")
def register_proc(
    visit_name: str,
    session_id: MurfeySessionID,
    proc_params: ProcessingJobParameters,
    db=murfey_db,
):
    proc_parameters: dict = {
        "session_id": session_id,
        "experiment_type": proc_params.experiment_type,
        "recipe": proc_params.recipe,
        "source": proc_params.source,
        "tag": proc_params.tag,
        "job_parameters": {
            k: v for k, v in proc_params.parameters.items() if v not in (None, "None")
        },
    }

    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()

    if session_processing_parameters:
        job_parameters: dict = proc_parameters["job_parameters"]
        job_parameters.update(
            {
                "gain_ref": session_processing_parameters[0].gain_ref,
                "dose_per_frame": session_processing_parameters[0].dose_per_frame,
                "eer_fractionation_file": session_processing_parameters[
                    0
                ].eer_fractionation_file,
                "symmetry": session_processing_parameters[0].symmetry,
            }
        )
        proc_parameters["job_parameters"] = job_parameters

    if _transport_object:
        _transport_object.send(
            _transport_object.feedback_queue,
            {"register": "processing_job", **proc_parameters},
        )
    return proc_params
