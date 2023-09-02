from typing import Optional, Tuple

from ispyb.sqlalchemy import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
)
from pydantic import BaseModel, validator
from sqlmodel import select

import murfey.util.db as db
from murfey.util.state import global_state


class DataCollectionGroupMessage(BaseModel):
    session_id: int
    client_id: int
    experiment_type: str
    experiment_type_id: int
    start_time: str
    tag: str


def register_data_collection_group(message: DataCollectionGroupMessage, engine):
    record = DataCollectionGroup(
        sessionId=message.session_id,
        experimentType=message.experiment_type,
        experimentTypeId=message.experiment_type_id,
    )
    dcgid = engine.transport_manager.do_insert_data_collection_group(record)[
        "return_value"
    ]
    if dcgid is None:
        raise ValueError()
    if global_state.get("data_collection_group_ids") and isinstance(
        global_state["data_collection_group_ids"], dict
    ):
        global_state["data_collection_group_ids"] = {
            **global_state["data_collection_group_ids"],
            message.tag: dcgid,
        }
    else:
        global_state["data_collection_group_ids"] = {message.tag: dcgid}
    client = engine.murfey_db_session.exec(
        select(db.ClientEnvironment).where(
            db.ClientEnvironment.client_id == message.client_id
        )
    ).one()
    murfey_dcg = db.DataCollectionGroup(
        id=dcgid,
        session_id=client.session_id,
        tag=message.tag,
    )
    engine.murfey_db_session.add(murfey_dcg)
    engine.murfey_db_session.commit()
    return None


class DataCollectionMessage(BaseModel):
    session_id: int
    experiment_type: str
    image_directory: str
    voltage: int
    pixel_size: float
    tag: str
    image_size: Tuple[int, int]
    slit_width: Optional[float] = None
    magnification: Optional[float] = None
    exposure_time: Optional[float] = None
    total_exposed_dose: Optional[float] = None
    c2aperture: Optional[float] = None
    phase_plate: int = 0


def register_data_collection(message: DataCollectionMessage, engine):
    dcgid = global_state.get("data_collection_group_ids", {}).get(  # type: ignore
        message["source"]
    )
    if dcgid is None:
        raise ValueError(
            f"No data collection group ID was found for image directory {message.image_directory}"
        )
    record = DataCollection(
        SESSIONID=message.session_id,
        experimenttype=message.experiment_type,
        imageDirectory=message.image_directory,
        imageSuffix=message.image_suffix,
        voltage=message.voltage,
        dataCollectionGroupId=dcgid,
        pixelSizeOnImage=message.pixel_size,
        imageSizeX=message.image_size_x,
        imageSizeY=message.image_size_y,
        slitGapHorizontal=message.slit_width,
        magnification=message.magnification,
        exposureTime=message.exposure_time,
        totalExposedDose=message.total_exposed_dose,
        c2aperture=message.c2aperture,
        phasePlate=message.phase_plate,
    )
    dcid = engine.transport_manager.do_insert_data_collection(record)["return_value"]
    murfey_dc = db.DataCollection(
        id=dcid,
        client=message.client_id,
        tag=message.tag,
        dcg_id=dcgid,
    )
    engine.murfey_db_session.add(murfey_dc)
    engine.murfey_db_session.commit()
    if dcid is None:
        raise ValueError
    if global_state.get("data_collection_ids") and isinstance(
        global_state["data_collection_ids"], dict
    ):
        global_state["data_collection_ids"] = {
            **global_state["data_collection_ids"],
            message.tag: dcid,
        }
    else:
        global_state["data_collection_ids"] = {message.tag: dcid}
    return None


class ProcessingJobMessage(BaseModel):
    client_id: int
    experiment_type: str
    tag: str
    recipe: str
    parameters: dict

    @validator("parameters")
    def check_parameters_content(cls, v):
        class ProcessingJobParametersMessage(BaseModel):
            angpix: float
            manual_tilt_offset: float

        ProcessingJobParametersMessage(v)
        return v


def register_processing_job(message: ProcessingJobMessage, engine):
    assert isinstance(global_state["data_collection_ids"], dict)
    _dcid = global_state["data_collection_ids"][message.tag]
    record = ProcessingJob(dataCollectionId=_dcid, recipe=message.recipe)
    if message.experiment_type != "spa":
        murfey_processing = db.TomographyProcessingParameters(
            client_id=message.client_id,
            pixel_size=message.parameters["angpix"],
            manual_tilt_offset=message.parameters["manual_tilt_offset"],
        )
        engine.murfey_db_session.add(murfey_processing)
        engine.murfey_db_session.commit()
    job_parameters = [
        ProcessingJobParameter(parameterKey=k, parameterValue=v)
        for k, v in message.parameters.items()
    ]
    pid = engine.transport_manager.do_create_ispyb_job(record, params=job_parameters)[
        "return_value"
    ]
    if pid is None:
        raise ValueError
    murfey_pj = db.ProcessingJob(id=pid, recipe=message.recipe, dc_id=_dcid)
    engine.murfey_db_session.add(murfey_pj)
    engine.murfey_db_session.commit()
    if global_state.get("processing_job_ids"):
        global_state["processing_job_ids"] = {
            **global_state["processing_job_ids"],  # type: ignore
            message.tag: {
                **global_state["processing_job_ids"].get(message.tag, {}),  # type: ignore
                message.recipe: pid,
            },
        }
    else:
        prids = {message.tag: {message.recipe: pid}}
        global_state["processing_job_ids"] = prids
    record = AutoProcProgram(processingJobId=pid)
    appid = engine.transport_manager.do_update_processing_status(record)["return_value"]
    murfey_app = db.AutoProcProgram(id=appid, pj_id=pid)
    engine.murfey_db_session.add(murfey_app)
    engine.murfey_db_session.commit()
    if global_state.get("autoproc_program_ids"):
        assert isinstance(global_state["autoproc_program_ids"], dict)
        global_state["autoproc_program_ids"] = {
            **global_state["autoproc_program_ids"],
            message.tag: {
                **global_state["processing_job_ids"].get(message.tag, {}),  # type: ignore
                message.recipe: appid,
            },
        }
    else:
        global_state["autoproc_program_ids"] = {message.tag: {message.recipe: appid}}
