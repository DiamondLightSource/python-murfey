import asyncio
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Any, Dict, List, Optional

import sqlalchemy
from fastapi import APIRouter, Depends
from ispyb.sqlalchemy import (
    Atlas,
    BLSample,
    BLSampleGroup,
    BLSampleGroupHasBLSample,
    BLSampleImage,
    BLSubSample,
)
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError
from sqlmodel import col, select
from werkzeug.utils import secure_filename

try:
    from PIL import Image
except ImportError:
    Image = None

import murfey.server.prometheus as prom
from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.server.api.auth import validate_instrument_token
from murfey.server.feedback import (
    _murfey_id,
    check_tilt_series_mc,
    get_all_tilts,
    get_angle,
    get_job_ids,
    get_tomo_proc_params,
)
from murfey.server.ispyb import DB as ispyb_db
from murfey.server.ispyb import get_proposal_id
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise
from murfey.util.config import get_machine_config
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    PreprocessStash,
    ProcessingJob,
    SearchMap,
    Session,
    SessionProcessingParameters,
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)
from murfey.util.models import (
    ProcessingParametersSPA,
    ProcessingParametersTomo,
    SearchMapParameters,
)
from murfey.util.processing_params import (
    cryolo_model_path,
    default_spa_parameters,
    motion_corrected_mrc,
)
from murfey.util.tomo import midpoint
from murfey.workflows.tomo.tomo_metadata import register_search_map_in_database

logger = getLogger("murfey.server.api.workflow")

router = APIRouter(
    prefix="/workflow",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: General"],
)


class DCGroupParameters(BaseModel):
    # DC = Data collection
    experiment_type: str
    experiment_type_id: int
    tag: str
    atlas: str = ""
    sample: Optional[int] = None
    atlas_pixel_size: float = 0


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
        dcg_murfey[0].atlas = dcg_params.atlas or dcg_murfey[0].atlas
        dcg_murfey[0].sample = dcg_params.sample or dcg_murfey[0].sample
        dcg_murfey[0].atlas_pixel_size = (
            dcg_params.atlas_pixel_size or dcg_murfey[0].atlas_pixel_size
        )

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
                        "dcgid": dcg_murfey[0].id,
                        "session_id": session_id,
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

        search_maps = db.exec(
            select(SearchMap)
            .where(SearchMap.session_id == session_id)
            .where(SearchMap.tag == dcg_params.tag)
        ).all()
        search_map_params = SearchMapParameters(tag=dcg_params.tag)
        for sm in search_maps:
            register_search_map_in_database(
                session_id, sm.name, search_map_params, db, close_db=False
            )
        db.close()
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


spa_router = APIRouter(
    prefix="/workflow/spa",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: SPA"],
)


@spa_router.post("/sessions/{session_id}/spa_processing_parameters")
def register_spa_proc_params(
    session_id: MurfeySessionID, proc_params: ProcessingParametersSPA, db=murfey_db
):
    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_processing_parameters:
        proc_params.gain_ref = session_processing_parameters[0].gain_ref
        proc_params.dose_per_frame = session_processing_parameters[0].dose_per_frame
        proc_params.eer_fractionation_file = session_processing_parameters[
            0
        ].eer_fractionation_file
        proc_params.symmetry = session_processing_parameters[0].symmetry

    zocalo_message = {
        "register": "spa_processing_parameters",
        **dict(proc_params),
        "session_id": session_id,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


class Tag(BaseModel):
    tag: str


@spa_router.post("/visits/{visit_name}/{session_id}/flush_spa_processing")
def flush_spa_processing(
    visit_name: str, session_id: MurfeySessionID, tag: Tag, db=murfey_db
):
    zocalo_message = {
        "register": "spa.flush_spa_preprocess",
        "session_id": session_id,
        "tag": tag.tag,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)
    return


class SPAProcessFile(BaseModel):
    tag: str
    path: str
    description: str
    processing_job: Optional[int] = None
    data_collection_id: Optional[int] = None
    image_number: int
    autoproc_program_id: Optional[int] = None
    foil_hole_id: Optional[int] = None
    pixel_size: Optional[float] = None
    dose_per_frame: Optional[float] = None
    mc_binning: Optional[int] = 1
    gain_ref: Optional[str] = None
    extract_downscale: bool = True
    eer_fractionation_file: Optional[str] = None
    source: str = ""


@spa_router.post("/visits/{visit_name}/{session_id}/spa_preprocess")
async def request_spa_preprocessing(
    visit_name: str,
    session_id: MurfeySessionID,
    proc_file: SPAProcessFile,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    mrc_out = motion_corrected_mrc(Path(proc_file.path), visit_name, machine_config)
    try:
        collected_ids = db.exec(
            select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
            .where(DataCollectionGroup.session_id == session_id)
            .where(DataCollectionGroup.tag == proc_file.tag)
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()
        params = db.exec(
            select(SPARelionParameters, SPAFeedbackParameters)
            .where(SPARelionParameters.pj_id == collected_ids[2].id)
            .where(SPAFeedbackParameters.pj_id == SPARelionParameters.pj_id)
        ).one()
        proc_params: Optional[dict] = dict(params[0])
        feedback_params = params[1]
    except sqlalchemy.exc.NoResultFound:
        proc_params = None
    try:
        foil_hole_id = (
            db.exec(
                select(FoilHole, GridSquare)
                .where(FoilHole.name == proc_file.foil_hole_id)
                .where(FoilHole.session_id == session_id)
                .where(GridSquare.id == FoilHole.grid_square_id)
                .where(GridSquare.tag == proc_file.tag)
            )
            .one()[0]
            .id
        )
    except Exception as e:
        logger.warning(
            f"Foil hole ID not found for foil hole {sanitise(str(proc_file.foil_hole_id))}: {e}",
            exc_info=True,
        )
        foil_hole_id = None
    if proc_params:

        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], db, number=2, close=False)

        if feedback_params.picker_murfey_id is None:
            feedback_params.picker_murfey_id = murfey_ids[1]
            db.add(feedback_params)
        movie = Movie(
            murfey_id=murfey_ids[0],
            path=proc_file.path,
            image_number=proc_file.image_number,
            tag=proc_file.tag,
            foil_hole_id=foil_hole_id,
        )
        db.add(movie)
        db.commit()
        db.close()

        if not mrc_out.parent.exists():
            Path(secure_filename(str(mrc_out))).parent.mkdir(
                parents=True, exist_ok=True
            )
        recipe_name = machine_config.recipes.get(
            "em-spa-preprocess", "em-spa-preprocess"
        )
        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": detached_ids[1],
                "kv": proc_params["voltage"],
                "autoproc_program_id": detached_ids[3],
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pixel_size": proc_params["angpix"],
                "image_number": proc_file.image_number,
                "microscope": instrument_name,
                "mc_uuid": murfey_ids[0],
                "foil_hole_id": foil_hole_id,
                "ft_bin": proc_params["motion_corr_binning"],
                "fm_dose": proc_params["dose_per_frame"],
                "gain_ref": proc_params["gain_ref"],
                "picker_uuid": murfey_ids[1],
                "session_id": session_id,
                "particle_diameter": proc_params["particle_diameter"] or 0,
                "fm_int_file": (
                    proc_params["eer_fractionation_file"]
                    if proc_params["eer_fractionation_file"]
                    else proc_file.eer_fractionation_file
                ),
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "cryolo_model_weights": str(
                    cryolo_model_path(visit_name, instrument_name)
                ),
            },
        }
        # log.info(f"Sending Zocalo message {zocalo_message}")
        if _transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = _transport_object.feedback_queue
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            logger.error(
                f"Pre-processing was requested for {sanitise(Path(proc_file.path).name)} "
                "but no Zocalo transport object was found"
            )
            return proc_file

    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            tag=proc_file.tag,
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            eer_fractionation_file=str(proc_file.eer_fractionation_file),
            foil_hole_id=foil_hole_id,
        )
        db.add(for_stash)
        db.commit()
        db.close()

    return proc_file


tomo_router = APIRouter(
    prefix="/workflow/tomo",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: CryoET"],
)


@tomo_router.post("/sessions/{session_id}/tomography_processing_parameters")
def register_tomo_proc_params(
    session_id: MurfeySessionID, proc_params: ProcessingParametersTomo, db=murfey_db
):
    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_processing_parameters:
        proc_params.gain_ref = session_processing_parameters[0].gain_ref
        proc_params.dose_per_frame = session_processing_parameters[0].dose_per_frame
        proc_params.eer_fractionation_file = session_processing_parameters[
            0
        ].eer_fractionation_file

    zocalo_message = {
        "register": "tomography_processing_parameters",
        **dict(proc_params),
        "session_id": session_id,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


class Source(BaseModel):
    rsync_source: str


@tomo_router.post("/visits/{visit_name}/{session_id}/flush_tomography_processing")
def flush_tomography_processing(
    visit_name: str, session_id: MurfeySessionID, rsync_source: Source, db=murfey_db
):
    zocalo_message = {
        "register": "flush_tomography_preprocess",
        "session_id": session_id,
        "visit_name": visit_name,
        "data_collection_group_tag": rsync_source.rsync_source,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)
    return


class TiltSeriesInfo(BaseModel):
    session_id: int
    tag: str
    source: str


@tomo_router.post("/visits/{visit_name}/tilt_series")
def register_tilt_series(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    session_id = tilt_series_info.session_id
    if db.exec(
        select(TiltSeries)
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.tag == tilt_series_info.tag)
        .where(TiltSeries.rsync_source == tilt_series_info.source)
    ).all():
        return
    tilt_series = TiltSeries(
        session_id=session_id,
        tag=tilt_series_info.tag,
        rsync_source=tilt_series_info.source,
    )
    db.add(tilt_series)
    db.commit()


class TiltSeriesGroupInfo(BaseModel):
    tags: List[str]
    source: str
    tilt_series_lengths: List[int]


@tomo_router.post("/sessions/{session_id}/tilt_series_length")
def register_tilt_series_length(
    session_id: int,
    tilt_series_group: TiltSeriesGroupInfo,
    db=murfey_db,
):
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(col(TiltSeries.tag).in_(tilt_series_group.tags))
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.rsync_source == tilt_series_group.source)
    ).all()
    for ts in tilt_series_db:
        ts_index = tilt_series_group.tags.index(ts.tag)
        ts.tilt_series_length = tilt_series_group.tilt_series_lengths[ts_index]
        db.add(ts)
    db.commit()


class TomoProcessFile(BaseModel):
    path: str
    description: str
    tag: str
    image_number: int
    pixel_size: float
    dose_per_frame: Optional[float] = None
    frame_count: int
    tilt_axis: Optional[float] = None
    mc_uuid: Optional[int] = None
    voltage: float = 300
    mc_binning: int = 1
    gain_ref: Optional[str] = None
    extract_downscale: int = 1
    eer_fractionation_file: Optional[str] = None
    group_tag: Optional[str] = None


@tomo_router.post("/visits/{visit_name}/{session_id}/tomography_preprocess")
async def request_tomography_preprocessing(
    visit_name: str,
    session_id: MurfeySessionID,
    proc_file: TomoProcessFile,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    mrc_out = motion_corrected_mrc(Path(proc_file.path), visit_name, machine_config)

    recipe_name = machine_config.recipes.get("em-tomo-preprocess", "em-tomo-preprocess")

    data_collection = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == proc_file.group_tag)
        .where(DataCollection.tag == proc_file.tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == recipe_name)
    ).all()
    if data_collection:
        if registered_tilts := db.exec(
            select(Tilt).where(Tilt.movie_path == proc_file.path)
        ).all():
            if len(registered_tilts) == 1:
                if registered_tilts[0].motion_corrected:
                    return proc_file
        dcid = data_collection[0][1].id
        appid = data_collection[0][3].id
        murfey_ids = _murfey_id(appid, db, number=1, close=False)
        if not mrc_out.parent.exists():
            mrc_out.parent.mkdir(parents=True, exist_ok=True)

        session_processing_parameters = db.exec(
            select(SessionProcessingParameters).where(
                SessionProcessingParameters.session_id == session_id
            )
        ).all()
        if session_processing_parameters:
            proc_file.gain_ref = session_processing_parameters[0].gain_ref
            proc_file.dose_per_frame = session_processing_parameters[0].dose_per_frame
            proc_file.eer_fractionation_file = session_processing_parameters[
                0
            ].eer_fractionation_file

        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": dcid,
                # "timestamp": datetime.datetime.now(),
                "autoproc_program_id": appid,
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pixel_size": (proc_file.pixel_size) * 10**10,
                "image_number": proc_file.image_number,
                "kv": int(proc_file.voltage),
                "microscope": instrument_name,
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_file.mc_binning,
                "fm_dose": proc_file.dose_per_frame,
                "frame_count": proc_file.frame_count,
                "gain_ref": (
                    str(machine_config.rsync_basepath / proc_file.gain_ref)
                    if proc_file.gain_ref and machine_config.data_transfer_enabled
                    else proc_file.gain_ref
                ),
                "fm_int_file": proc_file.eer_fractionation_file,
            },
        }
        if _transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = _transport_object.feedback_queue
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            logger.error(
                f"Pre-processing was requested for {sanitise(Path(proc_file.path).name)} "
                f"but no Zocalo transport object was found"
            )
            return proc_file
    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            tag=proc_file.tag,
            group_tag=proc_file.group_tag,
        )
        db.add(for_stash)
        db.commit()
        db.close()
    return proc_file


@tomo_router.post("/visits/{visit_name}/{session_id}/completed_tilt_series")
def register_completed_tilt_series(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_group: TiltSeriesGroupInfo,
    db=murfey_db,
):
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(col(TiltSeries.tag).in_(tilt_series_group.tags))
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.rsync_source == tilt_series_group.source)
    ).all()
    for ts in tilt_series_db:
        ts_index = tilt_series_group.tags.index(ts.tag)
        ts.tilt_series_length = tilt_series_group.tilt_series_lengths[ts_index]
        db.add(ts)
    db.commit()
    for ts in tilt_series_db:
        if (
            check_tilt_series_mc(ts.id)
            and not ts.processing_requested
            and ts.tilt_series_length > 2
        ):
            ts.processing_requested = True
            db.add(ts)

            collected_ids = db.exec(
                select(
                    DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram
                )
                .where(DataCollectionGroup.session_id == session_id)
                .where(DataCollectionGroup.tag == tilt_series_group.source)
                .where(DataCollection.tag == ts.tag)
                .where(DataCollection.dcg_id == DataCollectionGroup.id)
                .where(ProcessingJob.dc_id == DataCollection.id)
                .where(AutoProcProgram.pj_id == ProcessingJob.id)
                .where(ProcessingJob.recipe == "em-tomo-align")
            ).one()
            instrument_name = (
                db.exec(select(Session).where(Session.id == session_id))
                .one()
                .instrument_name
            )
            machine_config = get_machine_config(instrument_name=instrument_name)[
                instrument_name
            ]
            tilts = get_all_tilts(ts.id)
            ids = get_job_ids(ts.id, collected_ids[3].id)
            preproc_params = get_tomo_proc_params(ids.dcgid)

            first_tilt = db.exec(
                select(Tilt).where(Tilt.tilt_series_id == ts.id)
            ).first()
            parts = [secure_filename(p) for p in Path(first_tilt.movie_path).parts]
            visit_idx = parts.index(visit_name)
            core = Path(*Path(first_tilt.movie_path).parts[: visit_idx + 1])
            ppath = Path(
                "/".join(secure_filename(p) for p in Path(first_tilt.movie_path).parts)
            )
            sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
            extra_path = machine_config.processed_extra_directory
            stack_file = (
                core
                / machine_config.processed_directory_name
                / sub_dataset
                / extra_path
                / "Tomograms"
                / "job006"
                / "tomograms"
                / f"{ts.tag}_stack.mrc"
            )
            if not stack_file.parent.exists():
                stack_file.parent.mkdir(parents=True)
            tilt_offset = midpoint([float(get_angle(t)) for t in tilts])
            zocalo_message = {
                "recipes": ["em-tomo-align"],
                "parameters": {
                    "input_file_list": str([[t, str(get_angle(t))] for t in tilts]),
                    "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
                    "dcid": ids.dcid,
                    "appid": ids.appid,
                    "stack_file": str(stack_file),
                    "dose_per_frame": preproc_params.dose_per_frame,
                    "frame_count": preproc_params.frame_count,
                    "kv": preproc_params.voltage,
                    "tilt_axis": preproc_params.tilt_axis,
                    "pixel_size": preproc_params.pixel_size,
                    "manual_tilt_offset": -tilt_offset,
                    "node_creator_queue": machine_config.node_creator_queue,
                    "search_map_id": ts.search_map_id,
                    "x_location": ts.x_location,
                    "y_location": ts.y_location,
                },
            }
            if _transport_object:
                logger.info(f"Sending Zocalo message for processing: {zocalo_message}")
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
            else:
                logger.info(
                    f"No transport object found. Zocalo message would be {zocalo_message}"
                )
    db.commit()


@tomo_router.post("/visits/{visit_name}/rerun_tilt_series")
def register_tilt_series_for_rerun(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    """Set processing to false for cases where an extra tilt is found for a series"""
    session_id = tilt_series_info.session_id
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.tag == tilt_series_info.tag)
        .where(TiltSeries.rsync_source == tilt_series_info.source)
    ).all()
    for ts in tilt_series_db:
        ts.processing_requested = False
        db.add(ts)
    db.commit()


class TiltInfo(BaseModel):
    tilt_series_tag: str
    movie_path: str
    source: str


@tomo_router.post("/visits/{visit_name}/{session_id}/tilt")
async def register_tilt(
    visit_name: str, session_id: MurfeySessionID, tilt_info: TiltInfo, db=murfey_db
):
    def _add_tilt():
        tilt_series_id = (
            db.exec(
                select(TiltSeries)
                .where(TiltSeries.tag == tilt_info.tilt_series_tag)
                .where(TiltSeries.session_id == session_id)
                .where(TiltSeries.rsync_source == tilt_info.source)
            )
            .one()
            .id
        )
        if db.exec(
            select(Tilt)
            .where(Tilt.movie_path == tilt_info.movie_path)
            .where(Tilt.tilt_series_id == tilt_series_id)
        ).all():
            return
        tilt = Tilt(movie_path=tilt_info.movie_path, tilt_series_id=tilt_series_id)
        db.add(tilt)
        db.commit()

    try:
        _add_tilt()
    except OperationalError:
        await asyncio.sleep(30)
        _add_tilt()


correlative_router = APIRouter(
    prefix="/workflow/correlative",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: Correlative Imaging"],
)


class Sample(BaseModel):
    sample_group_id: int
    sample_id: int
    subsample_id: int
    image_path: Optional[Path] = None


@correlative_router.get("/visit/{visit_name}/samples")
def get_samples(visit_name: str, db=ispyb_db) -> List[Sample]:
    proposal_id = get_proposal_id(visit_name[:2], visit_name.split("-")[0][2:], db)
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


@correlative_router.post("/visit/{visit_name}/sample_group")
def register_sample_group(visit_name: str, db=ispyb_db) -> dict:
    proposal_id = get_proposal_id(visit_name[:2], visit_name.split("-")[0][2:], db=db)
    record = BLSampleGroup(proposalId=proposal_id)
    if _transport_object:
        return _transport_object.do_insert_sample_group(record)
    return {"success": False}


class BLSampleParameters(BaseModel):
    sample_group_id: int


@correlative_router.post("/visit/{visit_name}/sample")
def register_sample(visit_name: str, sample_params: BLSampleParameters) -> dict:
    record = BLSample()
    if _transport_object:
        return _transport_object.do_insert_sample(record, sample_params.sample_group_id)
    return {"success": False}


class BLSubSampleParameters(BaseModel):
    sample_id: int
    image_path: Optional[Path] = None


@correlative_router.post("/visit/{visit_name}/subsample")
def register_subsample(
    visit_name: str, subsample_params: BLSubSampleParameters
) -> dict:
    record = BLSubSample(
        blSampleId=subsample_params.sample_id, imgFilePath=subsample_params.image_path
    )
    if _transport_object:
        return _transport_object.do_insert_subsample(record)
    return {"success": False}


class BLSampleImageParameters(BaseModel):
    sample_id: int
    sample_path: Path


@correlative_router.post("/visit/{visit_name}/sample_image")
def register_sample_image(
    visit_name: str, sample_image_params: BLSampleImageParameters
) -> dict:
    record = BLSampleImage(
        blSampleId=sample_image_params.sample_id,
        imageFullPath=sample_image_params.image_path,
    )
    if _transport_object:
        return _transport_object.do_insert_sample_image(record)
    return {"success": False}


class MillingParameters(BaseModel):
    lamella_number: int
    images: List[str]
    raw_directory: str


@correlative_router.post("/visits/{year}/{visit_name}/{session_id}/make_milling_gif")
async def make_gif(
    year: int,
    visit_name: str,
    session_id: int,
    gif_params: MillingParameters,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    output_dir = (
        Path(machine_config.rsync_basepath)
        / secure_filename(year)
        / secure_filename(visit_name)
        / "processed"
    )
    output_dir.mkdir(exist_ok=True)
    output_dir = output_dir / secure_filename(gif_params.raw_directory)
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"lamella_{gif_params.lamella_number}_milling.gif"
    image_full_paths = [
        output_dir.parent / gif_params.raw_directory / i for i in gif_params.images
    ]
    if Image is not None:
        images = [Image.open(f) for f in image_full_paths]
    else:
        images = []
    for im in images:
        im.thumbnail((512, 512))
    images[0].save(
        output_path,
        format="GIF",
        append_images=images[1:],
        save_all=True,
        duration=30,
        loop=0,
    )
    return {"output_gif": str(output_path)}
