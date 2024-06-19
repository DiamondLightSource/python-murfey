from __future__ import annotations

import asyncio
import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import sqlalchemy
from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse
from ispyb.sqlalchemy import AutoProcProgram as ISPyBAutoProcProgram
from ispyb.sqlalchemy import (
    BLSample,
    BLSampleGroup,
    BLSampleImage,
    BLSession,
    BLSubSample,
    Proposal,
)
from PIL import Image
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.exc import NoResultFound, OperationalError
from sqlmodel import col, select
from werkzeug.utils import secure_filename

import murfey.server.ispyb
import murfey.server.prometheus as prom
import murfey.server.websocket as ws
import murfey.util.eer
from murfey.server import (
    _midpoint,
    _murfey_id,
    _transport_object,
    check_tilt_series_mc,
    get_all_tilts,
    get_angle,
    get_hostname,
    get_job_ids,
    get_machine_config,
    get_microscope,
    get_tomo_preproc_params,
    sanitise,
    templates,
)
from murfey.server.auth import validate_token
from murfey.server.config import from_file, settings
from murfey.server.gain import Camera, prepare_eer_gain, prepare_gain
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    PreprocessStash,
    ProcessingJob,
    RsyncInstance,
    Session,
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
    TomographyProcessingParameters,
)
from murfey.util.models import (
    BLSampleImageParameters,
    BLSampleParameters,
    BLSubSampleParameters,
    ClearanceKeys,
    ClientInfo,
    ConnectionFileParameters,
    ContextInfo,
    DCGroupParameters,
    DCParameters,
    File,
    FoilHoleParameters,
    FractionationParameters,
    GainReference,
    GridSquareParameters,
    MillingParameters,
    PostInfo,
    PreprocessingParametersTomo,
    ProcessFile,
    ProcessingJobParameters,
    ProcessingParametersSPA,
    ProcessingParametersTomo,
    RegistrationMessage,
    RsyncerInfo,
    Sample,
    SessionInfo,
    SPAProcessFile,
    SPAProcessingParameters,
    SuggestedPathParameters,
    TiltInfo,
    TiltSeriesGroupInfo,
    TiltSeriesInfo,
    Visit,
)
from murfey.util.spa_params import default_spa_parameters
from murfey.util.state import global_state

log = logging.getLogger("murfey.server.api")

machine_config = get_machine_config()

router = APIRouter(dependencies=[Depends(validate_token)])


# This will be the homepage for a given microscope.
@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "hostname": get_hostname(),
            "microscope": get_microscope(),
            "version": murfey.__version__,
        },
    )


@router.get("/health/")
def health_check(db=murfey.server.ispyb.DB):
    conn = db.connection()
    conn.close()
    return {
        "ispyb_connection": True,
        "rabbitmq_connection": _transport_object.transport.is_connected(),
    }


@router.get("/connections/")
def connections_check():
    return {"connections": list(ws.manager.active_connections.keys())}


@lru_cache(maxsize=1)
@router.get("/machine/")
def machine_info():
    if settings.murfey_machine_configuration:
        microscope = get_microscope()
        return from_file(settings.murfey_machine_configuration, microscope)
    return {}


@router.get("/microscope/")
def get_mic():
    microscope = get_microscope()
    return {"microscope": microscope}


@router.get("/visits/")
def all_visit_info(request: Request, db=murfey.server.ispyb.DB):
    microscope = machine_config.machine_override or get_microscope()
    visits = murfey.server.ispyb.get_all_ongoing_visits(microscope, db)

    if visits:
        return_query = [
            {
                "Start date": visit.start,
                "End date": visit.end,
                "Visit name": visit.name,
                "Time remaining": str(visit.end - datetime.datetime.now()),
            }
            for visit in visits
        ]  # "Proposal title": visit.proposal_title
        log.debug(
            f"{len(visits)} visits active for {microscope=}: {', '.join(v.name for v in visits)}"
        )
        return templates.TemplateResponse(
            "activevisits.html",
            {"request": request, "info": return_query, "microscope": microscope},
        )
    else:
        log.debug(f"No visits identified for {microscope=}")
        return templates.TemplateResponse(
            "activevisits.html",
            {"request": request, "info": [], "microscope": microscope},
        )


@router.post("/visits/{visit_name}")
def register_client_to_visit(visit_name: str, client_info: ClientInfo, db=murfey_db):
    client_env = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_info.id)
    ).one()
    if client_env:
        client_env.visit = visit_name
        db.add(client_env)
        db.commit()
        db.close()
    return client_info


@router.get("/num_movies")
def count_number_of_movies(db=murfey_db) -> Dict[str, int]:
    res = db.exec(
        select(Movie.tag, func.count(Movie.murfey_id)).group_by(Movie.tag)
    ).all()
    return {r[0]: r[1] for r in res}


@router.post("/visits/{visit_name}/rsyncer")
def register_rsyncer(visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db):
    rsync_instance = RsyncInstance(
        source=rsyncer_info.source,
        client_id=rsyncer_info.client_id,
        transferring=rsyncer_info.transferring,
        destination=rsyncer_info.destination,
    )
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(0)
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(
        0
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    return rsyncer_info


@router.get("/clients/{client_id}/rsyncers", response_model=List[RsyncInstance])
def get_rsyncers_for_client(client_id: int, db=murfey_db):
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    )
    return rsync_instances.all()


@router.post("/visits/{visit_name}/increment_rsync_file_count")
def increment_rsync_file_count(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.client_id == rsyncer_info.client_id,
        )
    ).one()
    rsync_instance.files_counted += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_count
    )
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_data_count
    )


@router.post("/visits/{visit_name}/increment_rsync_transferred_files")
def increment_rsync_transferred_files(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.client_id == rsyncer_info.client_id,
        )
    ).one()
    rsync_instance.files_transferred += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()


@router.post("/visits/{visit_name}/increment_rsync_transferred_files_prometheus")
def increment_rsync_transferred_files_prometheus(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_count)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.bytes)
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_data_count)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.data_bytes)


@router.get("/demo/visits_raw", response_model=List[Visit])
def get_current_visits_demo(db=murfey.server.ispyb.DB):
    microscope = "m12"
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@router.get("/clients/{client_id}/tomography_processing_parameters")
def get_tomo_proc_params(client_id: int, db=murfey_db) -> List[dict]:
    params = db.exec(
        select(TomographyProcessingParameters).where(
            TomographyProcessingParameters.client_id == client_id
        )
    ).all()
    return [p.json() for p in params]


@router.post("/clients/{client_id}/spa_processing_parameters")
def register_spa_proc_params(
    client_id: int, proc_params: ProcessingParametersSPA, db=murfey_db
):
    zocalo_message = {
        "register": "spa_processing_parameters",
        **dict(proc_params),
        "client_id": client_id,
    }
    if _transport_object:
        _transport_object.send(machine_config.feedback_queue, zocalo_message)


@router.get("/sessions/{session_id}/grid_squares")
def get_grid_squares(session_id: int, db=murfey_db):
    grid_squares = db.exec(
        select(GridSquare).where(GridSquare.session_id == session_id)
    ).all()
    tags = {gs.tag for gs in grid_squares}
    res = {}
    for t in tags:
        res[t] = [gs for gs in grid_squares if gs.tag == t]
    return res


@router.post("/sessions/{session_id}/grid_square/{gsid}")
def register_grid_square(
    session_id: int, gsid: int, grid_square_params: GridSquareParameters, db=murfey_db
):
    try:
        grid_square = db.exec(
            select(GridSquare)
            .where(GridSquare.name == gsid)
            .where(GridSquare.tag == grid_square_params.tag)
            .where(GridSquare.session_id == session_id)
        ).one()
        grid_square.x_location = grid_square_params.x_location
        grid_square.y_location = grid_square_params.y_location
        grid_square.x_stage_position = grid_square_params.x_stage_position
        grid_square.y_stage_position = grid_square_params.y_stage_position
    except Exception:
        if grid_square_params.image and Path(grid_square_params.image).is_file():
            jpeg_size = Image.open(grid_square_params.image).size
        else:
            jpeg_size = (0, 0)
        grid_square = GridSquare(
            name=gsid,
            session_id=session_id,
            tag=grid_square_params.tag,
            x_location=grid_square_params.x_location,
            y_location=grid_square_params.y_location,
            x_stage_position=grid_square_params.x_stage_position,
            y_stage_position=grid_square_params.y_stage_position,
            readout_area_x=grid_square_params.readout_area_x,
            readout_area_y=grid_square_params.readout_area_y,
            thumbnail_size_x=grid_square_params.thumbnail_size_x or jpeg_size[0],
            thumbnail_size_y=grid_square_params.thumbnail_size_y or jpeg_size[1],
            pixel_size=grid_square_params.pixel_size,
            image=grid_square_params.image,
        )
    db.add(grid_square)
    db.commit()
    db.close()


@router.get("/sessions/{session_id}/foil_hole/{fh_name}")
def get_foil_hole(session_id: int, fh_name: int, db=murfey_db) -> Dict[str, int]:
    foil_holes = db.exec(
        select(FoilHole, GridSquare)
        .where(FoilHole.name == fh_name)
        .where(FoilHole.session_id == session_id)
        .where(GridSquare.id == FoilHole.grid_square_id)
    ).all()
    return {f[1].tag: f[0].id for f in foil_holes}


@router.post("/sessions/{session_id}/grid_square/{gs_name}/foil_hole")
def register_foil_hole(
    session_id: int, gs_name: int, foil_hole_params: FoilHoleParameters, db=murfey_db
):
    try:
        gsid = (
            db.exec(
                select(GridSquare)
                .where(GridSquare.tag == foil_hole_params.tag)
                .where(GridSquare.session_id == session_id)
                .where(GridSquare.name == gs_name)
            )
            .one()
            .id
        )
    except NoResultFound:
        log.debug(
            f"Foil hole {foil_hole_params.name} could not be registered as grid square {gs_name} was not found"
        )
        return
    if foil_hole_params.image and Path(foil_hole_params.image).is_file():
        jpeg_size = Image.open(foil_hole_params.image).size
    else:
        jpeg_size = (0, 0)
    foil_hole = FoilHole(
        name=foil_hole_params.name,
        session_id=session_id,
        grid_square_id=gsid,
        x_location=foil_hole_params.x_location,
        y_location=foil_hole_params.y_location,
        x_stage_position=foil_hole_params.x_stage_position,
        y_stage_position=foil_hole_params.y_stage_position,
        readout_area_x=foil_hole_params.readout_area_x,
        readout_area_y=foil_hole_params.readout_area_y,
        thumbnail_size_x=foil_hole_params.thumbnail_size_x or jpeg_size[0],
        thumbnail_size_y=foil_hole_params.thumbnail_size_y or jpeg_size[1],
        pixel_size=foil_hole_params.pixel_size,
        image=foil_hole_params.image,
    )
    db.add(foil_hole)
    db.commit()
    db.close()


@router.post("/clients/{client_id}/tomography_preprocessing_parameters")
def register_tomo_preproc_params(
    client_id: int, proc_params: PreprocessingParametersTomo, db=murfey_db
):
    zocalo_message = {
        "register": "tomography_processing_parameters",
        **dict(proc_params),
        "client_id": client_id,
    }
    if _transport_object:
        _transport_object.send(machine_config.feedback_queue, zocalo_message)


@router.post("/clients/{client_id}/tomography_processing_parameters")
def register_tomo_proc_params(
    client_id: int, proc_params: ProcessingParametersTomo, db=murfey_db
):
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    session_id = client.session_id
    log.info(
        f"Registering tomography processing parameters {sanitise(proc_params.tag)}, {sanitise(proc_params.tilt_series_tag)}, {session_id}"
    )
    collected_ids = db.exec(
        select(
            DataCollectionGroup,
            DataCollection,
            ProcessingJob,
            AutoProcProgram,
        )
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == proc_params.tag)
        .where(DataCollection.tag == proc_params.tilt_series_tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == "em-tomo-preprocess")
    ).one()
    if not db.exec(
        select(func.count(TomographyProcessingParameters.pj_id)).where(
            TomographyProcessingParameters.pj_id == collected_ids[2].id
        )
    ).one():
        tomogram_params = TomographyProcessingParameters(
            pj_id=collected_ids[2].id, manual_tilt_offset=proc_params.manual_tilt_offset
        )
        db.add(tomogram_params)
    db.commit()
    db.close()


@router.get("/clients/{client_id}/spa_processing_parameters")
def get_spa_proc_params(client_id: int, db=murfey_db) -> List[dict]:
    params = db.exec(
        select(SPARelionParameters).where(SPARelionParameters.client_id == client_id)
    ).all()
    return [p.json() for p in params]


class Tag(BaseModel):
    tag: str


@router.post("/visits/{visit_name}/{client_id}/flush_spa_processing")
def flush_spa_processing(visit_name: str, client_id: int, tag: Tag, db=murfey_db):
    zocalo_message = {
        "register": "flush_spa_preprocess",
        "client_id": client_id,
        "tag": tag.tag,
    }
    if _transport_object:
        _transport_object.send(machine_config.feedback_queue, zocalo_message)
    return


class Source(BaseModel):
    rsync_source: str


@router.post("/visits/{visit_name}/{client_id}/flush_tomography_processing")
def flush_tomography_processing(
    visit_name: str, client_id: int, rsync_source: Source, db=murfey_db
):
    zocalo_message = {
        "register": "flush_tomography_preprocess",
        "client_id": client_id,
        "visit_name": visit_name,
        "data_collection_group_tag": rsync_source.rsync_source,
    }
    if _transport_object:
        _transport_object.send(machine_config.feedback_queue, zocalo_message)
    return


@router.post("/visits/{visit_name}/tilt_series")
def register_tilt_series(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(
                ClientEnvironment.client_id == tilt_series_info.client_id
            )
        )
        .one()
        .session_id
    )
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


@router.post("/sessions/{session_id}/tilt_series_length")
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


@router.post("/visits/{visit_name}/{client_id}/completed_tilt_series")
def register_completed_tilt_series(
    visit_name: str,
    client_id: int,
    tilt_series_group: TiltSeriesGroupInfo,
    db=murfey_db,
):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
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
        if check_tilt_series_mc(ts.id) and not ts.processing_requested:
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
            machine_config = get_machine_config()
            tilts = get_all_tilts(ts.id)
            ids = get_job_ids(ts.id, collected_ids[3].id)
            preproc_params = get_tomo_preproc_params(ids.dcgid)

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
            stack_file = (
                core
                / machine_config.processed_directory_name
                / sub_dataset
                / "align_output"
                / f"{ts.tag}_stack.mrc"
            )
            if not stack_file.parent.exists():
                stack_file.parent.mkdir(parents=True)
            tilt_offset = _midpoint([float(get_angle(t)) for t in tilts])
            zocalo_message = {
                "recipes": ["em-tomo-align"],
                "parameters": {
                    "input_file_list": str([[t, str(get_angle(t))] for t in tilts]),
                    "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
                    "dcid": ids.dcid,
                    "appid": ids.appid,
                    "stack_file": str(stack_file),
                    "pix_size": preproc_params.pixel_size,
                    "manual_tilt_offset": -tilt_offset,
                    "node_creator_queue": machine_config.node_creator_queue,
                },
            }
            if _transport_object:
                log.info(f"Sending Zocalo message for processing: {zocalo_message}")
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
            else:
                log.info(
                    f"No transport object found. Zocalo message would be {zocalo_message}"
                )
    db.commit()


@router.post("/visits/{visit_name}/rerun_tilt_series")
def register_tilt_series_for_rerun(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    """Set processing to false for cases where an extra tilt is found for a series"""
    session_id = (
        db.exec(
            select(ClientEnvironment).where(
                ClientEnvironment.client_id == tilt_series_info.client_id
            )
        )
        .one()
        .session_id
    )
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


@router.get("/clients/{client_id}/tilt_series/{tilt_series_tag}/tilts")
def get_tilts(client_id: int, tilt_series_tag: str, db=murfey_db):
    res = db.exec(
        select(ClientEnvironment, TiltSeries, Tilt)
        .where(ClientEnvironment.client_id == client_id)
        .where(TiltSeries.tag == tilt_series_tag)
        .where(TiltSeries.session_id == ClientEnvironment.session_id)
        .where(Tilt.tilt_series_id == TiltSeries.id)
    ).all()
    tilts: Dict[str, List[str]] = {}
    for el in res:
        if tilts.get(el[1].rsync_source):
            tilts[el[1].rsync_source].append(el[2].movie_path)
        else:
            tilts[el[1].rsync_source] = [el[2].movie_path]
    return tilts


@router.post("/visits/{visit_name}/{client_id}/tilt")
async def register_tilt(
    visit_name: str, client_id: int, tilt_info: TiltInfo, db=murfey_db
):
    def _add_tilt():
        session_id = (
            db.exec(
                select(ClientEnvironment).where(
                    ClientEnvironment.client_id == client_id
                )
            )
            .one()
            .session_id
        )
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


@router.get("/visits_raw", response_model=List[Visit])
def get_current_visits(db=murfey.server.ispyb.DB):
    microscope = get_microscope(machine_config=machine_config)
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@router.get("/visit/{visit_name}/samples")
def get_samples(visit_name: str, db=murfey.server.ispyb.DB) -> List[Sample]:
    return murfey.server.ispyb.get_sub_samples_from_visit(visit_name, db=db)


@router.post("/visit/{visit_name}/sample_group")
def register_sample_group(visit_name: str, db=murfey.server.ispyb.DB) -> dict:
    proposal_id = murfey.server.ispyb.get_proposal_id(
        visit_name[:2], visit_name.split("-")[0][2:], db=db
    )
    record = BLSampleGroup(proposalId=proposal_id)
    if _transport_object:
        return _transport_object.do_insert_sample_group(record)
    return {"success": False}


@router.post("/visit/{visit_name}/sample")
def register_sample(visit_name: str, sample_params: BLSampleParameters) -> dict:
    record = BLSample()
    if _transport_object:
        return _transport_object.do_insert_sample(record, sample_params.sample_group_id)
    return {"success": False}


@router.post("/visit/{visit_name}/subsample")
def register_subsample(
    visit_name: str, subsample_params: BLSubSampleParameters
) -> dict:
    record = BLSubSample(
        blSampleId=subsample_params.sample_id, imgFilePath=subsample_params.image_path
    )
    if _transport_object:
        return _transport_object.do_insert_subsample(record)
    return {"success": False}


@router.post("/visit/{visit_name}/sample_image")
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


@router.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str, db=murfey.server.ispyb.DB):
    microscope = get_microscope(machine_config=machine_config)
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
            BLSession.beamLineName,
            Proposal.proposalCode,
            Proposal.proposalNumber,
            BLSession.visit_number,
            Proposal.title,
        )
        .all()
    )
    if query:
        return_query = [
            {
                "Start date": id.startDate,
                "End date": id.endDate,
                "Beamline name": id.beamLineName,
                "Visit name": visit_name,
                "Time remaining": str(id.endDate - datetime.datetime.now()),
            }
            for id in query
            if id.proposalCode + str(id.proposalNumber) + "-" + str(id.visit_number)
            == visit_name
        ]  # "Proposal title": id.title
        return templates.TemplateResponse(
            "visit.html",
            {"request": request, "visit": return_query},
        )
    else:
        return None


@router.post("/visits/{visit_name}/context")
async def register_context(context_info: ContextInfo):
    await ws.manager.broadcast(f"Context registered: {context_info}")
    await ws.manager.set_state("experiment_type", context_info.experiment_type)
    await ws.manager.set_state(
        "acquisition_software", context_info.acquisition_software
    )


@router.post("/visits/{visit_name}/files")
async def add_file(file: File):
    message = f"File {file} transferred"
    log.info(message)
    await ws.manager.broadcast(f"File {file} transferred")
    return file


@router.post("/feedback")
async def send_murfey_message(msg: RegistrationMessage):
    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue, {"register": msg.registration}
        )


@router.post("/visits/{visit_name}/spa_processing")
async def request_spa_processing(visit_name: str, proc_params: SPAProcessingParameters):
    zocalo_message = {
        "parameters": {"ispyb_process": proc_params.job_id},
        "recipes": ["ispyb-relion"],
    }
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message)


@router.post("/visits/{visit_name}/{client_id}/spa_preprocess")
async def request_spa_preprocessing(
    visit_name: str, client_id: int, proc_file: SPAProcessFile, db=murfey_db
):
    parts = [secure_filename(p) for p in Path(proc_file.path).parts]
    visit_idx = parts.index(visit_name)
    core = Path("/") / Path(*parts[: visit_idx + 1])
    ppath = Path("/") / Path(*parts)
    sub_dataset = ppath.relative_to(core).parts[0]
    extra_path = machine_config.processed_extra_directory
    for i, p in enumerate(ppath.parts):
        if p.startswith("raw"):
            movies_path_index = i
            break
    else:
        raise ValueError(f"{proc_file.path} does not contain a raw directory")
    mrc_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / extra_path
        / "MotionCorr"
        / "job002"
        / "Movies"
        / "/".join(ppath.parts[movies_path_index + 1 : -1])
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    try:
        session_id = (
            db.exec(
                select(ClientEnvironment).where(
                    ClientEnvironment.client_id == client_id
                )
            )
            .one()
            .session_id
        )
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
        proc_params: dict | None = dict(params[0])
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
    except Exception:
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
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": detached_ids[1],
                "kv": proc_params["voltage"],
                "autoproc_program_id": detached_ids[3],
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pix_size": proc_params["angpix"],
                "image_number": proc_file.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_params["motion_corr_binning"],
                "fm_dose": proc_params["dose_per_frame"],
                "gain_ref": proc_params["gain_ref"],
                "picker_uuid": murfey_ids[1],
                "session_id": session_id,
                "particle_diameter": proc_params["particle_diameter"] or 0,
                "fm_int_file": proc_file.eer_fractionation_file,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            },
        }
        # log.info(f"Sending Zocalo message {zocalo_message}")
        if _transport_object:
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            log.error(
                f"Pe-processing was requested for {sanitise(ppath.name)} but no Zocalo transport object was found"
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


@router.post("/visits/{visit_name}/{client_id}/tomography_preprocess")
async def request_tomography_preprocessing(
    visit_name: str, client_id: int, proc_file: ProcessFile, db=murfey_db
):
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path("/".join(secure_filename(p) for p in Path(proc_file.path).parts))
    sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
    mrc_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / "MotionCorr"
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    mrc_out = Path("/".join(secure_filename(p) for p in mrc_out.parts))
    ctf_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / "CTF"
        / str(ppath.stem + "_ctf.mrc")
    )
    ctf_out = Path("/".join(secure_filename(p) for p in ctf_out.parts))
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
    data_collection = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == proc_file.group_tag)
        .where(DataCollection.tag == proc_file.tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == "em-tomo-preprocess")
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
        if not ctf_out.parent.exists():
            ctf_out.parent.mkdir(parents=True, exist_ok=True)
        zocalo_message = {
            "recipes": ["em-tomo-preprocess"],
            "parameters": {
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": dcid,
                # "timestamp": datetime.datetime.now(),
                "autoproc_program_id": appid,
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pix_size": (proc_file.pixel_size) * 10**10,
                "output_image": str(ctf_out),
                "image_number": proc_file.image_number,
                "kv": int(proc_file.voltage),
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_file.mc_binning,
                "fm_dose": proc_file.dose_per_frame,
                "gain_ref": (
                    str(machine_config.rsync_basepath / proc_file.gain_ref)
                    if proc_file.gain_ref
                    else proc_file.gain_ref
                ),
                "fm_int_file": proc_file.eer_fractionation_file,
            },
        }
        if _transport_object:
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            log.error(
                f"Pe-processing was requested for {sanitise(ppath.name)} but no Zocalo transport object was found"
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
    # await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
    return proc_file


@router.post("/visits/{visit_name}/suggested_path")
def suggest_path(visit_name, params: SuggestedPathParameters):
    count: int | None = None
    secure_path_parts = [secure_filename(p) for p in params.base_path.parts]
    base_path = "/".join(secure_path_parts)
    check_path = (
        machine_config.rsync_basepath / base_path
        if machine_config
        else Path(f"/dls/{get_microscope(machine_config=machine_config)}") / base_path
    )
    check_path_name = check_path.name
    while check_path.exists():
        count = count + 1 if count else 2
        check_path = check_path.parent / f"{check_path_name}{count}"
    if params.touch:
        check_path.mkdir(mode=0o750)
        if params.extra_directory:
            (check_path / secure_filename(params.extra_directory)).mkdir(mode=0o750)
    return {"suggested_path": check_path.relative_to(machine_config.rsync_basepath)}


@router.get("/sessions/{session_id}/data_collection_groups")
def get_dc_groups(session_id: int, db=murfey_db):
    data_collection_groups = db.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.session_id == session_id)
    ).all()
    return {dcg.tag: dcg for dcg in data_collection_groups}


@router.post("/visits/{visit_name}/{client_id}/register_data_collection_group")
def register_dc_group(
    visit_name, client_id: int, dcg_params: DCGroupParameters, db=murfey_db
):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    microscope = get_microscope(machine_config=machine_config)
    log.info(f"Registering data collection group on microscope {microscope}")
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    if dcg_murfey := db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == client.session_id)
        .where(DataCollectionGroup.tag == dcg_params.tag)
    ).all():
        dcg_murfey[0].atlas = dcg_params.atlas
        dcg_murfey[0].sample = dcg_params.sample
        db.add(dcg_murfey[0])
        db.commit()
    else:
        dcg_parameters = {
            "start_time": str(datetime.datetime.now()),
            "experiment_type": dcg_params.experiment_type,
            "experiment_type_id": dcg_params.experiment_type_id,
            "tag": dcg_params.tag,
            "client_id": client_id,
        }

        if _transport_object:
            _transport_object.send(
                machine_config.feedback_queue, {"register": "data_collection_group", **dcg_parameters, "microscope": microscope, "proposal_code": ispyb_proposal_code, "proposal_number": ispyb_proposal_number, "visit_number": ispyb_visit_number}  # type: ignore
            )
    return dcg_params


@router.post("/visits/{visit_name}/{client_id}/start_data_collection")
def start_dc(visit_name, client_id: int, dc_params: DCParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    log.info(
        f"Starting data collection on microscope {get_microscope(machine_config=machine_config)} "
        f"with basepath {machine_config.rsync_basepath} and directory {dc_params.image_directory}"
    )
    dc_parameters = {
        "visit": visit_name,
        "image_directory": str(
            machine_config.rsync_basepath / dc_params.image_directory
        ),
        "start_time": str(datetime.datetime.now()),
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
        "client_id": client_id,
    }

    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue,
            {
                "register": "data_collection",
                **dc_parameters,
                "microscope": get_microscope(machine_config=machine_config),
                "proposal_code": ispyb_proposal_code,
                "proposal_number": ispyb_proposal_number,
                "visit_number": ispyb_visit_number,
            },
        )
    if dc_params.exposure_time:
        prom.exposure_time.set(dc_params.exposure_time)
    return dc_params


@router.post("/visits/{visit_name}/{client_id}/register_processing_job")
def register_proc(
    visit_name: str, client_id: int, proc_params: ProcessingJobParameters, db=murfey_db
):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
    proc_parameters = {
        "session_id": session_id,
        "experiment_type": proc_params.experiment_type,
        "recipe": proc_params.recipe,
        "tag": proc_params.tag,
        "job_parameters": {
            k: v for k, v in proc_params.parameters.items() if v not in (None, "None")
        },
    }

    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue,
            {"register": "processing_job", **proc_parameters},
        )
    return proc_params


@router.post("/visits/{visit_name}/write_connections_file")
def write_conn_file(visit_name, params: ConnectionFileParameters):
    filepath = (
        Path(machine_config.rsync_basepath)
        / (machine_config.rsync_module or "data")
        / str(datetime.datetime.now().year)
        / secure_filename(visit_name)
    )
    with open(filepath / secure_filename(params.filename), "w") as f:
        for d in params.destinations:
            f.write(f"{d}\n")


@router.post("/visits/{visit_name}/process_gain")
async def process_gain(visit_name, gain_reference_params: GainReference):
    camera = getattr(Camera, machine_config.camera)
    if gain_reference_params.eer:
        executables = machine_config.external_executables_eer
    else:
        executables = machine_config.external_executables
    env = machine_config.external_environment
    safe_path_name = secure_filename(gain_reference_params.gain_ref.name)
    filepath = (
        Path(machine_config.rsync_basepath)
        / (machine_config.rsync_module or "data")
        / str(datetime.datetime.now().year)
        / secure_filename(visit_name)
        / machine_config.gain_directory_name
    )
    if gain_reference_params.eer:
        new_gain_ref, new_gain_ref_superres = await prepare_eer_gain(
            filepath / safe_path_name,
            executables,
            env,
        )
    else:
        new_gain_ref, new_gain_ref_superres = await prepare_gain(
            camera,
            filepath / safe_path_name,
            executables,
            env,
            rescale=gain_reference_params.rescale,
        )
    if new_gain_ref and new_gain_ref_superres:
        return {
            "gain_ref": new_gain_ref.relative_to(Path(machine_config.rsync_basepath)),
            "gain_ref_superres": new_gain_ref_superres.relative_to(
                Path(machine_config.rsync_basepath)
            ),
        }
    elif new_gain_ref:
        return {
            "gain_ref": new_gain_ref.relative_to(Path(machine_config.rsync_basepath)),
            "gain_ref_superres": None,
        }
    else:
        return {"gain_ref": str(filepath / safe_path_name), "gain_ref_superres": None}


@router.post("/visits/{visit_name}/eer_fractionation_file")
async def write_eer_fractionation_file(
    visit_name: str, fractionation_params: FractionationParameters
) -> dict:
    file_path = (
        Path(machine_config.rsync_basepath)
        / (machine_config.rsync_module or "data")
        / str(datetime.datetime.now().year)
        / secure_filename(visit_name)
        / "processing"
        / secure_filename(fractionation_params.fractionation_file_name)
    )
    if file_path.is_file():
        return {"eer_fractionation_file": str(file_path)}

    if fractionation_params.num_frames:
        num_eer_frames = fractionation_params.num_frames
    elif (
        fractionation_params.eer_path and Path(fractionation_params.eer_path).is_file()
    ):
        num_eer_frames = murfey.util.eer.num_frames(Path(fractionation_params.eer_path))
    else:
        log.warning(
            f"EER fractionation unable to find {fractionation_params.eer_path} "
            f"or use {fractionation_params.num_frames} frames"
        )
        return {"eer_fractionation_file": None}
    with open(file_path, "w") as frac_file:
        frac_file.write(
            f"{num_eer_frames} {fractionation_params.fractionation} {fractionation_params.dose_per_frame / fractionation_params.fractionation}"
        )
    return {"eer_fractionation_file": str(file_path)}


@router.post("/visits/{year}/{visit_name}/make_milling_gif")
async def make_gif(year, visit_name, gif_params: MillingParameters):
    output_dir = (
        Path(machine_config.rsync_basepath)
        / (machine_config.rsync_module or "data")
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
    images = [Image.open(f) for f in image_full_paths]
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


@router.post("/visits/{visit_name}/clean_state")
async def clean_state(visit_name, for_clearance: ClearanceKeys):
    if global_state.get("data_collection_group_ids") and isinstance(
        global_state["data_collection_group_ids"], dict
    ):
        global_state["data_collection_group_ids"] = {
            k: v
            for k, v in global_state["data_collection_group_ids"].items()
            if k not in for_clearance.data_collection_group
        }
    if global_state.get("data_collection_ids") and isinstance(
        global_state["data_collection_ids"], dict
    ):
        global_state["data_collection_ids"] = {
            k: v
            for k, v in global_state["data_collection_ids"].items()
            if k not in for_clearance.data_collection
        }
    if global_state.get("processing_job_ids") and isinstance(
        global_state["processing_job_ids"], dict
    ):
        global_state["processing_job_ids"] = {
            k: v
            for k, v in global_state["processing_job_ids"].items()
            if k not in for_clearance.processing_job
        }
    if global_state.get("autoproc_program_ids") and isinstance(
        global_state["autoproc_program_ids"], dict
    ):
        global_state["autoproc_program_ids"] = {
            k: v
            for k, v in global_state["autoproc_program_ids"].items()
            if k not in for_clearance.autoproc_program
        }


@router.get("/new_client_id/")
async def new_client_id(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    if not clients:
        return {"new_id": 0}
    sorted_ids = sorted([c.client_id for c in clients])
    return {"new_id": sorted_ids[-1] + 1}


@router.get("/clients")
async def get_clients(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    return clients


@router.get("/sessions")
async def get_sessions(db=murfey_db):
    sessions = db.exec(select(Session)).all()
    clients = db.exec(select(ClientEnvironment)).all()
    res = []
    for sess in sessions:
        r = {"session": sess, "clients": []}
        for cl in clients:
            if cl.session_id == sess.id:
                r["clients"].append(cl)
        res.append(r)
    return res


@router.post("/clients/{client_id}/session")
def link_client_to_session(client_id: int, sess: SessionInfo, db=murfey_db):
    sid = sess.session_id
    if sid is None:
        s = Session(name=sess.session_name)
        db.add(s)
        db.commit()
        sid = s.id
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    client.session_id = sid
    db.add(client)
    db.commit()
    db.close()
    return sid


@router.post("/clients/{client_id}/successful_processing")
def register_processing_success_in_ispyb(
    client_id: int, db=murfey.server.ispyb.DB, murfey_db=murfey_db
):
    session_id = (
        murfey_db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
    collected_ids = murfey_db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
    ).all()
    appids = [c[3].id for c in collected_ids]
    if _transport_object:
        apps = db.query(ISPyBAutoProcProgram).filter(
            ISPyBAutoProcProgram.autoProcProgramId.in_(appids)
        )
        for updated in apps:
            updated.processingStatus = True
            _transport_object.do_update_processing_status(updated)


@router.delete("/clients/{client_id}/session")
def remove_session(client_id: int, db=murfey_db):
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    session_id = client.session_id
    client.session_id = None
    db.add(client)
    db.commit()
    if session_id is None:
        return
    prom.monitoring_switch.remove(client.visit)
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    ).all()
    for ri in rsync_instances:
        prom.seen_files.remove(ri.source, client.visit)
        prom.transferred_files.remove(ri.source, client.visit)
        prom.transferred_files_bytes.remove(ri.source, client.visit)
        prom.seen_data_files.remove(ri.source, client.visit)
        prom.transferred_data_files.remove(ri.source, client.visit)
        prom.transferred_data_files_bytes.remove(ri.source, client.visit)
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
    ).all()
    for c in collected_ids:
        try:
            prom.preprocessed_movies.remove(c[2].id)
        except KeyError:
            continue
    if (
        len(
            db.exec(
                select(ClientEnvironment).where(
                    ClientEnvironment.session_id == session_id
                )
            ).all()
        )
        > 1
    ):
        return
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    return


@router.post("/visits/{visit_name}/monitoring/{on}")
def change_monitoring_status(visit_name: str, on: int):
    prom.monitoring_switch.labels(visit=visit_name)
    prom.monitoring_switch.labels(visit=visit_name).set(on)


@router.post("/failed_client_post")
def failed_client_post(post_info: PostInfo):
    zocalo_message = {
        "register": "failed_client_post",
        "url": post_info.url,
        "json": post_info.data,
    }
    if _transport_object:
        _transport_object.send(machine_config.feedback_queue, zocalo_message)


@router.get("/visits/{visit_name}/upstream_visits")
async def find_upstream_visits(visit_name: str):
    upstream_visits = {}
    # Iterates through provided upstream directories
    for p in machine_config.upstream_data_directories:
        # Looks for visit name in file path
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = v / machine_config.processed_directory_name
    return upstream_visits


def _get_upstream_tiff_dirs(visit_name: str) -> List[Path]:
    tiff_dirs = []
    for directory_name in machine_config.upstream_data_tiff_locations:
        for p in machine_config.upstream_data_directories:
            if (Path(p) / secure_filename(visit_name)).is_dir():
                processed_dir = Path(p) / secure_filename(visit_name) / directory_name
                tiff_dirs.append(processed_dir)
                break
    if not tiff_dirs:
        log.warning(
            f"No candidate directory found for upstream download from visit {sanitise(visit_name)}"
        )
    return tiff_dirs


@router.get("/visits/{visit_name}/upstream_tiff_paths")
async def gather_upstream_tiffs(visit_name: str):
    """
    Looks for TIFF files associated with the current session in the permitted storage
    servers, and returns their relative file paths as a list.
    """
    upstream_tiff_paths = []
    tiff_dirs = _get_upstream_tiff_dirs(visit_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
        for f in tiff_dir.glob("**/*.tif"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
    return upstream_tiff_paths


@router.get("/visits/{visit_name}/upstream_tiff/{tiff_path:path}")
async def get_tiff(visit_name: str, tiff_path: str):
    tiff_dirs = _get_upstream_tiff_dirs(visit_name)
    if not tiff_dirs:
        return None

    tiff_path = "/".join(secure_filename(p) for p in tiff_path.split("/"))
    for tiff_dir in tiff_dirs:
        test_path = tiff_dir / tiff_path
        if test_path.is_file():
            break
    else:
        log.warning(f"TIFF {tiff_path} not found")
        return None

    return FileResponse(path=test_path)
