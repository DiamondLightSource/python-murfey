from __future__ import annotations

import datetime
import logging
import random
from functools import lru_cache
from itertools import count
from pathlib import Path
from typing import Dict, List, Optional

import packaging.version
import sqlalchemy
from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse
from ispyb.sqlalchemy import BLSession
from PIL import Image
from pydantic import BaseModel, BaseSettings
from sqlalchemy import func
from sqlmodel import col, select
from werkzeug.utils import secure_filename

import murfey.server.api.bootstrap
import murfey.server.prometheus as prom
import murfey.server.websocket as ws
import murfey.util.eer
from murfey.server import (
    _flush_grid_square_records,
    _flush_tomography_preprocessing,
    _murfey_id,
    _register_picked_particles_use_diameter,
    feedback_callback,
    get_hostname,
    get_microscope,
    sanitise,
    sanitise_path,
)
from murfey.server import shutdown as _shutdown
from murfey.server import templates
from murfey.server.api.auth import validate_token
from murfey.server.config import from_file
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
    TomographyPreprocessingParameters,
    TomographyProcessingParameters,
)
from murfey.util.models import (
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
    PostInfo,
    PreprocessingParametersTomo,
    ProcessFile,
    ProcessingJobParameters,
    ProcessingParametersSPA,
    ProcessingParametersTomo,
    RegistrationMessage,
    RsyncerInfo,
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

log = logging.getLogger("murfey.server.demo_api")

tags_metadata = [murfey.server.api.bootstrap.tag]

router = APIRouter(dependencies=[Depends(validate_token)])
router.raw_count = 2


global_counter = count()


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()

machine_config: dict = {}
if settings.murfey_machine_configuration:
    microscope = get_microscope()
    machine_config = dict(
        from_file(Path(settings.murfey_machine_configuration), microscope)
    )


# This will be the homepage for a given microscope.
@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "hostname": get_hostname(),
            "microscope": get_microscope(),
            "version": murfey.__version__,
        },
    )


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
    return {
        "microscope": microscope,
        "display_name": machine_config.get("display_name", ""),
    }


@router.get("/microscope_image/")
def get_mic_image():
    if machine_config.get("image_path"):
        return FileResponse(machine_config["image_path"])
    return None


@router.get("/visits/")
def all_visit_info(request: Request):
    microscope = get_microscope()
    return_query = [
        {
            "Start date": datetime.datetime.now(),
            "End date": datetime.datetime.now(),
            "Visit name": "dummy",
            "Time remaining": 0,
        }
    ]  # "Proposal title": visit.proposal_title

    return templates.TemplateResponse(
        request=request,
        name="activevisits.html",
        context={"info": return_query, "microscope": microscope},
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
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(0)
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    return rsyncer_info


@router.get("/clients/{client_id}/rsyncers")
def get_rsyncers_for_client(client_id: int, db=murfey_db):
    log.info("rsyncers requested")
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    )
    res = rsync_instances.all()
    log.info(res)
    return res


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
    rsync_instance.files_counted += 1
    db.add(rsync_instance)
    db.commit()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_count
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
    rsync_instance.files_transferred += 1
    db.add(rsync_instance)
    db.commit()


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


@router.post("/clients/{client_id}/spa_processing_parameters")
def register_spa_proc_params(
    client_id: int, proc_params: ProcessingParametersSPA, db=murfey_db
):
    log.info(
        f"Registration request for SPA processing parameters with data: {proc_params.json()}"
    )
    try:
        client = db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        ).one()
        session_id = client.session_id
        collected_ids = db.exec(
            select(
                DataCollectionGroup,
                DataCollection,
                ProcessingJob,
                AutoProcProgram,
            )
            .where(DataCollectionGroup.session_id == session_id)
            .where(DataCollectionGroup.tag == proc_params.tag)
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()
        params = SPARelionParameters(
            pj_id=collected_ids[2].id,
            angpix=proc_params.pixel_size_on_image,
            dose_per_frame=proc_params.dose_per_frame,
            gain_ref=proc_params.gain_ref,
            voltage=proc_params.voltage,
            motion_corr_binning=proc_params.motion_corr_binning,
            eer_grouping=proc_params.eer_fractionation,
            symmetry=proc_params.symmetry,
            particle_diameter=proc_params.particle_diameter,
            downscale=proc_params.downscale,
            boxsize=proc_params.boxsize,
            small_boxsize=proc_params.small_boxsize,
            mask_diameter=proc_params.mask_diameter,
        )
        feedback_params = SPAFeedbackParameters(
            pj_id=collected_ids[2].id,
            estimate_particle_diameter=proc_params.particle_diameter is None,
            hold_class2d=False,
            hold_class3d=False,
            class_selection_score=0,
            star_combination_job=0,
            initial_model="",
            next_job=0,
        )
    except Exception as e:
        log.warning(f"registration failed: {e}")
        return
    db.add(params)
    db.add(feedback_params)
    db.commit()


@router.post("/clients/{client_id}/tomography_preprocessing_parameters")
def register_tomo_preproc_params(
    client_id: int, proc_params: PreprocessingParametersTomo, db=murfey_db
):
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    session_id = client.session_id
    log.info(
        f"Registering tomography preprocessing parameters {sanitise(proc_params.tag)}, {sanitise(proc_params.tilt_series_tag)}, {session_id}"
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
        select(func.count(TomographyPreprocessingParameters.dcg_id)).where(
            TomographyPreprocessingParameters.dcg_id == collected_ids[0].id
        )
    ).one():
        params = TomographyPreprocessingParameters(
            dcg_id=collected_ids[0].id,
            pixel_size=proc_params.pixel_size_on_image,
            dose_per_frame=proc_params.dose_per_frame,
            gain_ref=proc_params.gain_ref,
            motion_corr_binning=proc_params.motion_corr_binning,
            voltage=proc_params.voltage,
            # manual_tilt_offset=proc_params.manual_tilt_offset,
        )
        db.add(params)
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
    session_id: int,
    gsid: int,
    grid_square_params: GridSquareParameters,
    db=murfey_db,
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
        jpeg_size = Image.open(grid_square_params.image).size
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
    jpeg_size = Image.open(foil_hole_params.image).size
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
    tilt_series = TiltSeries(
        session_id=session_id,
        tag=tilt_series_info.tag,
        rsync_source=tilt_series_info.source,
    )
    db.add(tilt_series)
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
        ts.complete = True
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
def register_tilt(visit_name: str, client_id: int, tilt_info: TiltInfo, db=murfey_db):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
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
    tilt = Tilt(movie_path=tilt_info.movie_path, tilt_series_id=tilt_series_id)
    db.add(tilt)
    db.commit()


@router.get("/visits_raw", response_model=List[Visit])
def get_current_visits():
    return [
        Visit(
            start=datetime.datetime.now(),
            end=datetime.datetime.now() + datetime.timedelta(days=1),
            session_id=1,
            name="cm31111-2",
            beamline="m12",
            proposal_title="Nothing of importance",
        ),
        Visit(
            start=datetime.datetime.now(),
            end=datetime.datetime.now() + datetime.timedelta(days=1),
            session_id=1,
            name="cm31111-3",
            beamline="m12",
            proposal_title="Nothing of importance",
        ),
    ]


@router.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str):
    microscope = get_microscope()
    query = [
        BLSession(
            proposalId=1,
            beamLineName=microscope,
            endDate=datetime.datetime.now() + datetime.timedelta(days=1),
            startDate=datetime.datetime.now(),
            visitNumber=1,
        )
    ]
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
        request=request,
        name="visit.html",
        context={"visit": return_query},
    )


@router.post("/visits/{visit_name}/context")
async def register_context(context_info: ContextInfo):
    log.info(
        f"Context {context_info.experiment_type}:{context_info.acquisition_software} registered"
    )
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
    pass


@router.post("/visits/{visit_name}/spa_processing")
async def request_spa_processing(visit_name: str, proc_params: SPAProcessingParameters):
    log.info("SPA processing requested")
    return proc_params


class Tag(BaseModel):
    tag: str


@router.post("/visits/{visit_name}/{client_id}/flush_spa_processing")
def flush_spa_processing(visit_name: str, client_id: int, tag: Tag, db=murfey_db):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
    stashed_files = db.exec(
        select(PreprocessStash).where(PreprocessStash.session_id == session_id)
    ).all()
    if not stashed_files:
        return
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == tag.tag)
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
    proc_params = dict(params[0])
    feedback_params = params[1]
    if not proc_params:
        visit_name = visit_name.replace("\r\n", "").replace("\n", "")
        log.warning(
            f"No SPA processing parameters found for client {sanitise(str(client_id))} on visit {sanitise(visit_name)}"
        )
        return

    detached_ids = [c.id for c in collected_ids]

    murfey_ids = _murfey_id(
        detached_ids[3], db, number=2 * len(stashed_files), close=False
    )
    feedback_params.picker_murfey_id = murfey_ids[1]
    db.add(feedback_params)
    for i, f in enumerate(stashed_files):
        p = Path(f.mrc_out)
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        movie = Movie(
            murfey_id=murfey_ids[2 * i],
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
            foil_hole_id=f.foil_hole_id,
        )
        db.add(movie)
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config["feedback_queue"],
                "node_creator_queue": machine_config["node_creator_queue"],
                "dcid": detached_ids[1],
                "autoproc_program_id": detached_ids[3],
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params["angpix"],
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[2 * i],
                "ft_bin": proc_params["motion_corr_binning"],
                "fm_dose": proc_params["dose_per_frame"],
                "gain_ref": (
                    str(machine_config["rsync_basepath"] / proc_params["gain_ref"])
                    if proc_params["gain_ref"]
                    else proc_params["gain_ref"]
                ),
                "picker_uuid": murfey_ids[2 * i + 1],
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            },
        }
        log.info(f"Launching SPA preprocessing with Zoaclo message: {zocalo_message}")
        db.delete(f)
    db.commit()

    return


@router.post("/visits/{visit_name}/{client_id}/spa_preprocess")
async def request_spa_preprocessing(
    visit_name: str, client_id: int, proc_file: SPAProcessFile, db=murfey_db
):
    parts = [secure_filename(p) for p in Path(proc_file.path).parts]
    visit_idx = parts.index(visit_name)
    core = Path("/") / Path(*parts[: visit_idx + 1])
    ppath = Path("/") / Path(*parts)
    sub_dataset = ppath.relative_to(core).parts[0]
    for i, p in enumerate(ppath.parts):
        if p.startswith("raw"):
            movies_path_index = i
            break
    else:
        raise ValueError(f"{proc_file.path} does not contain a raw directory")
    mrc_out = (
        core
        / machine_config["processed_directory_name"]
        / sub_dataset
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
            .where(
                DataCollectionGroup.session_id == session_id
                and DataCollectionGroup.tag == "spa"
            )
            .where(DataCollectionGroup.tag == proc_file.tag)
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()

        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], db, number=2)

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

        if not mrc_out.parent.exists():
            Path(secure_filename(mrc_out)).parent.mkdir(parents=True)
        log.info("Sending Zocalo message")
        movie = db.exec(select(Movie).where(Movie.murfey_id == murfey_ids[0])).one()
        movie.preprocessed = True
        db.add(movie)
        db.commit()
        _register_picked_particles_use_diameter(
            {
                "session_id": session_id,
                "extraction_parameters": {
                    "micrographs_file": "MotionCorr/job002/micrographs.star",
                    "coord_list_file": "AutoPick/job007/coords.star",
                    "extract_file": "Extract/job008/particles.star",
                    "ctf_values": {
                        "CtfMaxResolution": 4,
                        "CtfFigureOfMerit": 0.8,
                        "DefocusU": 1,
                        "DefocusV": 1,
                        "DefocusAngle": 10,
                        "CtfImage": "CtfFind/job006/ctf.mrc",
                    },
                },
                "particle_diameters": [random.randint(20, 30) for i in range(400)],
                "program_id": detached_ids[3],
            },
            _db=db,
            demo=True,
        )
        prom.preprocessed_movies.labels(processing_job=detached_ids[2]).inc()

    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            tag=proc_file.tag,
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            foil_hole_id=foil_hole_id,
        )
        db.add(for_stash)
        db.commit()

    return proc_file


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
    _flush_tomography_preprocessing(zocalo_message)
    return


@router.post("/visits/{visit_name}/{client_id}/tomography_preprocess")
async def request_tomography_preprocessing(
    visit_name: str, client_id: int, proc_file: ProcessFile, db=murfey_db
):
    if not sanitise_path(Path(proc_file.path)).exists():
        log.warning(
            f"{sanitise(str(proc_file.path))} has not been transferred before preprocessing"
        )
    log.info(f"Tomo preprocesing requested for {sanitise(str(proc_file.path))}")
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path("/".join(secure_filename(p) for p in Path(proc_file.path).parts))
    sub_dataset = (
        ppath.relative_to(core).parts[0]
        if len(ppath.relative_to(core).parts) > 1
        else ""
    )
    mrc_out = (
        core
        / "processed"
        / sub_dataset
        / "MotionCorr"
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    mrc_out = Path("/".join(secure_filename(p) for p in mrc_out.parts))
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
        .where(DataCollectionGroup.id == DataCollection.dcg_id)
        .where(DataCollection.tag == proc_file.tag)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == "em-tomo-preprocess")
    ).all()
    if data_collection:
        if not mrc_out.parent.exists():
            mrc_out.parent.mkdir(parents=True)
        feedback_callback(
            {},
            {
                "register": "motion_corrected",
                "movie": str(proc_file.path),
                "mrc_out": str(mrc_out),
                "movie_id": proc_file.mc_uuid,
                "fm_int_file": proc_file.eer_fractionation_file,
                "program_id": data_collection[0][3].id,
            },
        )
        await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
        mrc_out.touch()
    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            tag=proc_file.tag,
        )
        db.add(for_stash)
        db.commit()
        db.close()
    return proc_file


@router.get("/version")
def get_version(client_version: str = ""):
    result = {
        "server": murfey.__version__,
        "oldest-supported-client": murfey.__supported_client_version__,
    }

    if client_version:
        client = packaging.version.parse(client_version)
        server = packaging.version.parse(murfey.__version__)
        minimum_version = packaging.version.parse(murfey.__supported_client_version__)
        result["client-needs-update"] = minimum_version > client
        result["client-needs-downgrade"] = client > server

    return result


@router.get("/shutdown", include_in_schema=False)
def shutdown():
    """A method to stop the server. This should be removed before Murfey is
    deployed in production. To remove it we need to figure out how to control
    to process (eg. systemd) and who to run it as."""
    log.info("Server shutdown request received")
    _shutdown()
    return {"success": True}


@router.post("/visits/{visit_name}/suggested_path")
def suggest_path(visit_name, params: SuggestedPathParameters):
    count: int | None = router.raw_count
    check_path = (
        machine_config["rsync_basepath"] / params.base_path
        if machine_config
        else Path(f"/dls/{get_microscope()}") / params.base_path
    )
    check_path = check_path.parent / f"{check_path.stem}{count}{check_path.suffix}"
    check_path_name = check_path.name
    while check_path.exists():
        count = count + 1 if count else 2
        check_path = check_path.parent / f"{check_path_name}{count}"
    router.raw_count += 1
    return {"suggested_path": check_path.relative_to(machine_config["rsync_basepath"])}


@router.get("/sessions/{session_id}/data_collection_groups")
def get_dc_groups(session_id: int, db=murfey_db):
    data_collection_groups = db.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.session_id == session_id)
    ).all()
    return {dcg.tag: dcg for dcg in data_collection_groups}


@router.post("/visits/{visit_name}/{client_id}/register_data_collection_group")
def register_dc_group(
    visit_name: str, client_id: int, dcg_params: DCGroupParameters, db=murfey_db
):
    log.info(f"Registering data collection group on microscope {get_microscope()}")
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
        dcgid = next(global_counter)
        murfey_dcg = DataCollectionGroup(
            id=dcgid,
            session_id=client.session_id,
            tag=dcg_params.tag,
            atlas=dcg_params.atlas,
            sample=dcg_params.sample,
        )
        db.add(murfey_dcg)
        db.commit()

        if dcg_params.experiment_type == "single particle":
            dcid = next(global_counter)
            murfey_dc = DataCollection(
                id=dcid,
                tag=dcg_params.tag,
                dcg_id=dcgid,
            )
            db.add(murfey_dc)
            db.commit()

            pjids = [next(global_counter) for _ in range(4)]

            murfey_pj_pre = ProcessingJob(
                id=pjids[0], recipe="em-spa-preprocess", dc_id=dcid
            )
            murfey_pj_ext = ProcessingJob(
                id=pjids[1], recipe="em-spa-extract", dc_id=dcid
            )
            murfey_pj_2d = ProcessingJob(
                id=pjids[2], recipe="em-spa-class2d", dc_id=dcid
            )
            murfey_pj_3d = ProcessingJob(
                id=pjids[3], recipe="em-spa-class3d", dc_id=dcid
            )
            db.add(murfey_pj_pre)
            db.add(murfey_pj_ext)
            db.add(murfey_pj_2d)
            db.add(murfey_pj_3d)
            db.commit()

            murfey_app_pre = AutoProcProgram(id=next(global_counter), pj_id=pjids[0])
            murfey_app_ext = AutoProcProgram(id=next(global_counter), pj_id=pjids[1])
            murfey_app_2d = AutoProcProgram(id=next(global_counter), pj_id=pjids[2])
            murfey_app_3d = AutoProcProgram(id=next(global_counter), pj_id=pjids[3])
            db.add(murfey_app_pre)
            db.add(murfey_app_ext)
            db.add(murfey_app_2d)
            db.add(murfey_app_3d)
            db.commit()

        if global_state.get("data_collection_group_ids") and isinstance(
            global_state["data_collection_group_ids"], dict
        ):
            global_state["data_collection_group_ids"] = {
                **global_state["data_collection_group_ids"],
                dcg_params.tag: dcgid,
            }
        else:
            global_state["data_collection_group_ids"] = {dcg_params.tag: dcgid}
    if dcg_params.atlas:
        _flush_grid_square_records(
            {"session_id": client.session_id, "tag": dcg_params.tag}, demo=True
        )
    return dcg_params


@router.post("/visits/{visit_name}/{client_id}/start_data_collection")
def start_dc(
    visit_name: str, client_id: int, dc_params: DCParameters, db=murfey_db
) -> Optional[DCParameters]:
    dcg_tag = dc_params.source.replace("\r\n", "").replace("\n", "")
    log.info(
        f"Starting data collection, data collection group tag {dcg_tag} and data collection tag {dc_params.tag}"
    )
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    dcg = db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.tag == dcg_tag)
        .where(DataCollectionGroup.session_id == client.session_id)
    ).one()
    dc_tag = dc_params.tag
    if db.exec(
        select(DataCollection)
        .where(DataCollection.tag == dc_tag)
        .where(DataCollection.dcg_id == dcg.id)
    ).all():
        return None
    dc_id = next(global_counter)
    murfey_dc = DataCollection(
        id=dc_id,
        client=client_id,
        tag=dc_tag,
        dcg_id=dcg.id,
    )
    db.add(murfey_dc)
    db.commit()
    pj_id_preproc = next(global_counter)
    pj_id_align = next(global_counter)
    murfey_pj = ProcessingJob(
        id=pj_id_preproc,
        recipe="em-tomo-preprocess",
        dc_id=dc_id,
    )
    db.add(murfey_pj)
    murfey_pj = ProcessingJob(
        id=pj_id_align,
        recipe="em-tomo-align",
        dc_id=dc_id,
    )
    db.add(murfey_pj)
    murfey_app = AutoProcProgram(id=pj_id_preproc, pj_id=pj_id_preproc)
    db.add(murfey_app)
    murfey_app = AutoProcProgram(id=pj_id_align, pj_id=pj_id_align)
    db.add(murfey_app)
    db.commit()
    db.close()
    if global_state.get("data_collection_ids") and isinstance(
        global_state["data_collection_ids"], dict
    ):
        global_state["data_collection_ids"] = {
            **global_state["data_collection_ids"],
            dc_params.tag: 1,
        }
    else:
        global_state["data_collection_ids"] = {dc_params.tag: 1}
    if dc_params.exposure_time:
        prom.exposure_time.set(dc_params.exposure_time)
    return dc_params


@router.post("/visits/{visit_name}/{client_id}/register_processing_job")
def register_proc(visit_name, client_id: int, proc_params: ProcessingJobParameters):
    log.info("Registering processing job")
    if global_state.get("processing_job_ids"):
        assert isinstance(global_state["processing_job_ids"], dict)
        global_state["processing_job_ids"] = {
            **{
                k: v
                for k, v in global_state["processing_job_ids"].items()
                if k != proc_params.tag
            },
            proc_params.tag: {
                **global_state["processing_job_ids"].get(proc_params.tag, {}),
                proc_params.recipe: 1,
            },
        }
    else:
        global_state["processing_job_ids"] = {proc_params.tag: {proc_params.recipe: 1}}
    if global_state.get("autoproc_program_ids"):
        assert isinstance(global_state["autoproc_program_ids"], dict)
        global_state["autoproc_program_ids"] = {
            **global_state["autoproc_program_ids"],
            proc_params.tag: {
                **global_state["autoproc_program_ids"].get(proc_params.tag, {}),
                proc_params.recipe: 1,
            },
        }
    else:
        global_state["autoproc_program_ids"] = {
            proc_params.tag: {proc_params.recipe: 1}
        }
    log.info("Processing job registered")
    return proc_params


@router.post("/visits/{visit_name}/write_connections_file")
def write_conn_file(visit_name, params: ConnectionFileParameters):
    filepath = (
        Path(machine_config["rsync_basepath"])
        / (machine_config.get("rsync_module") or "data")
        / str(datetime.datetime.now().year)
    )
    log.info(f"Write to connection file at {filepath}")


@router.post("/visits/{visit_name}/process_gain")
async def process_gain(visit_name, gain_reference_params: GainReference):
    if machine_config.get("rsync_basepath"):
        filepath = (
            Path(machine_config["rsync_basepath"])
            / (machine_config.get("rsync_module") or "data")
            / str(datetime.datetime.now().year)
            / visit_name
        )
    else:
        return {"gain_ref": None}
    return {
        "gain_ref": (filepath / "processing" / "gain.mrc").relative_to(
            Path(machine_config["rsync_basepath"])
        )
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
    return sid


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
    if db.exec(
        select(ClientEnvironment).where(ClientEnvironment.session_id == session_id)
    ).all():
        return
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    return


@router.delete("/sessions/{session_id}")
def remove_session_by_id(session_id: int, db=murfey_db):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    return


@router.post("/visits/{visit_name}/eer_fractionation_file")
async def write_eer_fractionation_file(
    visit_name: str, fractionation_params: FractionationParameters
) -> dict:
    file_path = (
        Path(machine_config["rsync_basepath"])
        / (machine_config["rsync_module"] or "data")
        / str(datetime.datetime.now().year)
        / secure_filename(visit_name)
        / secure_filename(fractionation_params.fractionation_file_name)
    )
    log.info(f"EER fractionation file {file_path} creation requested")
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
            f"{num_eer_frames} {fractionation_params.fractionation} {fractionation_params.dose_per_frame}"
        )
    return {"eer_fractionation_file": str(file_path)}


@router.post("/visits/{visit_name}/monitoring/{on}")
def change_monitoring_status(visit_name: str, on: int):
    prom.monitoring_switch.labels(visit=visit_name)
    prom.monitoring_switch.labels(visit=visit_name).set(on)


@router.get("/visits/{visit_name}/upstream_visits")
def find_upstream_visits(visit_name: str):
    upstream_visits = {}
    for p in machine_config["upstream_data_directories"]:
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = v / machine_config["processed_directory_name"]
    return upstream_visits


def _get_upstream_tiff_dirs(visit_name: str) -> List[Path]:
    tiff_dirs = []
    for directory_name in machine_config["upstream_data_tiff_locations"]:
        for p in machine_config["upstream_data_directories"]:
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
    upstream_tiff_paths = []
    tiff_dirs = _get_upstream_tiff_dirs(visit_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
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


@router.post("/failed_client_post")
def failed_client_post(post_info: PostInfo):
    log.info("Post failed")
    return
