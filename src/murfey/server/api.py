from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import packaging.version
import sqlalchemy
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
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
from sqlmodel import col, select
from werkzeug.utils import secure_filename

import murfey.server.bootstrap
import murfey.server.ispyb
import murfey.server.prometheus as prom
import murfey.server.websocket as ws
import murfey.util.eer
from murfey.server import (
    _murfey_id,
    _transport_object,
    get_hostname,
    get_machine_config,
    get_microscope,
    templates,
)
from murfey.server.config import from_file, settings
from murfey.server.gain import Camera, prepare_gain
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
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
    FractionationParameters,
    GainReference,
    MillingParameters,
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
    TiltSeriesProcessingDetails,
    Visit,
)
from murfey.util.state import global_state

log = logging.getLogger("murfey.server.api")

machine_config = get_machine_config()

router = APIRouter()


def sanitise(in_string: str) -> str:
    return in_string.replace("\r\n", "").replace("\n", "")


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
    prom.seen_files.labels(rsync_source=rsyncer_info.source)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source)
    prom.transferred_files_bytes.labels(rsync_source=rsyncer_info.source)
    prom.transferred_data_files.labels(rsync_source=rsyncer_info.source)
    prom.transferred_data_files_bytes.labels(rsync_source=rsyncer_info.source)
    prom.seen_files.labels(rsync_source=rsyncer_info.source).set(0)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source).set(0)
    prom.transferred_files_bytes.labels(rsync_source=rsyncer_info.source).set(0)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source).set(0)
    prom.transferred_data_files.labels(rsync_source=rsyncer_info.source).set(0)
    prom.transferred_data_files_bytes.labels(rsync_source=rsyncer_info.source).set(0)
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
    prom.seen_files.labels(rsync_source=rsyncer_info.source).inc(
        rsyncer_info.increment_count
    )
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source).inc(
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
    prom.transferred_files.labels(rsync_source=rsyncer_info.source).inc(
        rsyncer_info.increment_count
    )
    prom.transferred_files_bytes.labels(rsync_source=rsyncer_info.source).inc(
        rsyncer_info.bytes
    )
    prom.transferred_data_files.labels(rsync_source=rsyncer_info.source).inc(
        rsyncer_info.increment_data_count
    )
    prom.transferred_data_files_bytes.labels(rsync_source=rsyncer_info.source).inc(
        rsyncer_info.data_bytes
    )


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
    if proc_params:

        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], db, number=2, close=False)

        if feedback_params.picker_murfey_id is None:
            feedback_params.picker_murfey_id = murfey_ids[1]
            db.add(feedback_params)
        movie = Movie(murfey_id=murfey_ids[0], path=proc_file.path)
        db.add(movie)
        db.commit()
        db.close()

        if not mrc_out.parent.exists():
            Path(secure_filename(str(mrc_out))).parent.mkdir(
                parents=True, exist_ok=True
            )
        if not Path(proc_file.path).is_file():
            return proc_file
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config.feedback_queue,
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
                "downscale": proc_params["downscale"],
                "picker_uuid": murfey_ids[1],
                "session_id": session_id,
                "particle_diameter": proc_params["particle_diameter"] or 0,
                "fm_int_file": proc_file.eer_fractionation_file,
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
            client_id=client_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            eer_fractionation_file=str(proc_file.eer_fractionation_file),
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
        .where(DataCollectionGroup.tag == proc_file.tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == "em-tomo-preprocess")
    ).all()
    if data_collection:
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
                "gain_ref": str(machine_config.rsync_basepath / proc_file.gain_ref)
                if proc_file.gain_ref
                else proc_file.gain_ref,
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
            client_id=client_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            tag=proc_file.tag,
        )
        db.add(for_stash)
        db.commit()
        db.close()
    # await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
    return proc_file


@router.post("/visits/{visit_name}/align")
async def request_tilt_series_alignment(tilt_series: TiltSeriesProcessingDetails):
    stack_file = (
        Path(tilt_series.motion_corrected_path).parents[1]
        / "align_output"
        / f"{tilt_series.name}_stack.mrc"
    )
    if not stack_file.parent.exists():
        stack_file.parent.mkdir(parents=True)
    zocalo_message = {
        "recipes": ["em-tomo-align"],
        "parameters": {
            "input_file_list": tilt_series.file_tilt_list,
            "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
            "dcid": tilt_series.dcid,
            "appid": tilt_series.autoproc_program_id,
            "stack_file": str(stack_file),
            "pix_size": tilt_series.pixel_size,
            "manual_tilt_offset": tilt_series.manual_tilt_offset,
        },
    }
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message)
    else:
        log.error(
            f"Processing was requested for tilt series {sanitise(tilt_series.name)} but no Zocalo transport object was found"
        )
        return tilt_series
    await ws.manager.broadcast(
        f"Processing requested for tilt series {tilt_series.name}"
    )

    return tilt_series


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
        check_path.mkdir()
    return {"suggested_path": check_path.relative_to(machine_config.rsync_basepath)}


@router.post("/visits/{visit_name}/{client_id}/register_data_collection_group")
def register_dc_group(visit_name, client_id: int, dcg_params: DCGroupParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    microscope = get_microscope(machine_config=machine_config)
    log.info(f"Registering data collection group on microscope {microscope}")
    dcg_parameters = {
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=microscope,
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
        "start_time": str(datetime.datetime.now()),
        "experiment_type": dcg_params.experiment_type,
        "experiment_type_id": dcg_params.experiment_type_id,
        "tag": dcg_params.tag,
        "client_id": client_id,
    }

    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue, {"register": "data_collection_group", **dcg_parameters}  # type: ignore
        )
    return dcg_parameters


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
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=get_microscope(machine_config=machine_config),
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
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
            {"register": "data_collection", **dc_parameters},
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
            f"{num_eer_frames} {fractionation_params.fractionation} {fractionation_params.dose_per_frame}"
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
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    ).all()
    for ri in rsync_instances:
        prom.seen_files.remove(ri.source)
        prom.transferred_files.remove(ri.source)
        prom.transferred_files_bytes.remove(ri.source)
        prom.seen_data_files.remove(ri.source)
        prom.transferred_data_files.remove(ri.source)
        prom.transferred_data_files_bytes.remove(ri.source)
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
