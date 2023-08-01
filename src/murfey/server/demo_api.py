from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import List

import packaging.version
import sqlalchemy
from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, HTMLResponse
from ispyb.sqlalchemy import BLSession
from pydantic import BaseSettings
from sqlmodel import select

import murfey.server.bootstrap
import murfey.server.websocket as ws
from murfey.server import feedback_callback_async, get_hostname, get_microscope
from murfey.server import shutdown as _shutdown
from murfey.server import templates
from murfey.server.config import from_file
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    PreprocessStash,
    ProcessingJob,
    RsyncInstance,
    Session,
    SPARelionParameters,
    TiltSeries,
)
from murfey.util.models import (
    ClientInfo,
    ConnectionFileParameters,
    ContextInfo,
    DCGroupParameters,
    DCParameters,
    File,
    GainReference,
    ProcessFile,
    ProcessingJobParameters,
    ProcessingParametersSPA,
    RegistrationMessage,
    RsyncerInfo,
    SessionInfo,
    SPAProcessFile,
    SPAProcessingParameters,
    SuggestedPathParameters,
    TiltSeriesInfo,
    TiltSeriesProcessingDetails,
    Visit,
)
from murfey.util.state import global_state

log = logging.getLogger("murfey.server.demo_api")

tags_metadata = [murfey.server.bootstrap.tag]

router = APIRouter()


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
        "home.html",
        {
            "request": request,
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
        "activevisits.html",
        {"request": request, "info": return_query, "microscope": microscope},
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
    db.close()


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
    db.close()


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
        params = SPARelionParameters(
            session_id=session_id,
            angpix=proc_params.pixel_size_on_image,
            dose_per_frame=proc_params.dose_per_frame,
            gain_ref=proc_params.gain_ref,
            voltage=proc_params.voltage,
            motion_corr_binning=proc_params.motion_corr_binning,
            eer_grouping=proc_params.eer_grouping,
            symmetry=proc_params.symmetry,
            particle_diameter=proc_params.particle_diameter,
            downscale=proc_params.downscale,
            boxsize=proc_params.boxsize,
            small_boxsize=proc_params.small_boxsize,
            mask_diameter=proc_params.mask_diameter,
        )
    except Exception as e:
        log.warning(f"registration failed: {e}")
    db.add(params)
    db.commit()
    db.close()


@router.get("/clients/{client_id}/spa_processing_parameters")
def get_spa_proc_params(client_id: int, db=murfey_db) -> List[dict]:
    params = db.exec(
        select(SPARelionParameters).where(SPARelionParameters.client_id == client_id)
    ).all()
    return [p.json() for p in params]


@router.post("/visits/{visit_name}/tilt_series")
def register_tilt_series(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    tilt_series = TiltSeries(client_id=TiltSeriesInfo.client_id, tag=TiltSeriesInfo.tag)
    db.add(tilt_series)
    db.commit()
    db.close()


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
        "visit.html",
        {"request": request, "visit": return_query},
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


@router.post("/visits/{visit_name}/{client_id}/flush_spa_processing")
def flush_spa_processing(visit_name: str, client_id: int, db=murfey_db):
    session_id = (
        db.exec(
            select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
        )
        .one()
        .session_id
    )
    stashed_files = db.exec(
        select(PreprocessStash).where(PreprocessStash.client_id == client_id)
    ).all()
    proc_params = db.exec(
        select(SPARelionParameters).where(SPARelionParameters.session_id == session_id)
    ).one()
    if not proc_params:
        log.warning(
            f"No SPA processing parameters found for client {client_id} on visit {visit_name}"
        )
        return
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(
            DataCollectionGroup.session_id == session_id
            and DataCollectionGroup.tag == "spa"
        )
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == "em-spa-preprocess")
    ).one()
    for f in stashed_files:
        if not f.mrc_out.parent.exists():
            f.mrc_out.parent.mkdir(parents=True)
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config["feedback_queue"],
                "dcid": collected_ids[1].id,
                "autoproc_program_id": collected_ids[3].id,
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pix_size": proc_params.angpix,
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": f.mc_uuid,
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "gain_ref": str(machine_config["rsync_basepath"] / proc_params.gain_ref)
                if proc_params.gain_ref
                else proc_params.gain_ref,
                "downscale": proc_params.downscale,
            },
        }
        log.info(f"Launching SPA preprocessing with Zoaclo message: {zocalo_message}")
    return


@router.post("/visits/{visit_name}/{client_id}/spa_preprocess")
async def request_spa_preprocessing(
    visit_name: str, client_id: int, proc_file: SPAProcessFile, db=murfey_db
):
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path(proc_file.path)
    sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
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
        proc_params = db.exec(
            select(SPARelionParameters, ClientEnvironment)
            .where(SPARelionParameters.session_id == ClientEnvironment.session_id)
            .where(ClientEnvironment.client_id == client_id)
        ).one()[0]
    except sqlalchemy.exc.NoResultFound:
        proc_params = None
    if proc_params:

        collected_ids = db.exec(
            select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
            .where(
                DataCollectionGroup.client_id == client_id
                and DataCollectionGroup.tag == "spa"
            )
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()

        if not mrc_out.parent.exists():
            mrc_out.parent.mkdir(parents=True)
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config["feedback_queue"],
                "dcid": collected_ids[1].id,
                "autoproc_program_id": collected_ids[3].id,
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pix_size": proc_params.angpix,
                "image_number": proc_file.image_number,
                "microscope": get_microscope(),
                "mc_uuid": proc_file.mc_uuid,
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "gain_ref": str(machine_config["rsync_basepath"] / proc_params.gain_ref)
                if proc_params.gain_ref
                else proc_params.gain_ref,
                "downscale": proc_params.downscale,
            },
        }
        log.info(f"Sending Zocalo message {zocalo_message}")

    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            client_id=client_id,
            image_number=proc_file.image_number,
            mc_uuid=proc_file.mc_uuid,
            mrc_out=str(mrc_out),
        )
        db.add(for_stash)
        db.commit()
        db.close()

    return proc_file


@router.post("/visits/{visit_name}/tomography_preprocess")
async def request_tomography_preprocessing(visit_name: str, proc_file: ProcessFile):
    if not Path(proc_file.path).exists():
        log.warning(f"{proc_file.path} has not been transferred before preprocessing")
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path(proc_file.path)
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
    if not mrc_out.parent.exists():
        mrc_out.parent.mkdir(parents=True)
    await feedback_callback_async(
        {},
        {
            "register": "motion_corrected",
            "movie": str(proc_file.path),
            "mrc_out": str(mrc_out),
            "movie_id": proc_file.mc_uuid,
        },
    )
    await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
    mrc_out.touch()
    return proc_file


@router.post("/visits/{visit_name}/align")
async def request_tilt_series_alignment(tilt_series: TiltSeriesProcessingDetails):
    stack_file = (
        Path(tilt_series.motion_corrected_path).parents[1]
        / "align_output"
        / f"aligned_file_{tilt_series.name}.mrc"
    )
    if not stack_file.parent.exists():
        stack_file.parent.mkdir(parents=True)
    await ws.manager.broadcast(
        f"Processing requested for tilt series {tilt_series.name}"
    )
    stack_file.touch()
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
    count: int | None = None
    check_path = (
        machine_config["rsync_basepath"] / params.base_path
        if machine_config
        else Path(f"/dls/{get_microscope()}") / params.base_path
    )
    check_path_name = check_path.name
    while check_path.exists():
        count = count + 1 if count else 2
        check_path = check_path.parent / f"{check_path_name}{count}"
    return {"suggested_path": check_path.relative_to(machine_config["rsync_basepath"])}


@router.post("/visits/{visit_name}/{client_id}/register_data_collection_group")
def register_dc_group(
    visit_name: str, client_id: int, dcg_params: DCGroupParameters, db=murfey_db
):
    log.info(f"Registering data collection group on microscope {get_microscope()}")
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    murfey_dcg = DataCollectionGroup(
        id=1,
        session_id=client.session_id,
        tag=dcg_params.tag,
    )
    db.add(murfey_dcg)
    db.commit()
    db.close()
    if global_state.get("data_collection_group_ids") and isinstance(
        global_state["data_collection_group_ids"], dict
    ):
        global_state["data_collection_group_ids"] = {
            **global_state["data_collection_group_ids"],
            dcg_params.tag: 1,
        }
    else:
        global_state["data_collection_group_ids"] = {dcg_params.tag: 1}
    return dcg_params


@router.post("/visits/{visit_name}/start_data_collection")
def start_dc(visit_name, dc_params: DCParameters):
    if global_state.get("data_collection_ids") and isinstance(
        global_state["data_collection_ids"], dict
    ):
        global_state["data_collection_ids"] = {
            **global_state["data_collection_ids"],
            dc_params.tag: 1,
        }
    else:
        global_state["data_collection_ids"] = {dc_params.tag: 1}
    return dc_params


@router.post("/visits/{visit_name}/register_processing_job")
def register_proc(visit_name, proc_params: ProcessingJobParameters):
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


@router.delete("/clients/{client_id}/session")
def remove_session(client_id: int, db=murfey_db):
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    session_id = client.session_id
    client.session_id = None
    db.add(client)
    db.commit()
    assert session_id is not None
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    db.close()
    return
