from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import List

import packaging.version
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from ispyb.sqlalchemy import BLSession
from pydantic import BaseSettings

import murfey.server.bootstrap
import murfey.server.websocket as ws
from murfey.server import feedback_callback_async, get_hostname, get_microscope
from murfey.server import shutdown as _shutdown
from murfey.server import templates
from murfey.server.config import from_file
from murfey.util.models import (
    ConnectionFileParameters,
    ContextInfo,
    DCGroupParameters,
    DCParameters,
    File,
    ProcessFile,
    ProcessingJobParameters,
    RegistrationMessage,
    SPAProcessingParameters,
    SuggestedPathParameters,
    TiltSeries,
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
    return {"microscope": microscope}


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
async def request_tilt_series_alignment(tilt_series: TiltSeries):
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


@router.post("/visits/{visit_name}/register_data_collection_group")
def register_dc_group(visit_name, dcg_params: DCGroupParameters):
    log.info(f"Registering data collection group on microscope {get_microscope()}")
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
    log.info(f"Starting data collection on microscope {get_microscope()}")
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
