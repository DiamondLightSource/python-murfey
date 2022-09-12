from __future__ import annotations

import asyncio
import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import packaging.version
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from ispyb.sqlalchemy import BLSession
from pydantic import BaseModel, BaseSettings

import murfey.server
import murfey.server.bootstrap
import murfey.server.ispyb
import murfey.server.websocket as ws
import murfey.util.models
from murfey.server import get_hostname, get_microscope, templates
from murfey.server.config import from_file
from murfey.util.state import global_state

log = logging.getLogger("murfey.server.demo_api")

tags_metadata = [murfey.server.bootstrap.tag]

demo_router = APIRouter()


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()


# This will be the homepage for a given microscope.
@demo_router.get("/", response_class=HTMLResponse)
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
@demo_router.get("/machine/")
def machine_info():
    if settings.murfey_machine_configuration:
        microscope = get_microscope()
        return from_file(settings.murfey_machine_configuration, microscope)
    return {}


@demo_router.get("/microscope/")
def get_mic():
    microscope = get_microscope()
    return {"microscope": microscope}


@demo_router.get("/visits/")
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


@demo_router.get("/visits_raw", response_model=List[murfey.util.models.Visit])
def get_current_visits():
    return []


@demo_router.get("/visits/{visit_name}")
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


class ContextInfo(BaseModel):
    experiment_type: str
    acquisition_software: str


@demo_router.post("/visits/{visit_name}/context")
async def register_context(context_info: ContextInfo):
    log.info(
        f"Context {context_info.experiment_type}:{context_info.acquisition_software} registered"
    )
    await ws.manager.broadcast(f"Context registered: {context_info}")
    await ws.manager.set_state("experiment_type", context_info.experiment_type)
    await ws.manager.set_state(
        "acquisition_software", context_info.acquisition_software
    )


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float


@demo_router.post("/visits/{visit_name}/files")
async def add_file(file: File):
    message = f"File {file} transferred"
    log.info(message)
    await ws.manager.broadcast(f"File {file} transferred")
    return file


class RegistrationMessage(BaseModel):
    registration: str
    params: Optional[Dict[str, Any]] = None


@demo_router.post("/feedback")
async def send_murfey_message(msg: RegistrationMessage):
    pass


class ProcessFile(BaseModel):
    path: str
    description: str
    size: int
    timestamp: float
    processing_job: int
    data_collection_id: int
    image_number: int
    mc_uuid: int
    movie_uuid: int
    autoproc_program_id: int
    pixel_size: float


@demo_router.post("/visits/{visit_name}/tomography_preprocess")
async def request_tomography_preprocessing(visit_name: str, proc_file: ProcessFile):
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
        mrc_out.parent.mkdir(parents=True, mode=1411)
    asyncio.sleep(10)
    murfey.server.feedback_callback(
        {},
        {
            "register": "motion_corrected",
            "movie": str(proc_file.path),
            "mrc_out": str(mrc_out),
        },
    )
    await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
    return proc_file


class TiltSeries(BaseModel):
    name: str
    file_tilt_list: str
    dcid: int
    processing_job: int
    autoproc_program_id: int
    motion_corrected_path: str
    movie_id: int


@demo_router.post("/visits/{visit_name}/align")
async def request_tilt_series_alignment(tilt_series: TiltSeries):
    stack_file = (
        Path(tilt_series.motion_corrected_path).parents[1]
        / "align_output"
        / "aligned_file.mrc"
    )
    if not stack_file.parent.exists():
        stack_file.parent.mkdir(parents=True, mode=1411)
    await ws.manager.broadcast(
        f"Processing requested for tilt series {tilt_series.name}"
    )
    return tilt_series


@demo_router.get("/version")
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


@demo_router.get("/shutdown", include_in_schema=False)
def shutdown():
    """A method to stop the server. This should be removed before Murfey is
    deployed in production. To remove it we need to figure out how to control
    to process (eg. systemd) and who to run it as."""
    log.info("Server shutdown request received")
    murfey.server.shutdown()
    return {"success": True}


class SuggestedPathParameters(BaseModel):
    base_path: Path


@demo_router.post("/visits/{visit_name}/suggested_path")
def suggest_path(visit_name, params: SuggestedPathParameters):
    count: int | None = None
    check_path = Path(f"/dls/{get_microscope()}") / params.base_path
    check_path_name = check_path.name
    while check_path.exists():
        count = count + 1 if count else 2
        check_path = check_path.parent / f"{check_path_name}{count}"
    return {"suggested_path": check_path}


class DCGroupParameters(BaseModel):
    experiment_type: str


class DCParameters(BaseModel):
    voltage: float
    pixel_size_on_image: str
    experiment_type: str
    image_size_x: int
    image_size_y: int
    tilt: int
    file_extension: str
    acquisition_software: str
    image_directory: str
    tag: str


class ProcessingJobParameters(BaseModel):
    tag: str
    recipe: str


@demo_router.post("/visits/{visit_name}/register_data_collection_group")
def register_dc_group(visit_name, dcg_params: DCGroupParameters):
    log.info(f"Registering data collection group on microscope {get_microscope()}")
    global_state["data_collection_group_id"] = 1
    return dcg_params


@demo_router.post("/visits/{visit_name}/start_data_collection")
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


@demo_router.post("/visits/{visit_name}/register_processing_job")
def register_proc(visit_name, proc_params: ProcessingJobParameters):
    if global_state.get("processing_job_ids"):
        assert isinstance(global_state["processing_job_ids"], dict)
        global_state["processing_job_ids"] = {
            **global_state["processing_job_ids"],
            proc_params.tag: 1,
        }
    else:
        global_state["processing_job_ids"] = {proc_params.tag: 1}
    if global_state.get("autoproc_program_ids"):
        assert isinstance(global_state["autoproc_program_ids"], dict)
        global_state["autoproc_program_ids"] = {
            **global_state["autoproc_program_ids"],
            proc_params.tag: 1,
        }
    else:
        global_state["autoproc_program_ids"] = {proc_params.tag: 1}
    return proc_params
