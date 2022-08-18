from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import packaging.version
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from ispyb.sqlalchemy import BLSession, Proposal
from pydantic import BaseModel, BaseSettings

import murfey.server
import murfey.server.bootstrap
import murfey.server.ispyb
import murfey.server.websocket as ws
import murfey.util.models
from murfey.server import (
    _transport_object,
    get_hostname,
    get_microscope,
    template_files,
    templates,
)
from murfey.server.config import from_file

log = logging.getLogger("murfey.server.main")

tags_metadata = [murfey.server.bootstrap.tag]


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()

app = FastAPI(title="Murfey server", debug=True, openapi_tags=tags_metadata)
app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

app.include_router(murfey.server.bootstrap.bootstrap)
app.include_router(murfey.server.bootstrap.cygwin)
app.include_router(murfey.server.bootstrap.pypi)
app.include_router(murfey.server.websocket.ws)


# This will be the homepage for a given microscope.
@app.get("/", response_class=HTMLResponse)
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
@app.get("/machine/")
def machine_info():
    if settings.murfey_machine_configuration:
        return from_file(settings.murfey_machine_configuration)
    return {}


@app.get("/microscope/")
def get_mic():
    microscope = get_microscope()
    return {"microscope": microscope}


@app.get("/visits/")
def all_visit_info(request: Request, db=murfey.server.ispyb.DB):
    microscope = get_microscope()
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


@app.get("/demo/visits_raw", response_model=List[murfey.util.models.Visit])
def get_current_visits_demo(db=murfey.server.ispyb.DB):
    microscope = "m12"
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@app.get("/visits_raw", response_model=List[murfey.util.models.Visit])
def get_current_visits(db=murfey.server.ispyb.DB):
    microscope = get_microscope()
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@app.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str, db=murfey.server.ispyb.DB):
    microscope = get_microscope()
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


class ContextInfo(BaseModel):
    experiment_type: str
    acquisition_software: str


@app.post("/visits/{visit_name}/context")
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


@app.post("/visits/{visit_name}/files")
async def add_file(file: File):
    message = f"File {file} transferred"
    log.info(message)
    await ws.manager.broadcast(f"File {file} transferred")
    return file


class RegistrationMessage(BaseModel):
    registration: str
    params: Optional[Dict[str, Any]] = None


@app.post("/feedback")
async def send_murfey_message(msg: RegistrationMessage):
    if _transport_object:
        _transport_object.transport.send(
            "murfey_feedback", {"register": msg.registration}
        )


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


@app.post("/visits/{visit_name}/tomography_preprocess")
async def request_tomography_preprocessing(visit_name: str, proc_file: ProcessFile):
    path_parts = Path(proc_file.path).parts
    visit_idx = path_parts.index(visit_name)
    base_path = "/".join(path_parts[: visit_idx + 1])
    ppath = Path(proc_file.path)
    mrc_out = (
        Path(proc_file.base_path)
        / "processed"
        / ppath.relative_to(base_path).parts[0]
        / "MotionCorr"
        / ppath.with_suffix("_motion_corrected.mrc").name
    )
    ctf_out = (
        Path(proc_file.base_path)
        / "processed"
        / ppath.relative_to(base_path).parts[0]
        / "CTF"
        / ppath.with_suffix("_ctf.mrc").name
    )
    if not mrc_out.parent.exists():
        mrc_out.parent.mkdir(parents=True)
    if not ctf_out.parent.exists():
        ctf_out.parent.mkdir(parents=True)
    zocalo_message = {
        "recipes": ["em_tomo_preprocess"],
        "parameters": {
            "dcid": proc_file.data_collection_id,
            "autoproc_program_id": proc_file.autoproc_program_id,
            "movie": proc_file.path,
            "mrc_out": mrc_out,
            "pix_size": proc_file.pixel_size,
            "output_image": ctf_out,
            "image_number": proc_file.image_number,
            "microscope": get_microscope(),
            "mc_uuid": proc_file.mc_uuid,
            "movie_uuid": proc_file.movie_uuid,
        },
    }
    log.info(f"Sending Zocalo message {zocalo_message}")
    if _transport_object:
        _transport_object.transport.send("processing_recipe", zocalo_message)
    else:
        log.error(
            f"Processing was requested for {proc_file.name} but no Zocalo transport object was found"
        )
        return proc_file
    await ws.manager.broadcast(f"Processing requested for {proc_file.name}")
    return proc_file


class TiltSeries(BaseModel):
    name: str
    tilts: List[str]
    processing_job: int


@app.post("/visits/{visit_name}/align")
async def request_tilt_series_alignment(tilt_series: TiltSeries):
    zocalo_message = {
        "recipes": ["em_align"],
        "parameters": {
            "ispyb_process": tilt_series.processing_job,
            "tilts": tilt_series.tilts,
        },
    }
    log.info(f"Sending Zocalo message {zocalo_message}")
    if _transport_object:
        _transport_object.transport.send("processing_recipe", zocalo_message)
    else:
        log.error(
            f"Processing was requested for tilt series {tilt_series.name} but no Zocalo transport object was found"
        )
        return tilt_series
    await ws.manager.broadcast(
        f"Processing requested for tilt series {tilt_series.name}"
    )
    return tilt_series


@app.get("/version")
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


@app.get("/shutdown", include_in_schema=False)
def shutdown():
    """A method to stop the server. This should be removed before Murfey is
    deployed in production. To remove it we need to figure out how to control
    to process (eg. systemd) and who to run it as."""
    log.info("Server shutdown request received")
    murfey.server.shutdown()
    return {"success": True}


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


@app.post("/visits/{visit_name}/register_data_collection_group")
def register_dc_group(visit_name, dcg_params: DCGroupParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    log.info(f"Registering data collection group on microscope {get_microscope()}")
    dcg_parameters = {
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=get_microscope(),
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
        "start_time": str(datetime.datetime.now()),
        "experiment_type": dcg_params.experiment_type,
    }

    if _transport_object:
        _transport_object.transport.send(
            "murfey_feedback", {"register": "data_collection_group", **dcg_parameters}  # type: ignore
        )
    return dcg_params


@app.post("/visits/{visit_name}/start_data_collection")
def start_dc(visit_name, dc_params: DCParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    log.info(f"Starting data collection on microscope {get_microscope()}")
    dc_parameters = {
        "visit": visit_name,
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=get_microscope(),
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
        "image_directory": dc_params.image_directory,
        "start_time": str(datetime.datetime.now()),
        "voltage": dc_params.voltage,
        "pixel_size": dc_params.pixel_size_on_image,
        "image_suffix": dc_params.file_extension,
        "experiment_type": dc_params.experiment_type,
        "n_images": dc_params.tilt,
        "image_size_x": dc_params.image_size_x,
        "image_size_y": dc_params.image_size_y,
        "acquisition_software": dc_params.acquisition_software,
        "tag": dc_params.tag,
    }

    if _transport_object:
        log.debug(f"Send registration message to murfey_feedback: {dc_parameters}")
        _transport_object.transport.send(
            "murfey_feedback", {"register": "data_collection", **dc_parameters}
        )
    return dc_params


@app.post("/visits/{visit_name}/register_processing_job")
def register_proc(visit_name, proc_params: ProcessingJobParameters):
    proc_parameters = {
        "recipe": proc_params.recipe,
        "tag": proc_params.tag,
    }

    if _transport_object:
        log.debug(
            f"Send processing registration message to murfey_feedback: {proc_parameters}"
        )
        _transport_object.transport.send(
            "murfey_feedback", {"register": "processing_job", **proc_parameters}
        )
    return proc_params
