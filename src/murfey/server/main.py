from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from typing import List

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


class ProcessFile(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float
    processing_job: int


@app.post("/visits/{visit_name}/common_preprocess")
async def request_common_preprocessing(proc_file: ProcessFile):
    zocalo_message = {
        "recipes": ["em_common_preprocess"],
        "parameters": {
            "ispyb_process": proc_file.processing_job,
            "movie": proc_file.name,
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
