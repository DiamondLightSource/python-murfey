from __future__ import annotations

import datetime
import os
import socket

import ispyb
import sqlalchemy.exc
import sqlalchemy.orm
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ispyb.sqlalchemy import BLSession, Proposal
from pydantic import BaseModel
from requests import get

try:
    from importlib.resources import files
except ModuleNotFoundError:
    # Fallback for Python 3.8
    from importlib_resources import files  # type: ignore

app = FastAPI(title="Murfey server", debug=True)

template_files = files("murfey") / "templates"
templates = Jinja2Templates(directory=template_files)
app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

db_session = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)()

# This will be the homepage for a given microscope.
@app.get("/")
async def root(request: Request, response_class=HTMLResponse):
    client_host = request.client.host
    microscope = get_microscope()
    return templates.TemplateResponse(
        "home.html",
        {"request": request, "client_host": client_host, "microscope": microscope},
    )


class Visits(BaseModel):
    start: str
    end: str
    beamline_name: str
    visit_name: str
    proposal_title: str


@app.get("/visits/")
def all_visit_info(request: Request):
    bl_name = get_microscope()
    query = (
        db_session.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == bl_name,
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
                "Visit name": id.proposalCode
                + str(id.proposalNumber)
                + "-"
                + str(id.visit_number),
                "Time remaining": str(id.endDate - datetime.datetime.now()),
            }
            for id in query
        ]  # "Proposal title": id.title
        return templates.TemplateResponse(
            "activevisits.html",
            {"request": request, "info": return_query},
        )
    else:
        return None


@app.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str):
    bl_name = get_microscope()
    query = (
        db_session.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == bl_name,
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


@app.get("/pypi/{path}")
async def pypi_path_request(path: str):
    full_path = "https://pypi.org/simple/" + path
    full_path_response = get(full_path)
    return Response(
        content=full_path_response.content,
        media_type=full_path_response.headers["Content-Type"],
        status_code=full_path_response.status_code,
    )


@app.get("/pypi/")
async def pypi_request():
    full_path = "https://pypi.org/simple/"
    full_path_response = get(full_path)
    return Response(
        content=full_path_response.content,
        media_type=full_path_response.headers["Content-Type"],
        status_code=200,
    )


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float


@app.post("/visits/{bl_name}/{visit_name}/files")
async def add_file(bl_name: str, visit_name: str, file: File):
    return file


# @app.get("/microscope")
def get_microscope():
    try:
        hostname = socket.gethostname()
        microscope_from_hostname = hostname.split(".")[0]
    except OSError:
        microscope_from_hostname = "Unknown"
    microscope_name = os.getenv("BEAMLINE", microscope_from_hostname)
    return microscope_name
