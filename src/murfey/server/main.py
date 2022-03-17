from __future__ import annotations

import datetime
import logging

import ispyb
import packaging.version
import sqlalchemy.orm
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from ispyb.sqlalchemy import BLSession, Proposal
from pydantic import BaseModel

import murfey.server
import murfey.server.bootstrap
import murfey.server.websocket as ws
from murfey.server import get_hostname, get_microscope, template_files, templates

log = logging.getLogger("murfey.server.main")

tags_metadata = [murfey.server.bootstrap.tag]


app = FastAPI(title="Murfey server", debug=True, openapi_tags=tags_metadata)
app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

app.include_router(murfey.server.bootstrap.bootstrap)
app.include_router(murfey.server.bootstrap.cygwin)
app.include_router(murfey.server.bootstrap.pypi)
app.include_router(murfey.server.websocket.ws)

SessionLocal = sqlalchemy.orm.sessionmaker(
    bind=sqlalchemy.create_engine(
        ispyb.sqlalchemy.url(), connect_args={"use_pure": True}
    )
)


def get_db() -> sqlalchemy.orm.Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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


class Visits(BaseModel):
    start: str
    end: str
    beamline_name: str
    visit_name: str
    proposal_title: str


@app.get("/visits/")
def all_visit_info(request: Request, db: sqlalchemy.orm.Session = Depends(get_db)):
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
                "Visit name": id.proposalCode
                + str(id.proposalNumber)
                + "-"
                + str(id.visit_number),
                "Time remaining": str(id.endDate - datetime.datetime.now()),
            }
            for id in query
        ]  # "Proposal title": id.title
        log.debug(
            f"{len(return_query)} visits active for {microscope=}: {', '.join(r['Visit name'] for r in return_query)}"
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


@app.get("/visits/{visit_name}")
def visit_info(
    request: Request, visit_name: str, db: sqlalchemy.orm.Session = Depends(get_db)
):
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
