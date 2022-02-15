from __future__ import annotations

import datetime
import functools
import os
import re
import socket

import ispyb
import packaging.version
import sqlalchemy.exc
import sqlalchemy.orm
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ispyb.sqlalchemy import BLSession, Proposal
from pydantic import BaseModel
from requests import get

import murfey

try:
    from importlib.resources import files
except ImportError:
    # Fallback for Python 3.8
    from importlib_resources import files  # type: ignore

app = FastAPI(title="Murfey server", debug=True)

template_files = files("murfey") / "templates"
templates = Jinja2Templates(directory=template_files)
app.mount("/static", StaticFiles(directory=template_files / "static"), name="static")
app.mount("/images", StaticFiles(directory=template_files / "images"), name="images")

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
@app.get("/")
async def root(request: Request, response_class=HTMLResponse):
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "hostname": get_hostname(),
            "microscope": get_microscope(),
            "version": murfey.__version__,
        },
    )


@app.get("/bootstrap")
def bootstrap(request: Request, response_class=HTMLResponse):
    return templates.TemplateResponse(
        "bootstrap.html",
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
    bl_name = get_microscope()
    query = (
        db.query(BLSession)
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
def visit_info(
    request: Request, visit_name: str, db: sqlalchemy.orm.Session = Depends(get_db)
):
    bl_name = get_microscope()
    query = (
        db.query(BLSession)
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


@app.get("/pypi/")
def pypi_request():
    """Obtain list of all PyPI packages via the simple API (PEP 503)."""
    index = get("https://pypi.org/simple/")
    return Response(
        content=index.content,
        media_type=index.headers["Content-Type"],
        status_code=index.status_code,
    )


@app.get("/pypi/{package}/")
def pypi_package_request(package: str):
    """Obtain list of all package downloads from PyPI via the simple API (PEP 503),
    and rewrite all download URLs to point to this server,
    underneath the current directory."""
    full_path_response = get(f"https://pypi.org/simple/{package}")

    def rewrite_pypi_url(match):
        url = match.group(4)
        return (
            b"<a "
            + match.group(1)
            + b'href="'
            + url
            + b'"'
            + match.group(3)
            + b">"
            + match.group(4)
            + b"</a>"
        )

    content = re.sub(
        b'<a ([^>]*)href="([^">]*)"([^>]*)>([^<]*)</a>',
        rewrite_pypi_url,
        full_path_response.content,
    )
    return Response(
        content=content,
        media_type=full_path_response.headers["Content-Type"],
        status_code=full_path_response.status_code,
    )


@app.get("/pypi/{package}/{filename}")
def pypi_download_request(package: str, filename: str):
    """Obtain and pass through a specific download for a PyPI package."""
    full_path_response = get(f"https://pypi.org/simple/{package}")
    filename_bytes = re.escape(filename.encode("latin1"))

    selected_package_link = re.search(
        b'<a [^>]*?href="([^">]*)"[^>]*>' + filename_bytes + b"</a>",
        full_path_response.content,
    )
    if not selected_package_link:
        raise HTTPException(status_code=404, detail="File not found for package")
    original_url = selected_package_link.group(1)
    original_file = get(original_url)
    return Response(
        content=original_file.content,
        media_type=original_file.headers["Content-Type"],
        status_code=original_file.status_code,
    )


@app.get("/bootstrap/pip.whl")
def pypi_download_pip():
    # Return a static version of pip. This does not need to be the newest or best,
    # but has to be compatible with all supported Python versions.
    # This is only used during bootstrapping by the client to identify and then
    # download the actually newest appropriate version of pip.
    return pypi_download_request(package="pip", filename="pip-21.3.1-py3-none-any.whl")


@app.get("/bootstrap/murfey.whl")
def pypi_download_murfey():
    # Return the latest version of murfey. We should not have to worry about the exact
    # python compatibility here, as long as murfey.bootstrap is compatible with all
    # relevant versions of Python. This also ignores yanked releases, which again should
    # be fine.
    full_path_response = get("https://pypi.org/simple/murfey")
    wheels = {}
    for wheel_file in re.findall(
        b"<a [^>]*>([^<]*).whl</a>",
        full_path_response.content,
    ):
        try:
            filename = wheel_file.decode("latin-1") + ".whl"
            version = packaging.version.parse(filename.split("-")[1])
            wheels[version] = filename
        except Exception:
            pass
    if not wheels:
        raise HTTPException(
            status_code=404, detail="Could not identify appropriate version of murfey"
        )
    newest_version = max(wheels)
    return pypi_download_request(package="murfey", filename=wheels[newest_version])


class File(BaseModel):
    name: str
    description: str
    size: int
    timestamp: float


@app.post("/visits/{bl_name}/{visit_name}/files")
async def add_file(bl_name: str, visit_name: str, file: File):
    return file


@functools.lru_cache()
def get_microscope():
    try:
        hostname = get_hostname()
        microscope_from_hostname = hostname.split(".")[0]
    except OSError:
        microscope_from_hostname = "Unknown"
    microscope_name = os.getenv("BEAMLINE", microscope_from_hostname)
    return microscope_name


@functools.lru_cache()
def get_hostname():
    return socket.gethostname()


@app.get("/version")
def get_version():
    return {
        "server": murfey.__version__,
        "oldest-supported-client": murfey.__supported_client_version__,
    }
