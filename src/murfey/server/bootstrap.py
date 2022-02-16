"""
API endpoints related to installing Murfey on a client.

Client machines may not have a direct internet connection, so Murfey allows
passing through requests to PyPI using the PEP 503 simple API.

A static HTML page gives instructions on how to install on a network-isolated
system that has Python already installed. The system does not need to have
pip installed in order to bootstrap Murfey.
"""

from __future__ import annotations

import re

import packaging.version
import requests
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse

from murfey.server import respond_with_template

tag = {
    "name": "bootstrap",
    "description": __doc__,
    "externalDocs": {
        "description": "PEP 503",
        "url": "https://www.python.org/dev/peps/pep-0503/",
    },
}
bootstrap = APIRouter(prefix="/bootstrap", tags=["bootstrap"])
pypi = APIRouter(prefix="/pypi", tags=["bootstrap"])


@pypi.get("/", response_class=Response)
def get_pypi_index():
    """Obtain list of all PyPI packages via the simple API (PEP 503)."""
    index = requests.get("https://pypi.org/simple/")
    return Response(
        content=index.content,
        media_type=index.headers["Content-Type"],
        status_code=index.status_code,
    )


@pypi.get("/{package}/", response_class=Response)
def get_pypi_package_downloads_list(package: str):
    """Obtain list of all package downloads from PyPI via the simple API (PEP 503),
    and rewrite all download URLs to point to this server,
    underneath the current directory."""
    full_path_response = requests.get(f"https://pypi.org/simple/{package}")

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


@pypi.get("/{package}/{filename}", response_class=Response)
def get_pypi_file(package: str, filename: str):
    """Obtain and pass through a specific download for a PyPI package."""
    full_path_response = requests.get(f"https://pypi.org/simple/{package}")
    filename_bytes = re.escape(filename.encode("latin1"))

    selected_package_link = re.search(
        b'<a [^>]*?href="([^">]*)"[^>]*>' + filename_bytes + b"</a>",
        full_path_response.content,
    )
    if not selected_package_link:
        raise HTTPException(status_code=404, detail="File not found for package")
    original_url = selected_package_link.group(1)
    original_file = requests.get(original_url)
    return Response(
        content=original_file.content,
        media_type=original_file.headers["Content-Type"],
        status_code=original_file.status_code,
    )


@bootstrap.get("/", response_class=HTMLResponse)
def get_bootstrap_instructions(request: Request):
    """
    Return a website containing instructions for installing the Murfey client on a
    machine with no internet access.
    """
    return respond_with_template(
        "bootstrap.html",
        {
            "request": request,
        },
    )


@bootstrap.get("/pip.whl", response_class=Response)
def get_pip_wheel():
    """
    Return a static version of pip. This does not need to be the newest or best,
    but has to be compatible with all supported Python versions.
    This is only used during bootstrapping by the client to identify and then
    download the actually newest appropriate version of pip.
    """
    return get_pypi_file(package="pip", filename="pip-21.3.1-py3-none-any.whl")


@bootstrap.get("/murfey.whl", response_class=Response)
def get_murfey_wheel():
    """
    Return a wheel file containing the latest release version of Murfey. We should
    not have to worry about the exact Python compatibility here, as long as
    murfey.bootstrap is compatible with all relevant versions of Python.
    This also ignores yanked releases, which again should be fine.
    """
    full_path_response = requests.get("https://pypi.org/simple/murfey")
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
            status_code=404, detail="Could not identify appropriate version of Murfey"
        )
    newest_version = max(wheels)
    return get_pypi_file(package="murfey", filename=wheels[newest_version])
