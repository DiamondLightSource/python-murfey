"""
API endpoints related to installing Murfey on a client.

Client machines may not have a direct internet connection, so Murfey allows
passing through download requests for Cygwin and MSYS2 to their respective
websites, and requests for Python packages to PyPI using the PEP 503 simple
API.

A static HTML page gives instructions on how to install on a network-isolated
system that has Python already installed. A previously set up system does not
need to have pip installed in order to bootstrap Murfey. Python and rsync are
required.
"""

from __future__ import annotations

import functools
import json
import logging
import random
import re
import zipfile
from io import BytesIO
from typing import Any
from urllib.parse import quote

import packaging.version
import requests
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

import murfey
from murfey.server.api import templates
from murfey.util.config import get_hostname, get_machine_config, get_microscope

tag = {
    "name": "Bootstrap",
    "description": __doc__,
    "externalDocs": {
        "description": "PEP 503",
        "url": "https://www.python.org/dev/peps/pep-0503/",
    },
}

# Set up API endpoint groups
# NOTE: Routers MUST have prefixes. prefix="" causes an error
version = APIRouter(prefix="/version", tags=["Bootstrap"])
bootstrap = APIRouter(prefix="/bootstrap", tags=["Bootstrap"])
cygwin = APIRouter(prefix="/cygwin", tags=["Bootstrap"])
msys2 = APIRouter(prefix="/msys2", tags=["Bootstrap"])
rust = APIRouter(prefix="/rust", tags=["Bootstrap"])
pypi = APIRouter(prefix="/pypi", tags=["Bootstrap"])
plugins = APIRouter(prefix="/plugins", tags=["Bootstrap"])

logger = logging.getLogger("murfey.server.api.bootstrap")

# Create a reusable HTTP session to avoid spamming external endpoints
http_session = requests.Session()

"""
=======================================================================================
GENERAL HELPER FUNCTIONS
=======================================================================================
"""


def _sanitise_str(input: str) -> str:
    # Remove \r and \n characters from the string
    input_clean = input.replace("\r", "").replace("\n", "").rstrip()
    return input_clean


def resolve_netloc(request: Request) -> str:
    """
    Helper function to construct the correct netloc (hostname[:port]) to use based on
    the request received. It will prioritise parsing the request headers for the host,
    port, protocol and using them to construct the netloc before defaulting to parsing
    the FastAPI Request object to do so.
    """

    # Prefer headers added by reverse proxies
    host = request.headers.get("X-Forwarded-Host", request.url.hostname)
    port = request.headers.get(
        "X-Forwarded-Port", str(request.url.port) if request.url.port else None
    )
    proto = request.headers.get("X-Forwarded-Proto", request.url.scheme)

    # Default ports shouldn't be included; if no ports are found, return just the host
    if (
        (proto == "http" and port == "80")
        or (proto == "https" and port == "443")
        or not port
    ):
        return host

    return f"{host}:{port}"


"""
=======================================================================================
VERSION-RELATED API ENDPOINTS
=======================================================================================
"""


@version.get("/")
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


"""
=======================================================================================
GENERAL BOOTSTRAP-RELATED API ENDPOINTS
=======================================================================================
"""


def respond_with_template(
    request: Request, filename: str, parameters: dict[str, Any] | None = None
):
    template_parameters = {
        "hostname": get_hostname(),
        "microscope": get_microscope(),
        "version": murfey.__version__,
        # Extra parameters to reconstruct URLs for forwarded requests
        "netloc": request.url.netloc,
        "proxy_path": "",
    }
    if parameters:
        template_parameters.update(parameters)
    return templates.TemplateResponse(
        request=request, name=filename, context=template_parameters
    )


@bootstrap.get("/", response_class=HTMLResponse)
def get_bootstrap_instructions(request: Request):
    """
    Return a website containing instructions for installing the Murfey client on a
    machine with no internet access.
    """

    # Check if this is a forwarded request from somewhere else and construct netloc
    netloc = resolve_netloc(request)

    # Find path to 'bootstrap' router using current URL path
    proxy_path = request.url.path.removesuffix(f"{bootstrap.prefix}/")

    return respond_with_template(
        request=request,
        parameters={
            "netloc": netloc,
            "proxy_path": proxy_path,
        },
        filename="bootstrap.html",
    )


@bootstrap.get("/pip.whl", response_class=StreamingResponse)
def get_pip_wheel():
    """
    Return a static version of pip. This does not need to be the newest or best,
    but has to be compatible with all supported Python versions.
    This is only used during bootstrapping by the client to identify and then
    download the actually newest appropriate version of pip.
    """
    return get_pypi_file(
        package="pip",
        filename="pip-22.2.2-py3-none-any.whl",  # Highest version that works before PEP 658 change
    )


@bootstrap.get("/murfey.whl", response_class=StreamingResponse)
def get_murfey_wheel():
    """
    Return a wheel file containing the latest release version of Murfey. We should
    not have to worry about the exact Python compatibility here, as long as
    murfey.bootstrap is compatible with all relevant versions of Python.
    This also ignores yanked releases, which again should be fine.
    """
    full_path_response = http_session.get(f"{pypi_index_url.rstrip('/')}/murfey")
    wheels = {}

    for wheel_file in re.findall(
        b"<a [^>]*>([^<]*).whl</a>",
        full_path_response.content,
    ):
        try:
            filename = wheel_file.decode("utf-8") + ".whl"
            version = packaging.version.parse(filename.split("-")[1])
            wheels[version] = filename
        except Exception:
            pass  # Ignore searches that fail to return wheels
    if not wheels:
        raise HTTPException(
            status_code=404, detail="Could not identify appropriate version of Murfey"
        )
    newest_version = max(wheels)
    return get_pypi_file(package="murfey", filename=wheels[newest_version])


"""
=======================================================================================
CYGWIN-RELATED API ENDPOINTS
=======================================================================================
"""


@cygwin.get("/setup-x86_64.exe", response_class=StreamingResponse)
def get_cygwin_setup():
    """
    Obtain and pass through a Cygwin installer from an official source.
    This is used during client bootstrapping and can download and install the
    Cygwin distribution that then remains on the client machines.
    """
    filename = "setup-x86_64.exe"
    response = http_session.get(f"https://www.cygwin.com/{filename}")

    # Construct headers to return with response
    headers: dict[str, str] = {
        "Content-Disposition": f"attachment; filename=cygwin-{filename}"
    }
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type"),
    )


@functools.lru_cache()
def find_cygwin_mirror() -> str:
    """
    Find an appropriate Cygwin mirror to pass download requests through to.
    We don't expect these to change often, so we only do this once during the
    lifetime of the server.
    """
    url = "https://www.cygwin.com/mirrors.lst"
    mirrors = http_session.get(url)
    logger.info(
        f"Reading mirrors from {url} returned status code {mirrors.status_code} {mirrors.reason}"
    )

    # Don't cache result if we can't get mirrors list
    assert mirrors.status_code == 200

    mirror_priorities = {}
    for mirror in mirrors.content.split(b"\n"):
        mirror_line = mirror.decode("utf-8").strip().split(";")
        if not mirror_line or len(mirror_line) < 4:
            continue
        if not mirror_line[0].startswith("http"):
            continue
        if mirror_line[3] == "UK":
            priority = 20
        elif mirror_line[2] == "Europe":
            priority = 10
        else:
            priority = 0
        if mirror_line[0].startswith("https"):
            priority += 5
        mirror_priorities[mirror_line[0]] = priority

    elegible_mirrors = [
        mirror
        for mirror, priority in mirror_priorities.items()
        if priority == max(mirror_priorities.values())
    ]
    if not elegible_mirrors:
        logger.warning("No valid mirrors identified")
        assert elegible_mirrors

    picked_mirror = random.choice(elegible_mirrors)
    if not picked_mirror.endswith("/"):
        picked_mirror += "/"
    logger.info(f"Picked Cygwin mirror: {picked_mirror}")
    return picked_mirror


@cygwin.get("/{request_path:path}", response_class=StreamingResponse)
def parse_cygwin_request(
    request: Request,
    request_path: str,
):
    """
    Forward a Cygwin setup request to an official mirror.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate request path
    if bool(re.fullmatch(r"^[\w\s\.\-\+/]+$", request_path)) is False:
        raise ValueError(f"{request_path!r} is not a valid request path")

    try:
        url = f'{find_cygwin_mirror()}{quote(request_path, safe="/")}'
    except Exception:
        raise HTTPException(
            status_code=503, detail="Could not identify a suitable Cygwin mirror"
        )

    logger.info(f"Forwarding Cygwin download request to {_sanitise_str(url)}")
    response = http_session.get(url)

    headers: dict[str, str] = {}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type"),
    )


"""
=======================================================================================
MSYS2-RELATED FUNCTIONS AND ENDPOINTS
=======================================================================================
"""

# Variables used by the MSYS2 functions below
msys2_url = "https://repo.msys2.org"
msys2_file_ext = (".exe", ".sig", ".tar.xz", "tar.zst")
valid_envs = (
    # Tuple of systems and supported libraries/compilers/architectures within
    (
        "msys",  # Cygwin-like system
        (
            # Available environments
            "i686",  # 32-bit
            "x86_64",  # 64-bit
            "sources",
        ),
    ),
    (
        "mingw",  # Windows-like system
        (
            # Available environments
            # Toolchain: llvm, C library:   ucrt, C++ library:    libc++
            "clang32",
            "clang64",
            "clangarm64",
            # Toolchain:  gcc, C library:   ucrt, C++ library: libstdc++
            "ucrt64",
            # Toolchain:  gcc, C library: msvcrt, C++ library: libstdc++
            "mingw32",
            "mingw64",
            # Architecture types
            "i686",  # 32-bit
            "x86_64",  # 64-bit
            "sources",
        ),
    ),
)


@msys2.get("/config/pacman.d.zip", response_class=StreamingResponse)
def get_pacman_mirrors(request: Request):
    """
    Dynamically generates a zip file containing mirrorlist files that have been set
    up to mirror the MSYS2 package database for each environment.

    The files in this folder should be pasted into, and overwrite, the 'mirrorlist'
    files present in the %MSYS64%\\etc\\pacman.d folder. The default path to this
    folder is C:\\msys64\\etc\\pacman.d.
    """

    # Check if this is a forwarded request from somewhere else and construct netloc
    netloc = resolve_netloc(request)

    # Find path to Rust router using current URL Path
    path_to_router = request.url.path.removesuffix("/config/pacman.d.zip")

    # Construct base URL for subsequent use
    base_url = f"{request.url.scheme}://{netloc}{path_to_router}"
    logger.debug(f"Base URL to MSYS2 sub-router determined to be {base_url}")

    # Construct package database mirrors
    # Files are called mirrorlist.{environment}
    # URL format: {scheme}://{netloc}{proxy_path}/{router_prefix}/path/to/repo
    url_paths = {
        "clang64": "mingw/clang64",
        "mingw": "mingw/$repo",
        "mingw32": "mingw/i686",
        "mingw64": "mingw/x86_64",
        "msys": "msys/$arch",
        "ucrt64": "mingw/ucrt64",
    }
    # Construct file names and contents
    mirror_lists = {
        f"mirrorlist.{env}": "\n".join(
            [
                "# See https://www.msys2.org/dev/mirrors",
                "",
                "## Primary",
                f"Server = {base_url}/repo/{repo_path}",
                "",
            ]
        )
        for env, repo_path in url_paths.items()
    }

    # Create in-memory buffer for the ZIP file
    zip_buffer = BytesIO()

    # Create a zip file in the buffer
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_name, content in mirror_lists.items():
            zip_file.writestr(file_name, content)
    zip_buffer.seek(0)  # Move object pointer back to start

    # Construct and return streaming response
    headers = {
        "Content-Disposition": "attachment; filename=pacman.d.zip",
        "Content-Length": str(zip_buffer.getbuffer().nbytes),
    }
    return StreamingResponse(
        zip_buffer,
        status_code=200,
        headers=headers,
        media_type="application/zip",
    )


@msys2.get("/repo/distrib/{setup_file}", response_class=StreamingResponse)
def get_msys2_setup(
    request: Request,
    setup_file: str,
):
    """
    Obtain and pass through an MSYS2 installer from an official source.
    This is used during client bootstrapping, and can download and install the
    MSYS2 distribution that then remains on the client machines.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate characters in sent path
    if not bool(re.fullmatch(r"^[\w\.\-]+$", setup_file)):
        raise ValueError("Unallowed characters present in requested setup file")

    # Allow only '.exe', 'tar.xz', 'tar.zst', or '.sig' files
    if not setup_file.startswith("msys2") and not any(
        setup_file.endswith(ext) for ext in (msys2_file_ext)
    ):
        raise ValueError(f"{setup_file!r} is not a valid executable")

    response = http_session.get(f"{msys2_url}/distrib/{setup_file}")

    headers: dict[str, str] = {}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/repo/", response_class=Response)
def get_msys2_main_index(
    request: Request,
) -> Response:
    """
    Returns a simple index displaying valid MSYS2 systems and the latest setup file
    from the main MSYS2 repository.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Construct URL and get response
    env_url = f"{msys2_url}"
    response = http_session.get(env_url)

    # Parse and rewrite package index content
    content: bytes = response.content  # Get content in bytes
    content_text: str = content.decode("utf-8")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        if line.startswith("<a href"):
            # Mirror only lines related to MSYS2 environments or the distribution folder
            if any(env[0] in line for env in valid_envs) or "distrib" in line:
                content_text_list.append(line)
            # Other URLs don't need to be mirrored
            else:
                continue
        else:
            content_text_list.append(line)

    # Reconstruct conent
    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("utf-8")  # Convert back to bytes
    return Response(
        content=content_new,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/repo/{system}/", response_class=Response)
def get_msys2_environment_index(
    request: Request,
    system: str,
) -> Response:
    """
    Returns a list of all MSYS2 environments for a given system from the main MSYS2
    repository.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate provided system; use this endpoint to display 'distrib' folder too
    if not (any(system in env[0] for env in valid_envs) or system == "distrib"):
        raise ValueError(f"{system!r} is not a valid msys2 environment")

    # Construct URL to main MSYS repo and get response
    arch_url = f'{msys2_url}/{quote(system, safe="/")}'
    response = http_session.get(arch_url)

    # Parse and rewrite package index content
    content: bytes = response.content  # Get content in bytes
    content_text: str = content.decode("utf-8")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        if line.startswith("<a href="):
            # Skip non-executable files when querying 'distrib' repo
            if system == "distrib":
                if not any(ext in line for ext in msys2_file_ext):
                    continue
            content_text_list.append(line)
        else:
            content_text_list.append(line)

    # Reconstruct conent
    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("utf-8")  # Convert back to bytes
    return Response(
        content=content_new,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/repo/{system}/{environment}/", response_class=Response)
def get_msys2_package_index(
    request: Request,
    system: str,
    environment: str,
) -> Response:
    """
    Obtain a list of all available MSYS2 packages for a given environment from the main
    MSYS2 repo.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate environment
    if any(system in env[0] and environment in env[1] for env in valid_envs) is False:
        raise ValueError(f"{system!r}/{environment!r} is not a valid msys2 environment")

    # Construct URL to main MSYS repo and get response
    package_list_url = (
        f'{msys2_url}/{quote(system, safe="/")}/{quote(environment, safe="/")}'
    )
    response = http_session.get(package_list_url)
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/repo/{system}/{environment}/{package}", response_class=StreamingResponse)
def get_msys2_package_file(
    request: Request,
    system: str,
    environment: str,
    package: str,
) -> Response:
    """
    Obtain and pass through a specific download for an MSYS2 package.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate environment
    if any(system in env[0] and environment in env[1] for env in valid_envs) is False:
        raise ValueError(f"'{system}/{environment}' is not a valid msys2 environment")

    # Validate package name
    #   MSYS2 package names contain:
    #   - alphanumerics (includes "_"; \w),
    #   - periods (\.),
    #   - dashes (\-),
    #   - tildes (~), and
    #   - plus signs (+)
    if bool(re.fullmatch(r"^[\w\.\-\+~]+$", package)) is False:
        raise ValueError(f"{package!r} is not a valid package name")

    # Construct URL to main MSYS repo and get response
    package_url = f'{msys2_url}/{quote(system, safe="/")}/{quote(environment, safe="/")}/{quote(package, safe="/")}'
    response = http_session.get(package_url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)

    headers: dict[str, str] = {}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type"),
    )


"""
=======================================================================================
RUST-RELATED FUNCTIONS AND ENDPOINTS
=======================================================================================
"""

# Base URLs to use
rust_index = "https://index.crates.io"
rust_dl = "https://static.crates.io/crates"
rust_api = "https://crates.io"


@rust.get("/cargo/config.toml", response_class=StreamingResponse)
def get_cargo_config(request: Request):
    """
    Returns a properly configured Cargo config that sets it to look ONLY at the
    crates.io mirror.

    The default path for this config on Linux devices is ~/.cargo/config.toml,
    and its default path on Windows is %USERPROFILE%\\.cargo\\config.toml.
    """

    # Check if this is a forwarded request from somewhere else and construct netloc
    netloc = resolve_netloc(request)

    # Find path to Rust router using current URL Path
    path_to_router = request.url.path.removesuffix("/cargo/config.toml")

    # Construct base URL for subsequent use
    base_url = f"{request.url.scheme}://{netloc}{path_to_router}"
    logger.debug(f"Base URL to Rust sub-router determined to be {base_url}")

    # Construct URL to our mirror of the Rust sparse index
    index_url = f"{base_url}/index/"

    # Construct and return the config.toml file
    config_data = "\n".join(
        [
            "[source.crates-io]",
            'replace-with = "murfey-crates"',  # Redirect to our mirror
            "",
            "[source.murfey-crates]",
            f'registry = "sparse+{index_url}"',  # sparse+ to use sparse protocol
            "",
            "[registries.murfey-crates]",
            f'index = "sparse+{index_url}"',  # sparse+ to use sparse protocol
            "",
            "[registry]",
            'default = "murfey-crates"',  # Redirect to our mirror
            "",
        ]
    )
    config_bytes = config_data.encode("utf-8")

    headers: dict[str, str] = {
        "Content-Disposition": "attachment; filename=config.toml",
        "Content-Length": str(len(config_bytes)),
    }

    return StreamingResponse(
        BytesIO(config_bytes),
        status_code=200,
        headers=headers,
        media_type="application/toml+json",
    )


"""
crates.io Sparse Index Registry Key Endpoints
"""


@rust.get("/index/", response_class=Response)
def get_index_page():
    """
    Returns a mirror of the https://index.crates.io landing page.
    """

    response = http_session.get(rust_index)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@rust.get("/index/config.json", response_class=StreamingResponse)
def get_index_config(request: Request):
    """
    Download a config.json file used by Cargo to navigate sparse index registries
    with.

    The 'dl' key points to our mirror of the static crates.io repository, while
    the 'api' key points to an API version of that same registry. Both will be
    used by Cargo when searching for and downloading packages.
    """

    # Check if this is a forwarded request from somewhere else and construct netloc
    netloc = resolve_netloc(request)

    # Find path to Rust router using current URL Path
    path_to_router = request.url.path.removesuffix("/index/config.json")

    # Construct base URL for subsequent use
    base_url = f"{request.url.scheme}://{netloc}{path_to_router}"
    logger.debug(f"Base URL to Rust sub-router determined to be {base_url}")

    # Construct config file with the necessary endpoints
    config = {
        "dl": f"{base_url}/crates",
        "api": f"{base_url}",
    }

    # Save it as a JSON and return it as part of the response
    json_data = json.dumps(config, indent=4)
    json_bytes = json_data.encode("utf-8")

    headers: dict[str, str] = {
        "Content-Disposition": "attachment; filename=config.json",
        "Content-Length": str(len(json_bytes)),
    }

    return StreamingResponse(
        BytesIO(json_bytes),
        status_code=200,
        headers=headers,
        media_type="application/json",
    )


@rust.get("/index/{c1}/{c2}/{package}", response_class=StreamingResponse)
def get_index_package_metadata(
    request: Request,
    c1: str,
    c2: str,
    package: str,
):
    """
    Download the metadata for a given package from the crates.io sparse index.
    The path to the metadata file on the server side takes the following form:
    /{c1}/{c2}/{package}

    c1 and c2 are 2 characters-long strings that are taken from the first 4
    characters of the package name (a-z, A-Z, 0-9, -, _). For 3-letter packages,
    c1 = 3, and c2 is the first character of the package.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate path to the package metadata
    if any(not re.fullmatch(r"[\w\-]{1,2}", char) for char in (c1, c2)):
        raise ValueError("Invalid path to package metadata")

    if len(c1) == 1 and not c1 == "3":
        raise ValueError("Invalid path to package metadata")
    if c1 == "3" and not len(c2) == 1:
        raise ValueError("Invalid path to package metadata")

    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")

    # Request and return the metadata as a JSON file
    url = f"{rust_index}/{c1}/{c2}/{package}"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return StreamingResponse(
        response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@rust.get("/index/{n}/{package}", response_class=StreamingResponse)
def get_index_package_metadata_for_short_package_names(
    request: Request,
    n: str,
    package: str,
):
    """
    The Rust sparse index' naming scheme for packages with 1-2 characters is
    different from the standard path convention. They are stored under
    /1/{package} or /2/{package}.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate path to crate
    if n not in ("1", "2"):
        raise ValueError("Invalid path to package metadata")
    if not re.fullmatch(r"[\w\-]{1,2}", package):
        raise ValueError("Invalid package name")

    # Request and return the metadata as a JSON file
    url = f"{rust_index}/{n}/{package}"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return StreamingResponse(
        response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@rust.get("/crates/{package}/{version}/download", response_class=StreamingResponse)
def get_rust_package_download(
    request: Request,
    package: str,
    version: str,
):
    """
    Obtain and pass through a crate download request for a Rust package via the
    sparse index registry.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate package and version
    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")
    if not re.fullmatch(r"[\w\-\.\+]+", version):
        raise ValueError("Invalid version number")

    # Request and return crate from https://static.crates.io
    url = f"{rust_dl}/{package}/{version}/download"
    response = http_session.get(url)
    file_name = f"{package}-{version}.crate"  # Construct file name to save package as
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)

    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type", "application/octet-stream"),
    )


"""
crates.io API Key Endpoints
"""


@rust.get("/api/v1/crates")
def get_rust_api_package_index(
    request: Request,
    package: str = Query(None, alias="q"),
    per_page: int = Query(10),
    cursor: str = Query(None, alias="seek"),
):
    """
    Displays the Rust API package index, which returns names of available packages
    in a JSON object based on the search query given.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate package name
    if package and not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")
    if cursor and not re.fullmatch(r"[a-zA-Z0-9]+", cursor):
        raise ValueError("Invalid cursor")

    # Formulate the search query to pass to the crates page
    search_params: dict[str, str | int] = {}
    if package:
        search_params["q"] = package
    if per_page:
        search_params["per_page"] = per_page
    if cursor:
        search_params["seek"] = cursor

    # Submit request and return response
    url = f"{rust_api}/api/v1/crates"
    response = http_session.get(url, params=search_params)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return response.json()


@rust.get("/api/v1/crates/{package}")
def get_rust_api_package_info(
    request: Request,
    package: str,
):
    """
    Displays general information for a given Rust package, as a JSON object.
    Contains both version information and download information, in addition
    to other types of metadata.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate package name
    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")

    # Return JSON of the package's page
    url = f"{rust_api}/api/v1/crates/{package}"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return response.json()


@rust.get("/api/v1/crates/{package}/versions")
def get_rust_api_package_versions(
    request: Request,
    package: str,
):
    """
    Displays all available versions for a particular Rust package, along with download
    links for said versions, as a JSON object.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate crate name
    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")

    # Return JSON of the package's version information
    url = f"{rust_api}/api/v1/crates/{package}/versions"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return response.json()


@rust.get(
    "/api/v1/crates/{package}/{version}/download", response_class=StreamingResponse
)
def get_rust_api_package_download(
    request: Request,
    package: str,
    version: str,
):
    """
    Obtain and pass through a crate download request for a specific Rust package.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate package name
    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")
    # Validate version number
    # Not all developers adhere to guidelines when versioning their packages, so
    # '-', '_', '+', as well as letters can also be present in this field.
    if not re.fullmatch(r"[\w\-\.\+]+", version):
        raise ValueError("Invalid version number")

    # Request and return package
    url = f"{rust_api}/api/v1/crates/{package}/{version}/download"
    response = http_session.get(url)
    file_name = f"{package}-{version}.crate"  # Construct crate name to save as
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)

    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type", "application/octet-stream"),
    )


@rust.get("/crates/{package}/{crate}", response_class=StreamingResponse)
def get_rust_package_crate(
    request: Request,
    package: str,
    crate: str,
):
    """
    Obtain and pass through a download for a specific Rust crate. The Rust API
    download request actually redirects to the static crate repository, so this
    endpoint covers cases where the static crate download link is requested.

    The static Rust package repository has been configured such that only requests
    for a specific crate are accepted and handled.
    (e.g. https://static.crates.io/crates/anyhow/anyhow-1.0.97.crate will pass)

    A request for any other part of the URL path will be denied.
    (e.g. https://static.crates.io/crates/anyhow will fail)
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Validate crate and package names
    if not re.fullmatch(r"[\w\-]+", package):
        raise ValueError("Invalid package name")
    if not crate.endswith(".crate"):
        raise ValueError("This is a not a Rust crate")
    # Rust crates follow a '{crate}-{version}.crate' structure
    if not re.fullmatch(r"[\w\-\.]+\.crate", crate):
        raise ValueError("Invalid crate name")

    # Request and return package
    url = f"{rust_dl}/{package}/{crate}"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)

    headers = {"Content-Disposition": f'attachment; filename="{crate}"'}
    if response.headers.get("Content-Length"):
        headers["Content-Length"] = response.headers["Content-Length"]

    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type", "application/octet-stream"),
    )


"""
=======================================================================================
PYPI-RELATED FUNCTIONS AND ENDPOINTS
=======================================================================================
"""

python_repo_url = "https://files.pythonhosted.org"
pypi_index_url = "https://pypi.org/simple/"


def _get_full_pypi_path_response(package: str) -> requests.Response:
    """
    Validates the package name, sanitises it if valid, and attempts to return a HTTP
    response from PyPI.
    """

    # Check that a package name follows PEP 503 naming conventions, containing only
    # alphanumerics (including underscores; \w), dashes (\-), and periods (\.)
    if not re.fullmatch(r"[\w\-\.]+", package):
        raise ValueError(f"{package!r} is not a valid package name")

    # Sanitise and normalise package name according to PEP 503
    package_clean = quote(re.sub(r"[-_.]+", "-", package.lower()), safe="/")

    # Get HTTP response
    url = f"{pypi_index_url.rstrip('/')}/{package_clean}"
    response = http_session.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return response


@pypi.get("/index/", response_class=Response)
def get_pypi_index():
    """
    Obtain list of all PyPI packages via the simple API (PEP 503).
    """

    response = http_session.get(pypi_index_url)
    return Response(
        content=response.content,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@pypi.get("/index/{package}/", response_class=Response)
def get_pypi_package_downloads_list(request: Request, package: str) -> Response:
    """
    Obtain list of all package downloads from PyPI via the simple API (PEP 503), and
    rewrite all download URLs to point to this server, under the current directory.
    """

    logger.debug(f"Received request to access {str(request.url)!r}")

    # Construct base URL to rewrite with
    netloc = resolve_netloc(request)
    scheme = request.headers.get("X-Forwarded-Proto", request.url.scheme)
    router_path = request.url.path.removesuffix(f"/index/{package}/")
    base_url = f"{scheme}://{netloc}{router_path}"

    # Validate package and URL
    full_path_response = _get_full_pypi_path_response(package)

    # Process lines related to PyPI packages in response
    content: bytes = full_path_response.content  # In bytes
    content_text: str = content.decode("utf-8")  # Convert to strings

    # PyPI's simple index now directly points to https://pythonhosted.org
    # It also uses newlines partway through the '<a ...></a>' blocks now
    # It's thus now better to use regex substitution on the page as a whole
    content_text_new = re.sub(re.escape(python_repo_url), base_url, content_text)

    content_new = content_text_new.encode("utf-8")  # Convert back to bytes

    return Response(
        content=content_new,
        status_code=full_path_response.status_code,
        media_type=full_path_response.headers.get("Content-Type"),
    )


@pypi.get("/packages/{a}/{b}/{c}/{filename}", response_class=StreamingResponse)
def get_pypi_file(
    request: Request,
    a: str,
    b: str,
    c: str,
    filename: str,
):
    """
    Obtain and pass through a specific download for a PyPI package.
    """
    logger.debug(f"Received request to access {str(request.url)!r}")

    package_url = f"{python_repo_url}/packages/{a}/{b}/{c}/{filename}"
    logger.debug(f"Forwarding package request to {package_url!r}")
    response = http_session.get(package_url, stream=True)

    # Construct headers to return with response
    headers: dict[str, str] = {}
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return StreamingResponse(
        content=response.raw,
        status_code=response.status_code,
        headers=headers,
        media_type=response.headers.get("Content-Type"),
    )


"""
=======================================================================================
PYPI API ENDPOINT PLUGINS
=======================================================================================
"""


@plugins.get("/instruments/{instrument_name}/{package}", response_class=FileResponse)
def get_plugin_wheel(instrument_name: str, package: str):
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    wheel_path = machine_config.plugin_packages.get(package)
    if wheel_path is None:
        return None
    return FileResponse(
        wheel_path,
        headers={"Content-Disposition": "attachment; filename={wheel_path.name}"},
    )
