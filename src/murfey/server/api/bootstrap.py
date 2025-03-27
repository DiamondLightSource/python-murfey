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
import io
import json
import logging
import random
import re
from urllib.parse import quote

import packaging.version
import requests
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

import murfey
from murfey.server import get_machine_config, respond_with_template

tag = {
    "name": "bootstrap",
    "description": __doc__,
    "externalDocs": {
        "description": "PEP 503",
        "url": "https://www.python.org/dev/peps/pep-0503/",
    },
}

# Set up API endpoint groups
# NOTE: Routers MUST have prefixes. prefix="" causes an error
version = APIRouter(prefix="/version", tags=["bootstrap"])
bootstrap = APIRouter(prefix="/bootstrap", tags=["bootstrap"])
cygwin = APIRouter(prefix="/cygwin", tags=["bootstrap"])
msys2 = APIRouter(prefix="/msys2", tags=["bootstrap"])
rust = APIRouter(prefix="/rust", tags=["bootstrap"])
pypi = APIRouter(prefix="/pypi", tags=["bootstrap"])
plugins = APIRouter(prefix="/plugins", tags=["bootstrap"])

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


@bootstrap.get("/", response_class=HTMLResponse)
def get_bootstrap_instructions(request: Request):
    """
    Return a website containing instructions for installing the Murfey client on a
    machine with no internet access.
    """

    return respond_with_template(
        request=request,
        filename="bootstrap.html",
    )


@bootstrap.get("/pip.whl", response_class=Response)
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


@cygwin.get("/setup-x86_64.exe", response_class=Response)
def get_cygwin_setup():
    """
    Obtain and pass through a Cygwin installer from an official source.
    This is used during client bootstrapping and can download and install the
    Cygwin distribution that then remains on the client machines.
    """
    filename = "setup-x86_64.exe"
    installer = requests.get(f"https://www.cygwin.com/{filename}")
    return Response(
        content=installer.content,
        media_type=installer.headers.get("Content-Type"),
        status_code=installer.status_code,
    )


@functools.lru_cache()
def find_cygwin_mirror() -> str:
    """
    Find an appropriate Cygwin mirror to pass download requests through to.
    We don't expect these to change often, so we only do this once during the
    lifetime of the server.
    """
    url = "https://www.cygwin.com/mirrors.lst"
    mirrors = requests.get(url)
    logger.info(
        f"Reading mirrors from {url} returned status code {mirrors.status_code} {mirrors.reason}"
    )

    # Don't cache result if we can't get mirrors list
    assert mirrors.status_code == 200

    mirror_priorities = {}
    for mirror in mirrors.content.split(b"\n"):
        mirror_line = mirror.decode("latin1").strip().split(";")
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


@cygwin.get("/{request_path:path}", response_class=Response)
def parse_cygwin_request(request_path: str):
    """
    Forward a Cygwin setup request to an official mirror.
    """

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
    cygwin_data = requests.get(url)
    return Response(
        content=cygwin_data.content,
        media_type=cygwin_data.headers.get("Content-Type"),
        status_code=cygwin_data.status_code,
    )


"""
=======================================================================================
MSYS2-RELATED FUNCTIONS AND ENDPOINTS
=======================================================================================
"""

# Variables used by the MSYS2 functions below
msys2_url = "https://repo.msys2.org"
msys2_setup_file = "msys2-x86_64-latest.exe"
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


@msys2.get("/setup-x86_64.exe", response_class=Response)
def get_msys2_setup():
    """
    Obtain and pass through an MSYS2 installer from an official source.
    This is used during client bootstrapping, and can download and install the
    MSYS2 distribution that then remains on the client machines.
    """

    installer = requests.get(f"{msys2_url}/distrib/{msys2_setup_file}")
    return Response(
        content=installer.content,
        media_type=installer.headers.get("Content-Type"),
        status_code=installer.status_code,
    )


@msys2.get("", response_class=Response)
def get_msys2_main_index(
    request: Request,
) -> Response:
    """
    Returns a simple index displaying valid MSYS2 systems and the latest setup file
    from the main MSYS2 repository.
    """

    def get_msys2_setup_html():
        """
        Returns the HTML line for the latest MSYS2 installer for Windows from an official
        source.
        """
        url = f"{msys2_url}/distrib"
        index = requests.get(url)
        content: bytes = index.content
        content_text: str = content.decode("latin1")

        for line in content_text.splitlines():
            if line.startswith("<a href="):
                if f'"{msys2_setup_file}"' in line:
                    return line
            else:
                pass
        return None

    def _rewrite_url(match):
        """
        Use regular expression matching to rewrite the package URLs and point them
        explicitly to this current server.
        """
        url = (
            f"{base_path}/{match.group(1)}"
            if not str(match.group(1)).startswith("http")
            else str(match.group(1))
        )
        return f'<a href="{url}">' + match.group(2) + "</a>"

    # Get base path to current FastAPI endpoint
    base_url = str(request.base_url).strip("/")
    path = request.url.path.strip("/")
    base_path = f"{base_url}/{path}"

    env_url = f"{msys2_url}"
    response = requests.get(env_url)

    # Parse and rewrite package index content
    content: bytes = response.content  # Get content in bytes
    content_text: str = content.decode("latin1")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        if line.startswith("<a href"):
            # Mirror only lines related to MSYS2 environments
            if any(env[0] in line for env in valid_envs):
                line_new = re.sub(
                    '^<a href="([^">]*)">([^<]*)</a>',  # Regex search criteria
                    _rewrite_url,  # Function to apply search criteria to
                    line,
                )
                content_text_list.append(line_new)

            # Replace the "distrib/" hyperlink with one to the setup file
            elif "distrib" in line:
                # Set up URL to be requested on the Murfey server
                mirror_file_name = "setup-x86_64.exe"
                setup_url = f"{base_path}/{mirror_file_name}"

                # Get request from the "distrib" page and rewrite it
                setup_html = get_msys2_setup_html()
                if setup_html is None:
                    # Skip showing the setup file link if it fails to find it
                    continue

                line_new = "               ".join(  # Adjust spaces to align columns
                    re.sub(
                        '^<a href="([^">]*)">([^"<]*)</a>',
                        f'<a href="{setup_url}">{mirror_file_name}</a>',
                        setup_html,
                    ).split("        ", 1)
                )
                content_text_list.append(line_new)
            # Other URLs don't need to be mirrored
            else:
                pass
        else:
            content_text_list.append(line)

    # Reconstruct conent
    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("latin1")  # Convert back to bytes
    return Response(
        content=content_new,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/{system}", response_class=Response)
def get_msys2_environment_index(
    system: str,
    request: Request,
) -> Response:
    """
    Returns a list of all MSYS2 environments for a given system from the main MSYS2
    repository.
    """

    def _rewrite_url(match):
        """
        Use regular expression matching to rewrite the package URLs and point them
        explicitly to this current server.
        """
        url = (
            f"{base_path}/{match.group(1)}"
            if not str(match.group(1)).startswith("http")
            else str(match.group(1))
        )
        return f'<a href="{url}">' + match.group(2) + "</a>"

    # Get base path to current FastAPI endpoint
    base_url = str(request.base_url).strip("/")
    path = request.url.path.strip("/")
    base_path = f"{base_url}/{path}"

    # Validate provided system
    if any(system in env[0] for env in valid_envs) is False:
        raise ValueError(f"{system!r} is not a valid msys2 environment")

    # Construct URL to main MSYS repo and get response
    arch_url = f'{msys2_url}/{quote(system, safe="/")}'
    response = requests.get(arch_url)

    # Parse and rewrite package index content
    content: bytes = response.content  # Get content in bytes
    content_text: str = content.decode("latin1")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        if line.startswith("<a href="):
            # Rewrite URL to point explicitly to current server
            line_new = re.sub(
                '^<a href="([^">]*)">([^<]*)</a>',  # Regex search criteria
                _rewrite_url,  # Function to apply search criteria to
                line,
            )
            content_text_list.append(line_new)
        else:
            content_text_list.append(line)

    # Reconstruct conent
    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("latin1")  # Convert back to bytes
    return Response(
        content=content_new,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/{system}/{environment}", response_class=Response)
def get_msys2_package_index(
    system: str,
    environment: str,
    request: Request,
) -> Response:
    """
    Obtain a list of all available MSYS2 packages for a given environment from the main
    MSYS2 repo.
    """

    def _rewrite_url(match):
        """
        Use regular expression matching to rewrite the package URLs and point them
        explicitly to this current server.
        """
        url = (
            f"{base_path}/{match.group(1)}"
            if not str(match.group(1)).startswith("http")
            else str(match.group(1))
        )
        return f'<a href="{url}">' + match.group(2) + "</a>"

    # Get base path to current FastAPI endpoint
    base_url = str(request.base_url).strip("/")
    path = request.url.path.strip("/")
    base_path = f"{base_url}/{path}"

    # Validate environment
    if any(system in env[0] and environment in env[1] for env in valid_envs) is False:
        raise ValueError(f"{system!r}/{environment!r} is not a valid msys2 environment")

    # Construct URL to main MSYS repo and get response
    package_list_url = (
        f'{msys2_url}/{quote(system, safe="/")}/{quote(environment, safe="/")}'
    )
    response = requests.get(package_list_url)

    # Parse and rewrite package index content
    content: bytes = response.content  # Get content in bytes
    content_text: str = content.decode("latin1")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        if line.startswith("<a href="):
            line_new = re.sub(
                '^<a href="([^">]*)">([^<]*)</a>',  # Regex search criteria
                _rewrite_url,  # Function to apply search criteria to
                line,
            )
            content_text_list.append(line_new)
        else:
            content_text_list.append(line)

    # Reconstruct conent
    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("latin1")  # Convert back to bytes
    return Response(
        content=content_new,
        status_code=response.status_code,
        media_type=response.headers.get("Content-Type"),
    )


@msys2.get("/{system}/{environment}/{package}", response_class=Response)
def get_msys2_package_file(
    system: str,
    environment: str,
    package: str,
) -> Response:
    """
    Obtain and pass through a specific download for an MSYS2 package.
    """

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
    package_file = requests.get(package_url)

    if package_file.status_code == 200:
        return Response(
            content=package_file.content,
            media_type=package_file.headers.get("Content-Type"),
            status_code=package_file.status_code,
        )
    else:
        raise HTTPException(status_code=package_file.status_code)


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

    This file is saved as ~/.cargo/config.toml on Linux devices by default, but
    will be saved as %USERPROFILE%\\.cargo\\config.toml on Windows ones.
    """

    # Construct URL to our mirror of the Rust sparse index
    index_mirror = (
        f"{request.url.scheme}://{request.url.netloc}/{rust.prefix.strip('/')}/index/"
    )

    # Construct and return the config.toml file
    config_data = "\n".join(
        [
            "[registries.murfey-crates]",
            f'index = "sparse+{index_mirror}"',  # sparse+ to use sparse index logic
            "",
            "[registry]",
            'default = "murfey-crates"',  # Use our registry as default
            "",
            "[registries.crates-io]",
            'index = "false"',  # Disables using crates.io
        ]
    )
    config_bytes = io.BytesIO(config_data.encode("utf-8"))

    return StreamingResponse(
        config_bytes,
        media_type="application/toml+json",
        headers={"Content-Disposition": "attachment; filename=config.toml"},
    )


@rust.get("/index")
def get_index_page():
    """
    Returns a mirror of the https://index.crates.io landing page.
    """

    response = http_session.get(rust_index)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code)
    return Response(
        content=response.content,
        media_type=response.headers.get("Content-Type"),
        status_code=response.status_code,
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

    # Construct URL for Rust router
    base_url = f"{request.url.scheme}://{request.url.netloc}" + rust.prefix

    # Construct config file with the necessary endpoints
    config = {
        "dl": f"{base_url}/crates",
        "api": f"{base_url}",
    }

    # Save it as a JSON and return it as part of the response
    json_data = json.dumps(config, indent=4)
    json_bytes = io.BytesIO(json_data.encode("utf-8"))

    return StreamingResponse(
        json_bytes,
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=config.json"},
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

    logger.debug(f"Received request to access {str(request.url)}")

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
        media_type="application/json",
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

    logger.debug(f"Received request to access {str(request.url)}")

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
        media_type="application/json",
    )


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

    logger.debug(f"Received request to access {str(request.url)}")

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

    logger.debug(f"Received request to access {str(request.url)}")

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
    links for said versions.
    """

    logger.debug(f"Received request to access {str(request.url)}")

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

    logger.debug(f"Received request to access {str(request.url)}")

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
    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Content-Type": response.headers.get(
                "Content-Type", "application/octet-stream"
            ),
            "Content-Length": response.headers.get("Content-Length"),
        },
        status_code=response.status_code,
    )


@rust.get("/crates/{package}/{version}/download", response_class=StreamingResponse)
def get_rust_package_download(
    request: Request,
    package: str,
    version: str,
):
    """
    Obtain and pass through a crate download request for a Rust package.
    """

    logger.debug(f"Received request to access {str(request.url)}")

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
    return StreamingResponse(
        content=response.iter_content(chunk_size=8192),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Content-Type": response.headers.get(
                "Content-Type", "application/octet-stream"
            ),
            "Content-Length": response.headers.get("Content-Length"),
        },
        status_code=response.status_code,
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

    logger.debug(f"Received request to access {str(request.url)}")

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
    return StreamingResponse(
        content=response.iter_content(),
        headers={
            "Content-Disposition": f'attachment; filename="{crate}"',
            "Content-Type": response.headers.get(
                "Content-Type", "application/octet-stream"
            ),
            "Content-Length": response.headers.get("Content-Length"),
        },
        status_code=response.status_code,
    )


"""
=======================================================================================
PYPI-RELATED FUNCTIONS AND ENDPOINTS
=======================================================================================
"""


def _get_full_pypi_path_response(package: str) -> requests.Response:
    """
    Validates the package name, sanitises it if valid, and attempts to return a HTTP
    response from PyPI.
    """

    # Check that a package name follows PEP 503 naming conventions, containing only
    # alphanumerics (including underscores; \w), dashes (\-), and periods (\.)
    if re.match(r"^[\w\-\.]+$", package) is not None:
        # Sanitise and normalise package name according to PEP 503
        package_clean = quote(re.sub(r"[-_.]+", "-", package.lower()), safe="/")

        # Get HTTP response
        url = f"https://pypi.org/simple/{package_clean}"
        response = requests.get(url)

        if response.status_code == 200:
            return response
        else:
            raise HTTPException(status_code=response.status_code)
    else:
        raise ValueError(f"{package!r} is not a valid package name")


@pypi.get("/", response_class=Response)
def get_pypi_index():
    """
    Obtain list of all PyPI packages via the simple API (PEP 503).
    """

    index = requests.get("https://pypi.org/simple/")

    return Response(
        content=index.content,
        status_code=index.status_code,
        media_type=index.headers.get("Content-Type"),
    )


@pypi.get("/{package}/", response_class=Response)
def get_pypi_package_downloads_list(package: str) -> Response:
    """
    Obtain list of all package downloads from PyPI via the simple API (PEP 503), and
    rewrite all download URLs to point to this server, under the current directory.
    """

    def _rewrite_pypi_url(match):
        """
        Use regular expression matching to rewrite the URLs. Points them from
        pythonhosted.org to current server, and removes the hash from the URL as well
        """
        # url = match.group(4)  # Original
        url = match.group(3)
        return '<a href="' + url + '"' + match.group(2) + ">" + match.group(3) + "</a>"

    # Validate package and URL
    full_path_response = _get_full_pypi_path_response(package)

    # Process lines related to PyPI packages in response
    content: bytes = full_path_response.content  # In bytes
    content_text: str = content.decode("latin1")  # Convert to strings
    content_text_list = []
    for line in content_text.splitlines():
        # Look for lines with hyperlinks
        if "<a href" in line:
            # Rewrite URL to point to current proxy server
            line_new = re.sub(
                '^<a href="([^">]*)"([^>]*)>([^<]*)</a>',  # Regex search criteria
                _rewrite_pypi_url,  # Function to apply search criteria to
                line,
            )
            content_text_list.append(line_new)

            # Add entry for wheel metadata (PEP 658; see _expose_wheel_metadata)
            if ".whl" in line_new:
                line_metadata = line_new.replace(".whl", ".whl.metadata")
                content_text_list.append(line_metadata)
        else:
            # Append other lines as normal
            content_text_list.append(line)

    content_text_new = str("\n".join(content_text_list))  # Regenerate HTML structure
    content_new = content_text_new.encode("latin1")  # Convert back to bytes

    return Response(
        content=content_new,
        media_type=full_path_response.headers.get("Content-Type"),
        status_code=full_path_response.status_code,
    )


@pypi.get("/{package}/{filename}", response_class=Response)
def get_pypi_file(package: str, filename: str):
    """
    Obtain and pass through a specific download for a PyPI package.
    """

    def _expose_wheel_metadata(response_bytes: bytes) -> bytes:
        """
        As of pip v22.3 (coinciding with PEP 658), pip expects to find an additonal
        ".whl.metadata" file based on the URL of the ".whl" file present on the PyPI Simple
        Index. However, because it is not listed on the webpage itself, it is not copied
        across to the proxy. This function adds that URL to the proxy explicitly.
        """

        # Analyse API response line-by-line
        response_text: str = response_bytes.decode("latin1")  # Convert to text
        response_text_list = []  # Write line-by-line analysis to here

        for line in response_text.splitlines():
            # Process URLs
            if r"<a href=" in line:
                response_text_list.append(line)  # Add to list

                # Add new line to explicitly call for wheel metadata
                if ".whl" in line:
                    # Add ".metadata" to URL and file name
                    line_new = line.replace(".whl", ".whl.metadata")
                    response_text_list.append(line_new)  # Add to list

            # Append all other lines as normal
            else:
                response_text_list.append(line)

        # Recover original structure
        response_text_new = str("\n".join(response_text_list))
        response_bytes_new = bytes(response_text_new, encoding="latin-1")

        return response_bytes_new

    # Validate package and URL
    full_path_response = _get_full_pypi_path_response(package)

    # Get filename in bytes
    filename_bytes = re.escape(filename.encode("latin1"))

    # Add explicit URLs for ".whl.metadata" files
    content = _expose_wheel_metadata(full_path_response.content)

    # Find package matching the specified filename
    selected_package_link = re.search(
        b'<a href="([^">]*)"[^>]*>' + filename_bytes + b"</a>",
        content,
    )
    if not selected_package_link:
        raise HTTPException(status_code=404, detail="File not found for package")
    original_url = selected_package_link.group(1)
    original_file = requests.get(original_url)

    return Response(
        content=original_file.content,
        media_type=original_file.headers.get("Content-Type"),
        status_code=original_file.status_code,
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
