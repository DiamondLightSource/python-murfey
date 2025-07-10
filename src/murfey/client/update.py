from __future__ import annotations

import subprocess
import sys
from urllib.parse import ParseResult

import requests

import murfey
from murfey.util.api import url_path_for

# Standardised messages to print upon exit
UPDATE_SUCCESS = "Murfey has been updated. Please restart Murfey."
UPDATE_FAILURE = "Error occurred while updating Murfey."


def check(api_base: ParseResult, install: bool = True, force: bool = False):
    """
    Verify that the current client version can run against the selected server.
    If the version number is outside the allowed range then this can trigger
    an update on the client, and in that case will terminate the process.
    """
    proxy_path = api_base.path.rstrip("/")
    version_check_url = api_base._replace(
        path=f"{proxy_path}{url_path_for('bootstrap.version', 'get_version')}",
        query=f"client_version={murfey.__version__}",
    )
    server_reply = requests.get(version_check_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    versions = server_reply.json()
    if not install:
        return
    print(
        f"Murfey {murfey.__version__} connected to Murfey server {versions['server']}"
    )
    if versions["client-needs-update"] or versions["client-needs-downgrade"]:
        # Proceed with mandatory installation
        if versions["client-needs-update"]:
            print("This version of Murfey must be updated before continuing.")
        if versions["client-needs-downgrade"]:
            print(
                "This version of Murfey is too new for the server and must be downgraded before continuing."
            )
        result = install_murfey(api_base, versions["server"])
        if result:
            exit(UPDATE_SUCCESS)
        else:
            exit(UPDATE_FAILURE)

    if versions["server"] != murfey.__version__:
        if force:
            result = install_murfey(api_base, versions["server"])
            if result:
                exit(UPDATE_SUCCESS)
            else:
                exit(UPDATE_FAILURE)
        else:
            # Allow Murfey to start, but print an update prompt
            print("An update is available, install with 'murfey.client --update'.")


def install_murfey(api_base: ParseResult, version: str) -> bool:
    """Install a specific version of the Murfey client.
    Return 'true' on success and 'false' on error."""

    assert api_base.hostname is not None
    proxy_path = api_base.path.rstrip("/")
    result = subprocess.run(
        [
            sys.executable,
            "-mpip",
            "install",
            "--trusted-host",
            api_base.hostname,
            "-i",
            api_base._replace(
                path=f"{proxy_path}{url_path_for('bootstrap.pypi', 'get_pypi_index')}",
                query="",
            ).geturl(),
            f"murfey[client]=={version}",
        ]
    )
    return result.returncode == 0
