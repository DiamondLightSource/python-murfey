from __future__ import annotations

import subprocess
import sys
from urllib.parse import ParseResult

import requests

import murfey


def check(api_base: ParseResult, install: bool = True, force: bool = False):
    """
    Verify that the current client version can run against the selected server.
    If the version number is outside the allowed range then this can trigger
    an update on the client, and in that case will terminate the process.
    """
    version_check_url = api_base._replace(
        path="/version", query=f"client_version={murfey.__version__}"
    )
    server_reply = requests.get(version_check_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError("Server unreachable")
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
            print("\nMurfey has been updated. Please restart Murfey")
            exit()
        else:
            exit("Error occurred while updating Murfey")

    if versions["server"] != murfey.__version__:
        if force:
            result = install_murfey(api_base, versions["server"])
            if result:
                print("\nMurfey has been updated. Please restart Murfey")
                exit()
            else:
                exit("Error occurred while updating Murfey")
        else:
            print("An update is available, install with 'murfey update'.")


def install_murfey(api_base: ParseResult, version: str) -> bool:
    """Install a specific version of the Murfey client.
    Return 'true' on success and 'false' on error."""

    assert api_base.hostname is not None
    result = subprocess.run(
        [
            sys.executable,
            "-mpip",
            "install",
            "--trusted-host",
            api_base.hostname,
            "-i",
            api_base._replace(path="/pypi", query="").geturl(),
            f"murfey[client]=={version}",
        ]
    )
    return result.returncode == 0
