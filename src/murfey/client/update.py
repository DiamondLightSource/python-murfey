from __future__ import annotations

import subprocess
import sys
from urllib.parse import urlparse

import requests

import murfey


def check(api_base: str, install: bool = True, force: bool = False):
    """
    Verify that the current client version can run against the selected server.
    If the version number is outside the allowed range then this can trigger
    an update on the client, and in that case will terminate the process.
    """
    server_reply = requests.get(
        f"{api_base}/version?client_version={murfey.__version__}"
    )
    if server_reply.status_code != 200:
        raise ValueError("Server unreachable")
    versions = server_reply.json()
    if not install:
        return
    print(
        f"Murfey {murfey.__version__} connected to Murfey server {versions['server']}"
    )
    if versions["client-needs-update"]:
        # install mandatory update
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


def install_murfey(api_base: str, version: str) -> bool:
    """Install a specific version of the Murfey client.
    Return 'true' on success and 'false' on error."""

    murfey_url = urlparse(api_base)
    murfey_base = f"{murfey_url.scheme}://{murfey_url.netloc}"
    murfey_hostname = murfey_url.netloc.split(":")[0]
    result = subprocess.run(
        [
            sys.executable,
            "-mpip",
            "install",
            "--trusted-host",
            murfey_hostname,
            "-i",
            f"{murfey_base}/pypi",
            f"murfey[client]=={version}",
        ]
    )
    return result.returncode == 0
