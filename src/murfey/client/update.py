from __future__ import annotations

import enum

import requests

import murfey


class UPDATE(enum.Enum):
    NONE = 0
    OPTIONAL = 1
    MANDATORY = 2


def check(api_base: str) -> UPDATE | None:
    server_reply = requests.get(
        f"{api_base}/version?client_version={murfey.__version__}"
    )
    if server_reply.status_code == 200:
        versions = server_reply.json()
        if versions["client-needs-update"]:
            return UPDATE.MANDATORY
        if versions["server"] != murfey.__version__:
            return UPDATE.OPTIONAL
        return UPDATE.NONE
    return None


def install():
    ...
