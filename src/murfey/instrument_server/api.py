import secrets
import time
from logging import getLogger
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, HTTPException, status
from jose import jwt
from pydantic import BaseModel

from murfey.client import read_config
from murfey.client.multigrid_control import MultigridController
from murfey.client.rsync import RSyncer
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.server.auth.api import Token
from murfey.util.instrument_models import MultigridWatcherSpec

logger = getLogger("murfey.instrument_server.api")

router = APIRouter()

watchers = {}
rsyncers: Dict[str, RSyncer] = {}
controllers = {}
tokens = {}

config = read_config()

encoded_jwt = jwt.encode(
    {"timestamp": time.time()},
    config["Murfey"].get("auth_key", secrets.token_hex(32)),
    algorithm=config["Murfey"].get("auth_algorithm", "HS256"),
)


def get_machine_config():
    return {}


@router.get("/health")
def health():
    return True


def _get_murfey_url() -> str:
    known_server = config["Murfey"].get("server")
    if not known_server:
        exit("Murfey server not set")
    if not known_server.startswith(("http://", "https://")):
        if "://" in known_server:
            exit("Unknown server protocol. Only http:// and https:// are allowed")
        known_server = f"http://{known_server}"
    return known_server


async def murfey_server_handshake(token: str) -> bool:
    # test provided token against Murfey server
    murfey_url = urlparse(_get_murfey_url(), allow_fragments=False)
    handshake_response = requests.get(
        f"{murfey_url.geturl()}/validate_token",
        headers={"Authorization": f"Bearer {token}"},
    )
    res = handshake_response.status_code == 200 and handshake_response.json().get(
        "valid"
    )
    if res:
        tokens["token"] = token
    return res


@router.post("/token")
async def token_handshake(token: Token):
    handshake_success = await murfey_server_handshake(token.access_token)
    if handshake_success:
        return Token(access_token=encoded_jwt, token_type="bearer")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Handshake failure between Murfey servers",
    )


@router.post("/sessions/{session_id}/multigrid_watcher")
def start_multigrid_watcher(
    session_id: int, watcher_spec: MultigridWatcherSpec
) -> bool:
    label = watcher_spec.label
    controllers[label] = MultigridController(
        [], watcher_spec.visit, session_id, demo=True, do_transfer=False
    )
    watchers[label] = MultigridDirWatcher(
        watcher_spec.source,
        watcher_spec.configuration.dict(),
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[label].subscribe(controllers[label]._start_rsyncer_multigrid)
    watchers[label].start()
    return True


@router.delete("/sessions/{session_id}/multigrid_watcher/{label}")
def stop_multigrid_watcher(session_id: int, label: str):
    watchers[label].request_stop()


class RsyncerSource(BaseModel):
    source: Path
    label: str


@router.post("/sessions/{session_id}/stop_rsyncer")
def stop_rsyncer(session_id: int, rsyncer_source: RsyncerSource):
    controllers[rsyncer_source.label].rsync_processes[
        rsyncer_source.source
    ]._halt_thread = True