from __future__ import annotations

import secrets
import subprocess
import time
from datetime import datetime
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Annotated, Dict, List, Optional, Union
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from werkzeug.utils import secure_filename

from murfey.client import read_config
from murfey.client.multigrid_control import MultigridController
from murfey.client.rsync import RSyncer
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util import sanitise, sanitise_nonpath, secure_path
from murfey.util.instrument_models import MultigridWatcherSpec
from murfey.util.models import File, Token

logger = getLogger("murfey.instrument_server.api")

watchers: Dict[Union[str, int], MultigridDirWatcher] = {}
rsyncers: Dict[str, RSyncer] = {}
controllers: Dict[int, MultigridController] = {}
data_collection_parameters: dict = {}
tokens = {}

config = read_config()

SECRET_KEY = config["Murfey"].get("auth_key", secrets.token_hex(32))
launch_time = time.time()

encoded_jwt = jwt.encode(
    {"timestamp": launch_time},
    SECRET_KEY,
    algorithm=config["Murfey"].get("auth_algorithm", "HS256"),
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def validate_session_token(
    session_id: int, token: Annotated[str, Depends(oauth2_scheme)]
):
    try:
        decoded_data = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[config["Murfey"].get("auth_algorithm", "HS256")],
        )
        if not decoded_data.get("session") == session_id:
            raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return session_id


MurfeySessionID = Annotated[int, Depends(validate_session_token)]

router = APIRouter()


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


async def murfey_server_handshake(token: str, session_id: int | None = None) -> bool:
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
        tokens["token" if session_id is None else session_id] = token
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


@router.post("/sessions/{session_id}/token")
async def token_handshake_for_session(session_id: int, token: Token):
    handshake_success = await murfey_server_handshake(
        token.access_token, session_id=session_id
    )
    if handshake_success:
        session_jwt = jwt.encode(
            {"session": session_id},
            SECRET_KEY,
            algorithm=config["Murfey"].get("auth_algorithm", "HS256"),
        )
        return Token(access_token=session_jwt, token_type="bearer")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Handshake failure between Murfey servers",
    )


@router.get("/sessions/{session_id}/check_token")
def check_token(session_id: MurfeySessionID):
    return {"token_valid": True}


@router.post("/sessions/{session_id}/multigrid_watcher")
def start_multigrid_watcher(
    session_id: MurfeySessionID, watcher_spec: MultigridWatcherSpec
):
    if controllers.get(session_id) is not None:
        return {"success": True}
    label = watcher_spec.label
    controllers[session_id] = MultigridController(
        [],
        watcher_spec.visit,
        watcher_spec.instrument_name,
        session_id,
        murfey_url=_get_murfey_url(),
        demo=True,
        do_transfer=True,
        processing_enabled=not watcher_spec.skip_existing_processing,
        _machine_config=watcher_spec.configuration.dict(),
        token=tokens.get(session_id, "token"),
        data_collection_parameters=data_collection_parameters.get(label, {}),
        rsync_restarts=watcher_spec.rsync_restarts,
    )
    watcher_spec.source.mkdir(exist_ok=True)
    machine_config = requests.get(
        f"{_get_murfey_url()}/instruments/{sanitise_nonpath(watcher_spec.instrument_name)}/machine",
        headers={"Authorization": f"Bearer {tokens[session_id]}"},
    ).json()
    for d in machine_config.get("create_directories", []):
        (watcher_spec.source / d).mkdir(exist_ok=True)
    watchers[session_id] = MultigridDirWatcher(
        watcher_spec.source,
        watcher_spec.configuration.dict(),
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[session_id].subscribe(
        partial(
            controllers[session_id]._start_rsyncer_multigrid,
            destination_overrides=watcher_spec.destination_overrides,
        )
    )
    watchers[session_id].start()
    return {"success": True}


@router.delete("/sessions/{session_id}/multigrid_watcher/{label}")
def stop_multigrid_watcher(session_id: MurfeySessionID, label: str):
    watchers[label].request_stop()


class RsyncerSource(BaseModel):
    source: Path
    label: str


@router.post("/sessions/{session_id}/stop_rsyncer")
def stop_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id].rsync_processes[rsyncer_source.source]._halt_thread = True
    return {"success": True}


@router.post("/sessions/{session_id}/remove_rsyncer")
def remove_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id]._request_watcher_stop(rsyncer_source.source)
    controllers[session_id].rsync_processes[rsyncer_source.source]._stopping = True
    controllers[session_id].rsync_processes[rsyncer_source.source]._halt_thread = True
    controllers[session_id].rsync_processes[rsyncer_source.source].queue.put(
        None, block=False
    )
    return {"success": True}


@router.post("/sessions/{session_id}/finalise_rsyncer")
def finalise_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id]._finalise_rsyncer(rsyncer_source.source)
    return {"success": True}


@router.post("/sessions/{session_id}/restart_rsyncer")
def restart_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id]._restart_rsyncer(rsyncer_source.source)
    return {"success": True}


class ProcessingParameters(BaseModel):
    dose_per_frame: Optional[float] = None
    extract_downscale: bool = True
    particle_diameter: Optional[float] = None
    symmetry: str = "C1"
    eer_fractionation: int = 20


class ProcessingParameterBlock(BaseModel):
    label: str
    params: ProcessingParameters


@router.post("/sessions/{session_id}/processing_parameters")
def register_processing_parameters(
    session_id: MurfeySessionID, proc_param_block: ProcessingParameterBlock
):
    data_collection_parameters[proc_param_block.label] = {}
    for k, v in proc_param_block.params.dict().items():
        data_collection_parameters[proc_param_block.label][k] = v
    return {"success": True}


@router.get(
    "/instruments/{instrument_name}/sessions/{session_id}/possible_gain_references"
)
def get_possible_gain_references(
    instrument_name: str, session_id: MurfeySessionID
) -> List[File]:
    machine_config = requests.get(
        f"{_get_murfey_url()}/instruments/{sanitise_nonpath(instrument_name)}/machine",
        headers={"Authorization": f"Bearer {tokens[session_id]}"},
    ).json()
    candidates = []
    for gf in secure_path(
        Path(machine_config["gain_reference_directory"]), keep_spaces=True
    ).glob("**/*"):
        if gf.is_file():
            candidates.append(
                File(
                    name=gf.name,
                    description="",
                    size=gf.stat().st_size / 1e6,
                    timestamp=datetime.fromtimestamp(gf.stat().st_mtime),
                    full_path=str(gf),
                )
            )
    candidates.sort(key=lambda x: x.timestamp, reverse=True)
    return candidates


class GainReference(BaseModel):
    gain_path: Path
    visit_path: str
    gain_destination_dir: str = "processing"


@router.post(
    "/instruments/{instrument_name}/sessions/{session_id}/upload_gain_reference"
)
def upload_gain_reference(
    instrument_name: str, session_id: MurfeySessionID, gain_reference: GainReference
):
    safe_gain_path = sanitise(str(gain_reference.gain_path))
    safe_visit_path = sanitise(gain_reference.visit_path)
    safe_destination_dir = sanitise(gain_reference.gain_destination_dir)
    machine_config = requests.get(
        f"{_get_murfey_url()}/instruments/{sanitise_nonpath(instrument_name)}/machine",
        headers={"Authorization": f"Bearer {tokens[session_id]}"},
    ).json()
    cmd = [
        "rsync",
        safe_gain_path,
        f"{urlparse(_get_murfey_url(), allow_fragments=False).hostname}::{machine_config.get('rsync_module', 'data')}/{safe_visit_path}/{safe_destination_dir}/{secure_filename(gain_reference.gain_path.name)}",
    ]
    gain_rsync = subprocess.run(cmd)
    if gain_rsync.returncode:
        logger.warning(
            f"Gain reference file {safe_gain_path} was not successfully transferred to {safe_visit_path}/processing"
        )
        return {"success": False}
    return {"success": True}


class UpstreamTiffInfo(BaseModel):
    download_dir: Path


@router.post("/visits/{visit_name}/sessions/{session_id}/upstream_tiff_data_request")
def gather_upstream_tiffs(
    visit_name: str, session_id: MurfeySessionID, upstream_tiff_info: UpstreamTiffInfo
):
    sanitised_visit_name = sanitise_nonpath(visit_name)
    assert not any(c in visit_name for c in ("/", "\\", ":", ";"))
    murfey_url = urlparse(_get_murfey_url(), allow_fragments=False)
    upstream_tiff_info.download_dir.mkdir(exist_ok=True)
    upstream_tiff_paths = (
        requests.get(
            f"{murfey_url.geturl()}/visits/{sanitised_visit_name}/upstream_tiff_paths",
            headers={"Authorization": f"Bearer {tokens[session_id]}"},
        ).json()
        or []
    )
    for tiff_path in upstream_tiff_paths:
        tiff_data = requests.get(
            f"{murfey_url.geturl()}/visits/{sanitised_visit_name}/upstream_tiff/{tiff_path}",
            stream=True,
            headers={"Authorization": f"Bearer {tokens[session_id]}"},
        )
        with open(upstream_tiff_info.download_dir / tiff_path, "wb") as utiff:
            for chunk in tiff_data.iter_content(chunk_size=32 * 1024**2):
                utiff.write(chunk)
