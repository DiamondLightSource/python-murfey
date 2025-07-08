from __future__ import annotations

import secrets
import subprocess
import time
from datetime import datetime
from functools import partial
from logging import getLogger
from pathlib import Path
from typing import Annotated, Any, Optional
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from werkzeug.utils import secure_filename

from murfey.client.multigrid_control import MultigridController
from murfey.client.rsync import RSyncer
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util import posix_path, sanitise, sanitise_nonpath, secure_path
from murfey.util.api import url_path_for
from murfey.util.client import read_config
from murfey.util.instrument_models import MultigridWatcherSpec
from murfey.util.models import File, Token

logger = getLogger("murfey.instrument_server.api")

watchers: dict[str | int, MultigridDirWatcher] = {}
rsyncers: dict[str, RSyncer] = {}
controllers: dict[int, MultigridController] = {}
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
    """
    Validates the token received from the backend server
    """
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
            detail="Could not validate credentials from backend",
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
        f"{murfey_url.geturl()}{url_path_for('auth.router', 'simple_token_validation')}",
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
def setup_multigrid_watcher(
    session_id: MurfeySessionID, watcher_spec: MultigridWatcherSpec
):
    # Return 'True' if controllers are already set up
    if controllers.get(session_id) is not None:
        return {"success": True}

    label = watcher_spec.label
    for sid, controller in controllers.items():
        if controller.dormant:
            del controllers[sid]

    # Load machine config as dictionary
    machine_config: dict[str, Any] = requests.get(
        f"{_get_murfey_url()}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=sanitise_nonpath(watcher_spec.instrument_name))}",
        headers={"Authorization": f"Bearer {tokens[session_id]}"},
    ).json()

    # Set up the multigrid controll controller
    controllers[session_id] = MultigridController(
        [],
        watcher_spec.visit,
        watcher_spec.instrument_name,
        session_id,
        murfey_url=_get_murfey_url(),
        demo=True,
        do_transfer=True,
        processing_enabled=not watcher_spec.skip_existing_processing,
        _machine_config=machine_config,
        token=tokens.get(session_id, "token"),
        data_collection_parameters=data_collection_parameters.get(label, {}),
        rsync_restarts=watcher_spec.rsync_restarts,
        visit_end_time=watcher_spec.visit_end_time,
    )
    # Make child directories, if specified
    watcher_spec.source.mkdir(exist_ok=True)
    for d in machine_config.get("create_directories", []):
        (watcher_spec.source / d).mkdir(exist_ok=True)

    # Set up multigrid directory watcher
    watchers[session_id] = MultigridDirWatcher(
        watcher_spec.source,
        machine_config,
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[session_id].subscribe(
        partial(
            controllers[session_id]._start_rsyncer_multigrid,
            destination_overrides=watcher_spec.destination_overrides,
        )
    )
    watchers[session_id].subscribe(
        controllers[session_id]._multigrid_watcher_finalised, final=True
    )
    return {"success": True}


@router.post("/sessions/{session_id}/start_multigrid_watcher")
def start_multigrid_watcher(session_id: MurfeySessionID, process: bool = True):
    if watchers.get(session_id) is None:
        return {"success": False}
    if not process:
        watchers[session_id]._analyse = False
    watchers[session_id].start()
    return {"success": True}


@router.delete("/sessions/{session_id}/multigrid_watcher/{label}")
def stop_multigrid_watcher(session_id: MurfeySessionID, label: str):
    watchers[label].request_stop()
    return {"success": True}


@router.post("/sessions/{session_id}/multigrid_controller/visit_end_time")
def update_multigrid_controller_visit_end_time(
    session_id: MurfeySessionID, end_time: datetime
):
    controllers[session_id].update_visit_time(end_time)
    return {"success": True}


class RsyncerSource(BaseModel):
    source: Path


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


@router.post("/sessions/{session_id}/abandon_controller")
def abandon_controller(session_id: MurfeySessionID):
    controllers[session_id].abandon()
    return {"success": True}


@router.post("/sessions/{session_id}/finalise_rsyncer")
def finalise_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id]._finalise_rsyncer(rsyncer_source.source)
    return {"success": True}


@router.post("/sessions/{session_id}/finalise_session")
def finalise_session(session_id: MurfeySessionID):
    watchers[session_id].request_stop()
    controllers[session_id].finalise()
    return {"success": True}


@router.post("/sessions/{session_id}/restart_rsyncer")
def restart_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id]._restart_rsyncer(rsyncer_source.source)
    return {"success": True}


@router.post("/sessions/{session_id}/flush_skipped_rsyncer")
def flush_skipped_rsyncer(session_id: MurfeySessionID, rsyncer_source: RsyncerSource):
    controllers[session_id].rsync_processes[rsyncer_source.source].flush_skipped()
    return {"success": True}


class ObserverInfo(BaseModel):
    source: str
    num_files_transferred: int
    num_files_in_queue: int
    alive: bool
    stopping: bool
    num_files_skipped: int = 0


@router.get("/sessions/{session_id}/rsyncer_info")
def get_rsyncer_info(session_id: MurfeySessionID) -> list[ObserverInfo]:
    info = []
    for k, v in controllers[session_id].rsync_processes.items():
        info.append(
            ObserverInfo(
                source=str(k),
                num_files_transferred=v._files_transferred,
                num_files_in_queue=v.queue.qsize(),
                alive=v.thread.is_alive(),
                stopping=v._stopping,
                num_files_skipped=len(v._skipped_files),
            )
        )
    return info


@router.get("/sessions/{session_id}/analyser_info")
def get_analyser_info(session_id: MurfeySessionID) -> list[ObserverInfo]:
    info = []
    for k, v in controllers[session_id].analysers.items():
        info.append(
            ObserverInfo(
                source=str(k),
                num_files_transferred=0,
                num_files_in_queue=v.queue.qsize(),
                alive=v.thread.is_alive(),
                stopping=v._stopping,
            )
        )
    return info


class ProcessingParameters(BaseModel):
    gain_ref: str
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
    for k, v in proc_param_block.params.model_dump().items():
        if v is not None:
            data_collection_parameters[proc_param_block.label][k] = v
    if controllers.get(session_id):
        controllers[session_id].data_collection_parameters.update(
            data_collection_parameters[proc_param_block.label]
        )
        for k, v in proc_param_block.params.model_dump().items():
            if v is not None and hasattr(controllers[session_id]._environment, k):
                setattr(controllers[session_id]._environment, k, v)
    return {"success": True}


@router.get(
    "/instruments/{instrument_name}/sessions/{session_id}/possible_gain_references"
)
def get_possible_gain_references(
    instrument_name: str, session_id: MurfeySessionID
) -> list[File]:
    machine_config = requests.get(
        f"{_get_murfey_url()}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=sanitise_nonpath(instrument_name))}",
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

    # Load machine config and other needed properties
    machine_config: dict[str, Any] = requests.get(
        f"{_get_murfey_url()}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=sanitise_nonpath(instrument_name))}",
        headers={"Authorization": f"Bearer {tokens[session_id]}"},
    ).json()

    # Validate that file passed is from the gain reference directory
    gain_ref_dir = machine_config.get("gain_reference_directory", "")
    if not safe_gain_path.startswith(gain_ref_dir):
        raise ValueError(
            "Gain reference file does not originate from the gain reference directory "
            f"{gain_ref_dir!r}"
        )

    # Return the rsync URL if set, otherwise assume you are syncing via Murfey
    rsync_url = urlparse(
        str(machine_config["rsync_url"])
        if machine_config.get("rsync_url", "")
        else _get_murfey_url()
    )
    rsync_module = machine_config.get("rsync_module", "data")
    rsync_path = f"{rsync_url.hostname}::{rsync_module}/{safe_visit_path}/{safe_destination_dir}/{secure_filename(gain_reference.gain_path.name)}"

    # Run rsync subprocess to transfer gain reference
    cmd = [
        "rsync",
        posix_path(Path(safe_gain_path)),
        rsync_path,
    ]
    gain_rsync = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if gain_rsync.returncode:
        logger.warning(
            f"Failed to transfer gain reference file {safe_gain_path!r} to {f'{safe_visit_path}/processing'!r} \n"
            f"Executed the following command: {' '.join(cmd)!r} \n"
            f"Returned the following error: \n"
            f"{gain_rsync.stderr}"
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
            f"{murfey_url.geturl()}{url_path_for('session_control.correlative_router', 'gather_upstream_tiffs', session_id=session_id, visit_name=sanitised_visit_name)}",
            headers={"Authorization": f"Bearer {tokens[session_id]}"},
        ).json()
        or []
    )
    for tiff_path in upstream_tiff_paths:
        tiff_data = requests.get(
            f"{murfey_url.geturl()}{url_path_for('session_control.correlative_router', 'get_tiff', session_id=session_id, visit_name=sanitised_visit_name, tiff_path=tiff_path)}",
            stream=True,
            headers={"Authorization": f"Bearer {tokens[session_id]}"},
        )
        with open(upstream_tiff_info.download_dir / tiff_path, "wb") as utiff:
            for chunk in tiff_data.iter_content(chunk_size=32 * 1024**2):
                utiff.write(chunk)
