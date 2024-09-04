import secrets
import time
from logging import getLogger
from pathlib import Path
from typing import Annotated, Dict, List, Optional, Union
from urllib.parse import urlparse

import procrunner
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
from murfey.util.instrument_models import MultigridWatcherSpec
from murfey.util.models import File, Token

logger = getLogger("murfey.instrument_server.api")

watchers: Dict[Union[str, int], MultigridDirWatcher] = {}
rsyncers: Dict[str, RSyncer] = {}
controllers = {}
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


def validate_token(token: Annotated[str, Depends(oauth2_scheme)]):
    try:
        decoded_data = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[config["Murfey"].get("auth_algorithm", "HS256")],
        )
        if not decoded_data.get("timestamp") == launch_time:
            raise JWTError
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return None


router = APIRouter(dependencies=[Depends(validate_token)])
handshake_router = APIRouter()


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


@handshake_router.post("/token")
async def token_handshake(token: Token):
    handshake_success = await murfey_server_handshake(token.access_token)
    if handshake_success:
        return Token(access_token=encoded_jwt, token_type="bearer")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Handshake failure between Murfey servers",
    )


@router.post("/sessions/{session_id}/multigrid_watcher")
def start_multigrid_watcher(session_id: int, watcher_spec: MultigridWatcherSpec):
    label = watcher_spec.label
    controllers[label] = MultigridController(
        [],
        watcher_spec.visit,
        session_id,
        murfey_url=_get_murfey_url(),
        demo=True,
        do_transfer=False,
        processing_enabled=not watcher_spec.skip_existing_processing,
        _machine_config=watcher_spec.configuration.dict(),
        token=tokens.get("token", ""),
        data_collection_parameters=data_collection_parameters.get(label, {}),
    )
    watcher_spec.source.mkdir(exist_ok=True)
    machine_config = requests.get(
        f"{_get_murfey_url()}/machine",
        headers={"Authorization": f"Bearer {tokens['token']}"},
    ).json()
    for d in machine_config.get("create_directories", {}).values():
        (watcher_spec.source / d).mkdir(exist_ok=True)
    watchers[label] = MultigridDirWatcher(
        watcher_spec.source,
        watcher_spec.configuration.dict(),
        skip_existing_processing=watcher_spec.skip_existing_processing,
    )
    watchers[label].subscribe(controllers[label]._start_rsyncer_multigrid)
    watchers[label].start()
    return {"success": True}


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
    return {"success": True}


@router.post("/sessions/{session_id}/remove_rsyncer")
def remove_rsyncer(session_id: int, rsyncer_source: RsyncerSource):
    controllers[rsyncer_source.label]._request_watcher_stop(rsyncer_source.source)
    controllers[rsyncer_source.label].rsync_processes[
        rsyncer_source.source
    ]._stopping = True
    controllers[rsyncer_source.label].rsync_processes[
        rsyncer_source.source
    ]._halt_thread = True
    controllers[rsyncer_source.label].rsync_processes[rsyncer_source.source].queue.put(
        None, block=False
    )
    return {"success": True}


@router.post("/sessions/{session_id}/finalise_rsyncer")
def finalise_rsyncer(session_id: int, rsyncer_source: RsyncerSource):
    controllers[rsyncer_source.label]._finalise_rsyncer(rsyncer_source.source)
    return {"success": True}


@router.post("/sessions/{session_id}/restart_rsyncer")
def restart_rsyncer(session_id: int, rsyncer_source: RsyncerSource):
    controllers[rsyncer_source.label]._restart_rsyncer(rsyncer_source.source)
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


@router.post("/processing_parameters")
def register_processing_parameters(proc_param_block: ProcessingParameterBlock):
    data_collection_parameters[proc_param_block.label] = {}
    for k, v in proc_param_block.params.dict().items():
        data_collection_parameters[proc_param_block.label][k] = v
    return {"success": True}


@router.get("/possible_gain_references")
def get_possible_gain_references() -> List[File]:
    machine_config = requests.get(
        f"{_get_murfey_url()}/machine",
        headers={"Authorization": f"Bearer {tokens['token']}"},
    ).json()
    candidates = []
    for gf in Path(machine_config["gain_reference_directory"]).glob("**/*"):
        if gf.is_file():
            candidates.append(
                File(
                    name=gf.name,
                    description="",
                    size=gf.stat().st_size / 1e6,
                    timestamp=gf.stat().st_mtime,
                    full_path=str(gf),
                )
            )
    candidates.sort(key=lambda x: x.timestamp, reverse=True)
    return candidates


class GainReference(BaseModel):
    gain_path: Path
    visit_path: str
    gain_destination_dir: str = "processing"


@router.post("/upload_gain_reference")
def upload_gain_reference(gain_reference: GainReference):
    cmd = [
        "rsync",
        str(gain_reference.gain_path),
        f"{urlparse(_get_murfey_url(), allow_fragments=False).hostname}::{gain_reference.visit_path}/{gain_reference.gain_destination_dir}/{secure_filename(gain_reference.gain_path.name)}",
    ]
    gain_rsync = procrunner.run(cmd)
    if gain_rsync.returncode:
        logger.warning(
            f"Gain reference file {gain_reference.gain_path} was not successfully transferred to {gain_reference.visit_path}/processing"
        )
        return {"success": False}
    return {"success": True}


class UpstreamTiffInfo(BaseModel):
    download_dir: Path


@router.post("/visits/{visit_name}/upstream_tiff_data_request")
def gather_upstream_tiffs(visit_name: str, upstream_tiff_info: UpstreamTiffInfo):
    murfey_url = urlparse(_get_murfey_url(), allow_fragments=False)
    upstream_tiff_info.download_dir.mkdir(exist_ok=True)
    upstream_tiff_paths = (
        requests.get(
            f"{murfey_url.geturl()}/visits/{visit_name}/upstream_tiff_paths",
            headers={"Authorization": f"Bearer {tokens['token']}"},
        ).json()
        or []
    )
    for tiff_path in upstream_tiff_paths:
        tiff_data = requests.get(
            f"{murfey_url.geturl()}/visits/{visit_name}/upstream_tiff/{tiff_path}",
            stream=True,
            headers={"Authorization": f"Bearer {tokens['token']}"},
        )
        with open(upstream_tiff_info.download_dir / tiff_path, "wb") as utiff:
            for chunk in tiff_data.iter_content(chunk_size=32 * 1024**2):
                utiff.write(chunk)