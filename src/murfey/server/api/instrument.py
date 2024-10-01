from __future__ import annotations

import asyncio
import datetime
import logging
from pathlib import Path
from typing import Annotated, List, Optional

import aiohttp
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select
from werkzeug.utils import secure_filename

from murfey.server import sanitise
from murfey.server.api import MurfeySessionID
from murfey.server.api.auth import (
    create_access_token,
    instrument_server_tokens,
    oauth2_scheme,
    validate_token,
)
from murfey.server.murfey_db import murfey_db
from murfey.util import secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import Session
from murfey.util.models import File, MultigridWatcherSetup

# Create APIRouter class object
router = APIRouter(dependencies=[Depends(validate_token)])

log = logging.getLogger("murfey.server.instrument")

lock = asyncio.Lock()


@router.post(
    "/instruments/{instrument_name}/sessions/{session_id}/activate_instrument_server"
)
async def activate_instrument_server_for_session(
    instrument_name: str,
    session_id: int,
    token_in: Annotated[str, Depends(oauth2_scheme)],
    db=murfey_db,
):
    log.info(
        f"Activating instrument server for session {int(sanitise(str(session_id)))}"
    )
    if not session_id > 0:
        log.warning("Invalid session ID")
        return False
    visit_name = db.exec(select(Session).where(Session.id == session_id)).one().visit
    timestamp = datetime.datetime.now().timestamp()
    token = create_access_token(
        {"timestamp": timestamp, "session": session_id, "visit": visit_name},
        token=token_in,
    )
    async with lock:
        instrument_server_tokens[session_id] = {}
        machine_config = get_machine_config(instrument_name=instrument_name)[
            instrument_name
        ]
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{int(sanitise(str(session_id)))}/token",
                json={"access_token": token, "token_type": "bearer"},
            ) as response:
                success = response.status == 200
                instrument_server_token = await response.json()
                instrument_server_tokens[session_id] = instrument_server_token
    if success:
        log.info("Handshake successful")
    else:
        log.warning("Handshake unsuccessful")
    return success


@router.post("/sessions/{session_id}/multigrid_watcher")
async def start_multigrid_watcher(
    session_id: MurfeySessionID, watcher_spec: MultigridWatcherSetup, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        session = db.exec(select(Session).where(Session.id == session_id)).one()
        visit = session.visit
        _config = {
            "acquisition_software": machine_config.acquisition_software,
            "calibrations": machine_config.calibrations,
            "data_directories": {
                str(k): v for k, v in machine_config.data_directories.items()
            },
            "rsync_basepath": str(machine_config.rsync_basepath),
            "visit": visit,
            "default_model": str(machine_config.default_model),
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/multigrid_watcher",
                json={
                    "source": str(secure_path(watcher_spec.source / visit)),
                    "visit": visit,
                    "configuration": _config,
                    "label": visit,
                    "instrument_name": instrument_name,
                    "skip_existing_processing": watcher_spec.skip_existing_processing,
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


class ProvidedProcessingParameters(BaseModel):
    dose_per_frame: float
    extract_downscale: bool = True
    particle_diameter: Optional[float] = None
    symmetry: str = "C1"
    eer_fractionation: int = 20


@router.post("/sessions/{session_id}/provided_processing_parameters")
async def pass_proc_params_to_instrument_server(
    session_id: MurfeySessionID, proc_params: ProvidedProcessingParameters, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        label = db.exec(select(Session).where(Session.id == session_id)).one().name
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/processing_parameters",
                json={
                    "label": label,
                    "params": {
                        "dose_per_frame": proc_params.dose_per_frame,
                        "extract_downscale": proc_params.extract_downscale,
                        "particle_diameter": proc_params.particle_diameter,
                        "symmetry": proc_params.symmetry,
                        "eer_fractionation": proc_params.eer_fractionation,
                    },
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.get("/instruments/{instrument_name}/instrument_server")
async def check_instrument_server(instrument_name: str):
    data = None
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{machine_config.instrument_server_url}/health",
            ) as resp:
                data = await resp.json()
    return data


@router.get(
    "/instruments/{instrument_name}/sessions/{session_id}/possible_gain_references"
)
async def get_possible_gain_references(
    instrument_name: str, session_id: MurfeySessionID
) -> List[File]:
    data = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with lock:
            token = instrument_server_tokens[session_id]["access_token"]
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{machine_config.instrument_server_url}/instruments/{sanitise(instrument_name)}/sessions/{sanitise(str(session_id))}/possible_gain_references",
                headers={"Authorization": f"Bearer {token}"},
            ) as resp:
                data = await resp.json()
    return data


class GainReferenceRequest(BaseModel):
    gain_path: Path


@router.post("/sessions/{session_id}/upload_gain_reference")
async def request_gain_reference_upload(
    session_id: MurfeySessionID,
    gain_reference_request: GainReferenceRequest,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    visit = db.exec(select(Session).where(Session.id == session_id)).one().visit
    visit_path = f"{machine_config.rsync_module or 'data'}/{datetime.datetime.now().year}/{visit}"
    data = {}
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/upload_gain_reference",
                json={
                    "gain_path": str(gain_reference_request.gain_path),
                    "visit_path": visit_path,
                    "gain_destination_dir": machine_config.gain_directory_name,
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/visits/{visit_name}/{session_id}/upstream_tiff_data_request")
async def request_upstream_tiff_data_download(
    visit_name: str, session_id: MurfeySessionID, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.upstream_data_download_directory:
        download_dir = str(
            machine_config.upstream_data_download_directory
            / secure_filename(visit_name)
        )
        if machine_config.instrument_server_url:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{machine_config.instrument_server_url}/visits/{secure_filename(visit_name)}/sessions/{sanitise(str(session_id))}/upstream_tiff_data_request",
                    json={"download_dir": download_dir},
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    data = await resp.json()
    return data


class RsyncerSource(BaseModel):
    source: str


@router.post("/sessions/{session_id}/stop_rsyncer")
async def stop_rsyncer(
    session_id: MurfeySessionID, rsyncer_source: RsyncerSource, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/stop_rsyncer",
                json={
                    "label": session_id,
                    "source": str(secure_path(Path(rsyncer_source.source))),
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/finalise_rsyncer")
async def finalise_rsyncer(
    session_id: MurfeySessionID, rsyncer_source: RsyncerSource, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/finalise_rsyncer",
                json={
                    "label": session_id,
                    "source": str(secure_path(Path(rsyncer_source.source))),
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/remove_rsyncer")
async def remove_rsyncer(
    session_id: MurfeySessionID, rsyncer_source: RsyncerSource, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if isinstance(session_id, int):
        if machine_config.instrument_server_url:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{machine_config.instrument_server_url}/sessions/{session_id}/remove_rsyncer",
                    json={
                        "label": session_id,
                        "source": str(secure_path(Path(rsyncer_source.source))),
                    },
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    data = await resp.json()
    return data


@router.post("/sessions/{session_id}/restart_rsyncer")
async def restart_rsyncer(
    session_id: MurfeySessionID, rsyncer_source: RsyncerSource, db=murfey_db
):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if isinstance(session_id, int):
        if machine_config.instrument_server_url:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{machine_config.instrument_server_url}/sessions/{session_id}/restart_rsyncer",
                    json={
                        "label": session_id,
                        "source": str(secure_path(Path(rsyncer_source.source))),
                    },
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    data = await resp.json()
    return data
