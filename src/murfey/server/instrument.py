from __future__ import annotations

import datetime
import logging
from pathlib import Path
from typing import List, Optional

import aiohttp
from fastapi import APIRouter
from pydantic import BaseModel
from sqlmodel import select

from murfey.server.auth import instrument_server_tokens
from murfey.server.auth.api import create_access_token
from murfey.server.config import get_machine_config
from murfey.server.murfey_db import murfey_db
from murfey.util.db import Session
from murfey.util.models import File, MultigridWatcherSetup

# Create APIRouter class object
router = APIRouter()
machine_config = get_machine_config()

log = logging.getLogger("murfey.server.instrument")


@router.post("/activate_instrument_server")
async def activate_instrument_server():
    log.info("Activating instrument server")
    timestamp = datetime.datetime.now().timestamp()
    token = create_access_token({"timestamp": timestamp})
    instrument_server_tokens[timestamp] = None
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{machine_config.instrument_server_url}/token",
            json={"access_token": token, "token_type": "bearer"},
        ) as response:
            success = response.status == 200
            instrument_server_token = await response.json()
            instrument_server_tokens[timestamp] = instrument_server_token
    log.info("Handshake successful" if success else "Handshake unsuccessful")
    return success


@router.get("/instrument_name/")
def get_instrument_name():
    return machine_config.instrument_name


@router.post("/sessions/{session_id}/multigrid_watcher")
async def start_multigrid_watcher(
    session_id: int, watcher_spec: MultigridWatcherSetup, db=murfey_db
):
    if machine_config.instrument_server_url:
        session = db.exec(select(Session).where(Session.id == session_id)).one()
        visit = session.visit
        label = session.name
        _config = {
            "acquisition_software": machine_config.acquisition_software,
            "calibrations": machine_config.calibrations,
            "data_directories": {
                str(k): v for k, v in machine_config.data_directories.items()
            },
            "rsync_basepath": str(machine_config.rsync_basepath),
            "murfey_db_credentials": machine_config.murfey_db_credentials,
            "visit": visit,
            "crypto_key": "",
        }
        async with aiohttp.ClientSession() as session:
            log.info(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/multigrid_watcher"
            )
            async with session.post(
                f"{machine_config.instrument_server_url}/sessions/{session_id}/multigrid_watcher",
                json={
                    "source": str(watcher_spec.source / visit),
                    "visit": visit,
                    "configuration": _config,
                    "label": label,
                    "skip_existing_processing": watcher_spec.skip_existing_processing,
                },
                headers={
                    "Authorization": f"Bearer {list(instrument_server_tokens.values())[0]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
                log.info(resp.status)
    return data


class ProvidedProcessingParameters(BaseModel):
    dose_per_frame: float
    extract_downscale: bool = True
    particle_diameter: Optional[float] = None
    symmetry: str = "C1"
    eer_fractionation: int = 20


@router.post("/sessions/{session_id}/provided_processing_parameters")
async def pass_proc_params_to_instrument_server(
    session_id: int, proc_params: ProvidedProcessingParameters, db=murfey_db
):
    if machine_config.instrument_server_url:
        label = db.exec(select(Session).where(Session.id == session_id)).one().name
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/processing_parameters",
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
                    "Authorization": f"Bearer {list(instrument_server_tokens.values())[0]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.get("/instrument_server")
async def check_instrument_server():
    data = None
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{machine_config.instrument_server_url}/health",
                headers={
                    "Authorization": f"Bearer {list(instrument_server_tokens.values())[0]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.get("/possible_gain_references")
async def get_possible_gain_references() -> List[File]:
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{machine_config.instrument_server_url}/possible_gain_references",
                headers={
                    "Authorization": f"Bearer {list(instrument_server_tokens.values())[0]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


class GainReferenceRequest(BaseModel):
    gain_path: Path


@router.post("/sessions/{session_id}/upload_gain_reference")
async def request_gain_reference_upload(
    session_id: int, gain_reference_request: GainReferenceRequest, db=murfey_db
):
    visit = db.exec(select(Session).where(Session.id == session_id)).one().visit
    visit_path = f"{machine_config.rsync_module or 'data'}/{datetime.datetime.now().year}/{visit}"
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{machine_config.instrument_server_url}/upload_gain_reference",
                json={
                    "gain_path": str(gain_reference_request.gain_path),
                    "visit_path": visit_path,
                    "gain_destination_dir": machine_config.gain_directory_name,
                },
                headers={
                    "Authorization": f"Bearer {list(instrument_server_tokens.values())[0]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data
