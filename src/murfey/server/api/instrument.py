from __future__ import annotations

import asyncio
import datetime
import logging
from pathlib import Path
from typing import Annotated, List, Optional
from urllib.parse import quote

import aiohttp
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select
from werkzeug.utils import secure_filename

from murfey.server.api.auth import MurfeyInstrumentNameFrontend as MurfeyInstrumentName
from murfey.server.api.auth import MurfeySessionIDFrontend as MurfeySessionID
from murfey.server.api.auth import (
    create_access_token,
    instrument_server_tokens,
    oauth2_scheme,
    validate_token,
)
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise, secure_path
from murfey.util.api import url_path_for
from murfey.util.config import get_machine_config
from murfey.util.db import RsyncInstance, Session, SessionProcessingParameters
from murfey.util.models import File, MultigridWatcherSetup

# Create APIRouter class object
router = APIRouter(
    prefix="/instrument_server",
    dependencies=[Depends(validate_token)],
    tags=["Instrument Server"],
)

log = logging.getLogger("murfey.server.instrument")

lock = asyncio.Lock()


@router.post(
    "/instruments/{instrument_name}/sessions/{session_id}/activate_instrument_server"
)
async def activate_instrument_server_for_session(
    instrument_name: MurfeyInstrumentName,
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
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'token_handshake_for_session', session_id=session_id)}",
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


@router.get("/instruments/{instrument_name}/sessions/{session_id}/active")
async def check_if_session_is_active(
    instrument_name: MurfeyInstrumentName, session_id: int
):
    if instrument_server_tokens.get(session_id) is None:
        return {"active": False}
    async with lock:
        async with aiohttp.ClientSession() as clientsession:
            machine_config = get_machine_config(instrument_name=instrument_name)[
                instrument_name
            ]
            async with clientsession.get(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'check_token', session_id=session_id)}",
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as response:
                return {"active": response.status == 200}


@router.post("/sessions/{session_id}/multigrid_watcher")
async def setup_multigrid_watcher(
    session_id: MurfeySessionID, watcher_spec: MultigridWatcherSetup, db=murfey_db
):
    data = {}
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    instrument_name = session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        session = db.exec(select(Session).where(Session.id == session_id)).one()
        visit = session.visit
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'setup_multigrid_watcher', session_id=session_id)}",
                json={
                    "source": str(secure_path(watcher_spec.source / visit)),
                    "visit": visit,
                    "label": visit,
                    "instrument_name": instrument_name,
                    "skip_existing_processing": watcher_spec.skip_existing_processing,
                    "destination_overrides": {
                        str(k): v for k, v in watcher_spec.destination_overrides.items()
                    },
                    "rsync_restarts": watcher_spec.rsync_restarts,
                    "visit_end_time": (
                        str(session.visit_end_time) if session.visit_end_time else None
                    ),
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/start_multigrid_watcher")
async def start_multigrid_watcher(session_id: MurfeySessionID, db=murfey_db):
    data = {}
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    process = session.process
    instrument_name = session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        log.debug(
            f"Submitting request to start multigrid watcher for session {session_id} "
            f"with processing {('enabled' if process else 'disabled')}"
        )
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'start_multigrid_watcher', session_id=session_id)}?process={'true' if process else 'false'}",
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    log.debug(f"Received response: {data}")
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
    session = db.exec(select(Session).where(Session.id == session_id)).one()

    session_processing_parameters = SessionProcessingParameters(
        session_id=session_id,
        dose_per_frame=proc_params.dose_per_frame,
        gain_ref=session.current_gain_ref,
        symmetry=proc_params.symmetry,
        eer_fractionation=proc_params.eer_fractionation,
    )
    db.add(session_processing_parameters)
    db.commit()

    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        label = db.exec(select(Session).where(Session.id == session_id)).one().name
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'register_processing_parameters', session_id=session_id)}",
                json={
                    "label": label,
                    "params": {
                        "dose_per_frame": proc_params.dose_per_frame,
                        "symmetry": proc_params.symmetry,
                        "eer_fractionation": proc_params.eer_fractionation,
                        "gain_ref": session.current_gain_ref,
                    },
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.get("/instruments/{instrument_name}/instrument_server")
async def check_instrument_server(instrument_name: MurfeyInstrumentName):
    data = None
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.get(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'health')}",
            ) as resp:
                data = await resp.json()
    return data


@router.get(
    "/instruments/{instrument_name}/sessions/{session_id}/possible_gain_references"
)
async def get_possible_gain_references(
    instrument_name: MurfeyInstrumentName, session_id: MurfeySessionID
) -> List[File]:
    data = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with lock:
            token = instrument_server_tokens[session_id]["access_token"]
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.get(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'get_possible_gain_references', instrument_name=sanitise(instrument_name), session_id=session_id)}",
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
    visit_path = f"{datetime.datetime.now().year}/{visit}"
    data = {}
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'upload_gain_reference', instrument_name=instrument_name, session_id=session_id)}",
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
            async with aiohttp.ClientSession() as clientsession:
                async with clientsession.post(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'gather_upstream_tiffs', visit_name=secure_filename(visit_name), session_id=session_id)}",
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
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'stop_rsyncer', session_id=session_id)}",
                json={
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
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'finalise_rsyncer', session_id=session_id)}",
                json={
                    "source": str(secure_path(Path(rsyncer_source.source))),
                },
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/finalise_session")
async def finalise_session(session_id: MurfeySessionID, db=murfey_db):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'finalise_session', session_id=session_id)}",
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/multigrid_controller/visit_end_time")
async def update_visit_end_time(
    session_id: MurfeySessionID, end_time: datetime.datetime, db=murfey_db
):
    # Load data for session
    session_entry = db.exec(select(Session).where(Session.id == session_id)).one()
    instrument_name = session_entry.instrument_name

    # Update visit end time in database
    session_entry.visit_end_time = end_time
    db.add(session_entry)
    db.commit()

    # Update the multigrid controller
    data = {}
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api.router', 'update_multigrid_controller_visit_end_time', session_id=session_id)}?end_time={quote(end_time.isoformat())}",
                headers={
                    "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                },
            ) as resp:
                data = await resp.json()
    return data


@router.post("/sessions/{session_id}/abandon_session")
async def abandon_session(session_id: MurfeySessionID, db=murfey_db):
    data = {}
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.instrument_server_url:
        async with aiohttp.ClientSession() as clientsession:
            async with clientsession.post(
                f"{machine_config.instrument_server_url}{url_path_for('api_router', 'abandon_controller', session_id=session_id)}",
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
            async with aiohttp.ClientSession() as clientsession:
                async with clientsession.post(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'remove_rsyncer', session_id=session_id)}",
                    json={
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
            async with aiohttp.ClientSession() as clientsession:
                async with clientsession.post(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'restart_rsyncer', session_id=session_id)}",
                    json={
                        "source": str(secure_path(Path(rsyncer_source.source))),
                    },
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    data = await resp.json()
    return data


@router.post("/sessions/{session_id}/flush_skipped_rsyncer")
async def flush_skipped_rsyncer(
    session_id: MurfeySessionID, rsyncer_source: RsyncerSource, db=murfey_db
):
    # Load data for session
    session_entry = db.exec(select(Session).where(Session.id == session_id)).one()
    instrument_name = session_entry.instrument_name

    # Define a new visit end time that's slightly ahead of current time
    new_end_time = datetime.datetime.now().replace(
        second=0, microsecond=0
    ) + datetime.timedelta(minutes=5)
    # Update the stored visit end time if the new one exceeds it
    if session_entry.visit_end_time:
        if new_end_time > session_entry.visit_end_time:
            session_entry.visit_end_time = new_end_time
            db.add(session_entry)
            db.commit()

    # Send request to flush rsyncer
    data: dict = {}
    update_result: dict = {}
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if isinstance(session_id, int):
        if machine_config.instrument_server_url:
            async with aiohttp.ClientSession() as clientsession:
                # Send request to instrument server to update multigrid controller
                async with clientsession.post(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'update_multigrid_controller_visit_end_time', session_id=session_id)}?end_time={quote(session_entry.visit_end_time.isoformat())}",
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    update_result = await resp.json()
                if not update_result.get("success", False):
                    return {"success": False}
                # Send request to flush the rsyncer
                async with clientsession.post(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'flush_skipped_rsyncer', session_id=session_id)}",
                    json={
                        "source": str(secure_path(Path(rsyncer_source.source))),
                    },
                    headers={
                        "Authorization": f"Bearer {instrument_server_tokens[session_id]['access_token']}"
                    },
                ) as resp:
                    data = await resp.json()
    return data


class RSyncerInfo(BaseModel):
    source: str
    num_files_transferred: int
    num_files_in_queue: int
    num_files_to_analyse: int
    alive: bool
    stopping: bool
    analyser_alive: bool
    analyser_stopping: bool
    destination: str
    tag: str
    files_transferred: int
    files_counted: int
    transferring: bool
    session_id: int
    num_files_skipped: int = 0


@router.get("/instruments/{instrument_name}/sessions/{session_id}/rsyncer_info")
async def get_rsyncer_info(
    instrument_name: MurfeyInstrumentName, session_id: MurfeySessionID, db=murfey_db
) -> List[RSyncerInfo]:
    rsyncer_list = []
    analyser_list = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.session_id == session_id)
    ).all()
    if machine_config.instrument_server_url:
        try:
            async with lock:
                token = instrument_server_tokens[session_id]["access_token"]
            async with aiohttp.ClientSession() as clientsession:
                async with clientsession.get(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'get_rsyncer_info', session_id=session_id)}",
                    headers={"Authorization": f"Bearer {token}"},
                ) as resp:
                    if resp.status == 200:
                        rsyncer_list = await resp.json()
                    else:
                        rsyncer_list = []
        except KeyError:
            rsyncer_list = []
        except Exception:
            log.warning(
                "Exception encountered gathering rsyncer info from the instrument server",
                exc_info=True,
            )

        try:
            async with lock:
                token = instrument_server_tokens[session_id]["access_token"]
            async with aiohttp.ClientSession() as clientsession:
                async with clientsession.get(
                    f"{machine_config.instrument_server_url}{url_path_for('api.router', 'get_analyser_info', session_id=session_id)}",
                    headers={"Authorization": f"Bearer {token}"},
                ) as resp:
                    if resp.status == 200:
                        analyser_list = await resp.json()
                    else:
                        analyser_list = []
        except KeyError:
            analyser_list = []
        except Exception:
            log.warning(
                "Exception encountered gathering analyser info from the instrument server",
                exc_info=True,
            )

    combined_data = []
    rsyncer_source_lookup = {d["source"]: d for d in rsyncer_list}
    analyser_source_lookup = {d["source"]: d for d in analyser_list}
    for ri in rsync_instances:
        rsync_data = rsyncer_source_lookup.get(ri.source, {})
        analyser_data = analyser_source_lookup.get(ri.source, {})
        combined_data.append(
            RSyncerInfo(
                source=ri.source,
                num_files_transferred=rsync_data.get("num_files_transferred", 0),
                num_files_in_queue=rsync_data.get("num_files_in_queue", 0),
                num_files_to_analyse=analyser_data.get("num_files_in_queue", 0),
                alive=rsync_data.get("alive", False),
                stopping=rsync_data.get("stopping", True),
                analyser_alive=analyser_data.get("alive", False),
                analyser_stopping=analyser_data.get("stopping", True),
                destination=ri.destination,
                tag=ri.tag,
                files_transferred=ri.files_transferred,
                files_counted=ri.files_counted,
                transferring=ri.transferring,
                session_id=session_id,
                num_files_skipped=rsync_data.get("num_files_skipped", 0),
            )
        )
    return combined_data
