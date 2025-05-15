from logging import getLogger
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends
from ispyb.sqlalchemy import AutoProcProgram as ISPyBAutoProcProgram
from pydantic import BaseModel
from sqlmodel import select

import murfey.server.prometheus as prom

try:
    from murfey.server.ispyb import DB
except ImportError:
    DB = None
from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionID, validate_token
from murfey.server.api.shared import get_foil_hole as _get_foil_hole
from murfey.server.api.shared import (
    get_foil_holes_from_grid_square as _get_foil_holes_from_grid_square,
)
from murfey.server.api.shared import get_grid_squares as _get_grid_squares
from murfey.server.api.shared import (
    get_grid_squares_from_dcg as _get_grid_squares_from_dcg,
)
from murfey.server.api.shared import (
    get_machine_config_for_instrument,
    remove_session_by_id,
)
from murfey.server.murfey_db import murfey_db
from murfey.util.config import MachineConfig
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    ProcessingJob,
    RsyncInstance,
    Session,
)
from murfey.util.models import FoilHoleParameters, GridSquareParameters, RsyncerInfo
from murfey.workflows.spa.flush_spa_preprocess import (
    register_foil_hole as _register_foil_hole,
)
from murfey.workflows.spa.flush_spa_preprocess import (
    register_grid_square as _register_grid_square,
)

logger = getLogger("murfey.server.api.session_control")

router = APIRouter(
    prefix="/session_control",
    dependencies=[Depends(validate_token)],
    tags=["session control"],
)


@router.get("/instruments/{instrument_name}/machine")
def machine_info_by_instrument(instrument_name: str) -> Optional[MachineConfig]:
    return get_machine_config_for_instrument(instrument_name)


@router.get("/new_client_id/")
async def new_client_id(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    if not clients:
        return {"new_id": 0}
    sorted_ids = sorted([c.client_id for c in clients])
    return {"new_id": sorted_ids[-1] + 1}


class SessionInfo(BaseModel):
    session_id: Optional[int]
    session_name: str = ""
    rescale: bool = True


@router.post("/instruments/{instrument_name}/clients/{client_id}/session")
def link_client_to_session(
    instrument_name: str, client_id: int, sess: SessionInfo, db=murfey_db
):
    sid = sess.session_id
    if sid is None:
        s = Session(name=sess.session_name, instrument_name=instrument_name)
        db.add(s)
        db.commit()
        sid = s.id
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    client.session_id = sid
    db.add(client)
    db.commit()
    db.close()
    return sid


@router.delete("/sessions/{session_id}")
def remove_session(session_id: MurfeySessionID, db=murfey_db):
    remove_session_by_id(session_id, db)


@router.post("/sessions/{session_id}/successful_processing")
def register_processing_success_in_ispyb(
    session_id: MurfeySessionID, db=DB, murfey_db=murfey_db
):
    collected_ids = murfey_db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
    ).all()
    appids = [c[3].id for c in collected_ids]
    if _transport_object:
        if db is not None:
            apps = db.query(ISPyBAutoProcProgram).filter(
                ISPyBAutoProcProgram.autoProcProgramId.in_(appids)
            )
            for updated in apps:
                updated.processingStatus = True
                _transport_object.do_update_processing_status(updated)


class PostInfo(BaseModel):
    url: str
    data: dict


@router.post("/instruments/{instrument_name}/failed_client_post")
def failed_client_post(instrument_name: str, post_info: PostInfo):
    zocalo_message = {
        "register": "failed_client_post",
        "url": post_info.url,
        "json": post_info.data,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


@router.post("/sessions/{session_id}/rsyncer")
def register_rsyncer(session_id: int, rsyncer_info: RsyncerInfo, db=murfey_db):
    visit_name = db.exec(select(Session).where(Session.id == session_id)).one().visit
    rsync_instance = RsyncInstance(
        source=rsyncer_info.source,
        session_id=rsyncer_info.session_id,
        transferring=rsyncer_info.transferring,
        destination=rsyncer_info.destination,
        tag=rsyncer_info.tag,
    )
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(0)
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(
        0
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    return rsyncer_info


@router.get("/sessions/{session_id}/rsyncers", response_model=List[RsyncInstance])
def get_rsyncers_for_session(session_id: MurfeySessionID, db=murfey_db):
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.session_id == session_id)
    )
    return rsync_instances.all()


class RsyncerSource(BaseModel):
    source: str


@router.post("/sessions/{session_id}/rsyncer_stopped")
def register_stopped_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = False
    db.add(rsyncer)
    db.commit()


@router.post("/sessions/{session_id}/rsyncer_started")
def register_restarted_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = True
    db.add(rsyncer)
    db.commit()


spa_router = APIRouter(
    prefix="/session_control/spa",
    dependencies=[Depends(validate_token)],
    tags=["session info for SPA"],
)


@spa_router.get("/sessions/{session_id}/grid_squares")
def get_grid_squares(session_id: MurfeySessionID, db=murfey_db):
    return _get_grid_squares(session_id, db)


@spa_router.get("/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares")
def get_grid_squares_from_dcg(
    session_id: MurfeySessionID, dcgid: int, db=murfey_db
) -> List[GridSquare]:
    return _get_grid_squares_from_dcg(session_id, dcgid, db)


@spa_router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{gsid}/foil_holes"
)
def get_foil_holes_from_grid_square(
    session_id: MurfeySessionID, dcgid: int, gsid: int, db=murfey_db
) -> List[FoilHole]:
    return _get_foil_holes_from_grid_square(session_id, dcgid, gsid, db)


@spa_router.get("/sessions/{session_id}/foil_hole/{fh_name}")
def get_foil_hole(
    session_id: MurfeySessionID, fh_name: int, db=murfey_db
) -> Dict[str, int]:
    return _get_foil_hole(session_id, fh_name, db)


@spa_router.post("/sessions/{session_id}/grid_square/{gsid}")
def register_grid_square(
    session_id: MurfeySessionID,
    gsid: int,
    grid_square_params: GridSquareParameters,
    db=murfey_db,
):
    return _register_grid_square(session_id, gsid, grid_square_params, db)


@spa_router.post("/sessions/{session_id}/grid_square/{gs_name}/foil_hole")
def register_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    db=murfey_db,
):
    logger.info(
        f"Registering foil hole {foil_hole_params.name} with position {(foil_hole_params.x_location, foil_hole_params.y_location)}"
    )
    return _register_foil_hole(session_id, gs_name, foil_hole_params, db)
