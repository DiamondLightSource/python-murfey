from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from ispyb.sqlalchemy import AutoProcProgram as ISPyBAutoProcProgram
from pydantic import BaseModel
from sqlalchemy import func
from sqlmodel import select
from werkzeug.utils import secure_filename

import murfey.server.prometheus as prom
from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.server.api.auth import validate_instrument_token
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
    get_upstream_tiff_dirs,
    remove_session_by_id,
)
from murfey.server.ispyb import DB as ispyb_db
from murfey.server.ispyb import get_all_ongoing_visits
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise
from murfey.util.config import MachineConfig, get_machine_config
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    ProcessingJob,
    RsyncInstance,
    Session,
)
from murfey.util.models import (
    BatchPositionParameters,
    ClientInfo,
    FoilHoleParameters,
    GridSquareParameters,
    RsyncerInfo,
    SearchMapParameters,
    Visit,
)
from murfey.workflows.spa.flush_spa_preprocess import (
    register_foil_hole as _register_foil_hole,
)
from murfey.workflows.spa.flush_spa_preprocess import (
    register_grid_square as _register_grid_square,
)
from murfey.workflows.tomo.tomo_metadata import (
    register_batch_position_in_database,
    register_search_map_in_database,
)

logger = getLogger("murfey.server.api.session_control")

router = APIRouter(
    prefix="/session_control",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Session Control: General"],
)


@router.get("/time")
async def get_current_timestamp():
    return {"timestamp": datetime.now().timestamp()}


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


@router.get("/instruments/{instrument_name}/visits_raw", response_model=List[Visit])
def get_current_visits(instrument_name: str, db=ispyb_db):
    logger.debug(
        f"Received request to look up ongoing visits for {sanitise(instrument_name)}"
    )
    return get_all_ongoing_visits(instrument_name, db)


class SessionInfo(BaseModel):
    session_id: Optional[int] = None
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


@router.post("/visits/{visit_name}")
def register_client_to_visit(visit_name: str, client_info: ClientInfo, db=murfey_db):
    client_env = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_info.id)
    ).one()
    session = db.exec(select(Session).where(Session.id == client_env.session_id)).one()
    if client_env:
        client_env.visit = visit_name
        db.add(client_env)
        db.commit()
    if session:
        session.visit = visit_name
        db.add(session)
        db.commit()
    db.close()
    return client_info


@router.get("/sessions")
async def get_sessions(db=murfey_db):
    sessions = db.exec(select(Session)).all()
    clients = db.exec(select(ClientEnvironment)).all()
    res = []
    for sess in sessions:
        r = {"session": sess, "clients": []}
        for cl in clients:
            if cl.session_id == sess.id:
                r["clients"].append(cl)
        res.append(r)
    return res


@router.delete("/sessions/{session_id}")
def remove_session(session_id: MurfeySessionID, db=murfey_db):
    remove_session_by_id(session_id, db)


@router.post("/sessions/{session_id}/successful_processing")
def register_processing_success_in_ispyb(
    session_id: MurfeySessionID, db=ispyb_db, murfey_db=murfey_db
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


@router.get("/num_movies")
def count_number_of_movies(db=murfey_db) -> Dict[str, int]:
    res = db.exec(
        select(Movie.tag, func.count(Movie.murfey_id)).group_by(Movie.tag)
    ).all()
    return {r[0]: r[1] for r in res}


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


@router.delete("/sessions/{session_id}/rsyncer")
def delete_rsyncer(session_id: int, source: Path, db=murfey_db):
    try:
        rsync_instance = db.exec(
            select(RsyncInstance)
            .where(RsyncInstance.session_id == session_id)
            .where(RsyncInstance.source == str(source))
        ).one()
        db.delete(rsync_instance)
        db.commit()
    except Exception:
        logger.error(
            f"Failed to delete rsyncer for source directory {sanitise(str(source))!r} "
            f"in session {session_id}.",
            exc_info=True,
        )


spa_router = APIRouter(
    prefix="/session_control/spa",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Session Control: SPA"],
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


tomo_router = APIRouter(
    prefix="/session_control/tomo",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Session Control: CryoET"],
)


@tomo_router.post("/sessions/{session_id}/search_map/{sm_name}")
def register_search_map(
    session_id: MurfeySessionID,
    sm_name: str,
    search_map_params: SearchMapParameters,
    db=murfey_db,
):
    return register_search_map_in_database(session_id, sm_name, search_map_params, db)


@tomo_router.post("/sessions/{session_id}/batch_position/{batch_name}")
def register_batch_position(
    session_id: MurfeySessionID,
    batch_name: str,
    batch_params: BatchPositionParameters,
    db=murfey_db,
):
    return register_batch_position_in_database(session_id, batch_name, batch_params, db)


correlative_router = APIRouter(
    prefix="/session_control/correlative",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Session Control: Correlative Imaging"],
)


@correlative_router.get("/sessions/{session_id}/upstream_visits")
async def find_upstream_visits(session_id: MurfeySessionID, db=murfey_db):
    murfey_session = db.exec(select(Session).where(Session.id == session_id)).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    upstream_visits = {}
    # Iterates through provided upstream directories
    for p in machine_config.upstream_data_directories:
        # Looks for visit name in file path
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = v / machine_config.processed_directory_name
    return upstream_visits


@correlative_router.get("/visits/{visit_name}/{session_id}/upstream_tiff_paths")
async def gather_upstream_tiffs(visit_name: str, session_id: int, db=murfey_db):
    """
    Looks for TIFF files associated with the current session in the permitted storage
    servers, and returns their relative file paths as a list.
    """
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    upstream_tiff_paths = []
    tiff_dirs = get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
        for f in tiff_dir.glob("**/*.tif"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
    return upstream_tiff_paths


@correlative_router.get(
    "/visits/{visit_name}/{session_id}/upstream_tiff/{tiff_path:path}"
)
async def get_tiff(visit_name: str, session_id: int, tiff_path: str, db=murfey_db):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    tiff_dirs = get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None

    tiff_path = "/".join(secure_filename(p) for p in tiff_path.split("/"))
    for tiff_dir in tiff_dirs:
        test_path = tiff_dir / tiff_path
        if test_path.is_file():
            break
    else:
        logger.warning(f"TIFF {tiff_path} not found")
        return None

    return FileResponse(path=test_path)
