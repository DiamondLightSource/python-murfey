from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlmodel import select

import murfey.server.api.websocket as ws
from murfey.server import _transport_object
from murfey.server.api import templates
from murfey.server.api.auth import (
    MurfeyInstrumentNameFrontend as MurfeyInstrumentName,
    MurfeySessionIDFrontend as MurfeySessionID,
    validate_token,
)
from murfey.server.api.session_shared import (
    find_upstream_visits as _find_upstream_visits,
    gather_upstream_files as _gather_upstream_files,
    gather_upstream_tiffs as _gather_upstream_tiffs,
    get_foil_hole as _get_foil_hole,
    get_foil_holes_from_grid_square as _get_foil_holes_from_grid_square,
    get_grid_squares as _get_grid_squares,
    get_grid_squares_from_dcg as _get_grid_squares_from_dcg,
    get_tiff_file as _get_tiff_file,
    get_upstream_file as _get_upstream_file,
    remove_session_by_id,
)
from murfey.server.ispyb import DB as ispyb_db, get_all_ongoing_visits
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise
from murfey.util.config import get_machine_config
from murfey.util.db import (
    ClassificationFeedbackParameters,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    ProcessingJob,
    RsyncInstance,
    Session,
    SessionProcessingParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)
from murfey.util.models import UpstreamFileRequestInfo, Visit

logger = getLogger("murfey.server.api.session_info")

router = APIRouter(
    prefix="/session_info",
    dependencies=[Depends(validate_token)],
    tags=["Session Info: General"],
)


@router.get("/health/")
def health_check(db=ispyb_db):
    conn = db.connection()
    conn.close()
    return {
        "ispyb_connection": True,
        "rabbitmq_connection": _transport_object.transport.is_connected(),
    }


@router.get("/connections/")
def connections_check():
    return {"connections": list(ws.manager.active_connections.keys())}


@router.get("/instruments/{instrument_name}/machine")
def machine_info_by_instrument(
    instrument_name: MurfeyInstrumentName,
):
    return get_machine_config(instrument_name)[instrument_name]


@router.get("/instruments/{instrument_name}/visits_raw", response_model=List[Visit])
def get_current_visits(instrument_name: MurfeyInstrumentName, db=ispyb_db):
    logger.debug(
        f"Received request to look up ongoing visits for {sanitise(instrument_name)}"
    )
    return get_all_ongoing_visits(instrument_name, db)


@router.get("/instruments/{instrument_name}/visits/")
def all_visit_info(
    instrument_name: MurfeyInstrumentName, request: Request, db=ispyb_db
):
    visits = get_all_ongoing_visits(instrument_name, db)

    if visits:
        return_query = [
            {
                "Start date": visit.start,
                "End date": visit.end,
                "Visit name": visit.name,
                "Time remaining": str(visit.end - datetime.now()),
            }
            for visit in visits
        ]  # "Proposal title": visit.proposal_title
        logger.debug(
            f"{len(visits)} visits active for {sanitise(instrument_name)=}: {', '.join(v.name for v in visits)}"
        )
        return templates.TemplateResponse(
            request=request,
            name="activevisits.html",
            context={"info": return_query, "microscope": instrument_name},
        )
    else:
        logger.debug(f"No visits identified for {sanitise(instrument_name)=}")
        return templates.TemplateResponse(
            request=request,
            name="activevisits.html",
            context={"info": [], "microscope": instrument_name},
        )


@router.get("/sessions/{session_id}/rsyncers", response_model=List[RsyncInstance])
def get_rsyncers_for_client(session_id: MurfeySessionID, db=murfey_db):
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.session_id == session_id)
    )
    return rsync_instances.all()


class SessionClients(BaseModel):
    session: Session
    clients: List[ClientEnvironment]


@router.get("/sessions/{session_id}")
async def get_session(session_id: MurfeySessionID, db=murfey_db) -> SessionClients:
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    clients = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.session_id == session_id)
    ).all()
    return SessionClients(session=session, clients=clients)


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


class VisitEndTime(BaseModel):
    end_time: Optional[datetime] = None


@router.post("/instruments/{instrument_name}/visits/{visit}/sessions/{name}")
def create_session(
    instrument_name: MurfeyInstrumentName,
    visit: str,
    name: str,
    visit_end_time: VisitEndTime,
    db=murfey_db,
) -> int:
    s = Session(
        name=name,
        visit=visit,
        instrument_name=instrument_name,
        visit_end_time=visit_end_time.end_time,
    )
    db.add(s)
    db.commit()
    sid = s.id
    return sid


@router.post("/sessions/{session_id}")
def update_session(
    session_id: MurfeySessionID, process: bool = True, db=murfey_db
) -> None:
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    session.process = process
    db.add(session)
    db.commit()
    return None


@router.delete("/sessions/{session_id}")
def remove_session(session_id: MurfeySessionID, db=murfey_db):
    remove_session_by_id(session_id, db)


@router.get("/instruments/{instrument_name}/visits/{visit_name}/sessions")
def get_sessions_with_visit(
    instrument_name: MurfeyInstrumentName, visit_name: str, db=murfey_db
) -> List[Session]:
    sessions = db.exec(
        select(Session)
        .where(Session.instrument_name == instrument_name)
        .where(Session.visit == visit_name)
    ).all()
    return sessions


@router.get("/instruments/{instrument_name}/sessions")
async def get_sessions_by_instrument_name(
    instrument_name: MurfeyInstrumentName, db=murfey_db
) -> List[Session]:
    sessions = db.exec(
        select(Session).where(Session.instrument_name == instrument_name)
    ).all()
    return sessions


@router.get("/sessions/{session_id}/data_collection_groups")
def get_dc_groups(
    session_id: MurfeySessionID, db=murfey_db
) -> Dict[str, DataCollectionGroup]:
    data_collection_groups = db.exec(
        select(DataCollectionGroup).where(DataCollectionGroup.session_id == session_id)
    ).all()
    return {dcg.tag: dcg for dcg in data_collection_groups}


@router.get("/sessions/{session_id}/data_collection_groups/{dcgid}/data_collections")
def get_data_collections(
    session_id: MurfeySessionID, dcgid: int, db=murfey_db
) -> List[DataCollection]:
    data_collections = db.exec(
        select(DataCollection).where(DataCollection.dcg_id == dcgid)
    ).all()
    return data_collections


@router.get("/clients")
async def get_clients(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    return clients


class CurrentGainRef(BaseModel):
    path: str


@router.put("/sessions/{session_id}/current_gain_ref")
def update_current_gain_ref(
    session_id: MurfeySessionID, new_gain_ref: CurrentGainRef, db=murfey_db
):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    session.current_gain_ref = new_gain_ref.path
    db.add(session)

    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_processing_parameters:
        session_processing_parameters[0].gain_ref = new_gain_ref.path
        db.add(session_processing_parameters[0])

    db.commit()


spa_router = APIRouter(
    prefix="/session_info/spa",
    dependencies=[Depends(validate_token)],
    tags=["Session Info: SPA"],
)


class ProcessingDetails(BaseModel):
    data_collection_group: DataCollectionGroup
    data_collections: List[DataCollection]
    processing_jobs: List[ProcessingJob]
    relion_params: SPARelionParameters
    feedback_params: ClassificationFeedbackParameters


@spa_router.get("/sessions/{session_id}/spa_processing_parameters")
def get_spa_proc_param_details(
    session_id: MurfeySessionID, db=murfey_db
) -> Optional[List[ProcessingDetails]]:
    params = db.exec(
        select(
            DataCollectionGroup,
            DataCollection,
            ProcessingJob,
            SPARelionParameters,
            ClassificationFeedbackParameters,
        )
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.id == DataCollection.dcg_id)
        .where(DataCollection.id == ProcessingJob.dc_id)
        .where(SPARelionParameters.pj_id == ProcessingJob.id)
        .where(ClassificationFeedbackParameters.pj_id == ProcessingJob.id)
    ).all()
    if not params:
        return None
    unique_dcg_indices = []
    dcg_ids = []
    for i, p in enumerate(params):
        if p[0].id not in dcg_ids:
            dcg_ids.append(p[0].id)
            unique_dcg_indices.append(i)

    def _parse(ps, i, dcg_id):
        res = []
        for p in ps:
            if p[0].id == dcg_id:
                if p[i] not in res:
                    res.append(p[i])
        return res

    return [
        ProcessingDetails(
            data_collection_group=params[i][0],
            data_collections=_parse(params, 1, d),
            processing_jobs=_parse(params, 2, d),
            relion_params=_parse(params, 3, d)[0],
            feedback_params=_parse(params, 4, d)[0],
        )
        for i, d in zip(unique_dcg_indices, dcg_ids)
    ]


@spa_router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{gsid}/foil_holes/{fhid}/num_movies"
)
def get_number_of_movies_from_foil_hole(
    session_id: int, dcgid: int, gsid: int, fhid: int, db=murfey_db
) -> int:
    movies = db.exec(
        select(Movie, FoilHole, GridSquare, DataCollectionGroup)
        .where(Movie.foil_hole_id == FoilHole.id)
        .where(FoilHole.name == fhid)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.name == gsid)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return len(movies)


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


tomo_router = APIRouter(
    prefix="/session_info/tomo",
    dependencies=[Depends(validate_token)],
    tags=["Session Info: CryoET"],
)


@tomo_router.get("/sessions/{session_id}/tilt_series/{tilt_series_tag}/tilts")
def get_tilts(
    session_id: MurfeySessionID, tilt_series_tag: str, db=murfey_db
) -> Dict[str, List[str]]:
    res = db.exec(
        select(TiltSeries, Tilt)
        .where(TiltSeries.tag == tilt_series_tag)
        .where(TiltSeries.session_id == session_id)
        .where(Tilt.tilt_series_id == TiltSeries.id)
    ).all()
    tilts: Dict[str, List[str]] = {}
    for el in res:
        if tilts.get(el[1].rsync_source):
            tilts[el[1].rsync_source].append(el[2].movie_path)
        else:
            tilts[el[1].rsync_source] = [el[2].movie_path]
    return tilts


correlative_router = APIRouter(
    prefix="/session_info/correlative",
    dependencies=[Depends(validate_token)],
    tags=["Session Info: Correlative Imaging"],
)


@correlative_router.get("/sessions/{session_id}/upstream_visits")
async def find_upstream_visits(session_id: MurfeySessionID, db=murfey_db):
    return _find_upstream_visits(session_id=session_id, db=db)


@correlative_router.get(
    "/visits/{visit_name}/sessions/{session_id}/upstream_file_paths"
)
async def gather_upstream_files(
    visit_name: str,
    session_id: MurfeySessionID,
    upstream_file_request: UpstreamFileRequestInfo,
    db=murfey_db,
):
    return _gather_upstream_files(
        session_id=session_id,
        upstream_instrument=upstream_file_request.upstream_instrument,
        upstream_visit_path=upstream_file_request.upstream_visit_path,
        db=db,
    )


@correlative_router.get(
    "/visits/{visit_name}/sessions/{session_id}/upstream_file/{upstream_file_path:path}"
)
async def get_upstream_file(
    visit_name: str,
    session_id: MurfeySessionID,
    upstream_file_path: Path,
    db=murfey_db,
):
    upstream_file = _get_upstream_file(upstream_file_path)
    return (
        FileResponse(path=upstream_file) if upstream_file is not None else upstream_file
    )


@correlative_router.get(
    "/visits/{visit_name}/sessions/{session_id}/upstream_tiff_paths"
)
async def gather_upstream_tiffs(visit_name: str, session_id: int, db=murfey_db):
    return _gather_upstream_tiffs(visit_name=visit_name, session_id=session_id, db=db)


@correlative_router.get(
    "/visits/{visit_name}/sessions/{session_id}/upstream_tiff/{tiff_path:path}"
)
async def get_tiff_file(visit_name: str, session_id: int, tiff_path: str, db=murfey_db):
    tiff_file = _get_tiff_file(
        visit_name=visit_name, session_id=session_id, tiff_path=tiff_path, db=db
    )
    return FileResponse(path=tiff_file) if isinstance(tiff_file, Path) else tiff_file
