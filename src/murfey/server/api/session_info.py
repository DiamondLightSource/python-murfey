from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy import func
from sqlmodel import select

import murfey.server.ispyb
from murfey.server import sanitise, templates
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
from murfey.util.config import MachineConfig, get_machine_config
from murfey.util.db import (
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    Movie,
    ProcessingJob,
    RsyncInstance,
    Session,
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)
from murfey.util.models import Visit

logger = getLogger("murfey.server.api.session_info")

router = APIRouter(
    prefix="/session_info",
    dependencies=[Depends(validate_token)],
    tags=["session info"],
)


@router.get("/instruments/{instrument_name}/machine")
def machine_info_by_instrument(instrument_name: str) -> Optional[MachineConfig]:
    return get_machine_config_for_instrument(instrument_name)


@router.get("/instruments/{instrument_name}/visits_raw", response_model=List[Visit])
def get_current_visits(instrument_name: str, db=murfey.server.ispyb.DB):
    return murfey.server.ispyb.get_all_ongoing_visits(instrument_name, db)


@router.get("/instruments/{instrument_name}/visits/")
def all_visit_info(instrument_name: str, request: Request, db=murfey.server.ispyb.DB):
    visits = murfey.server.ispyb.get_all_ongoing_visits(instrument_name, db)

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


@router.get("/session/{session_id}")
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


@router.post("/instruments/{instrument_name}/visits/{visit}/session/{name}")
def create_session(instrument_name: str, visit: str, name: str, db=murfey_db) -> int:
    s = Session(name=name, visit=visit, instrument_name=instrument_name)
    db.add(s)
    db.commit()
    sid = s.id
    return sid


@router.post("/sessions/{session_id}")
def update_session(
    session_id: MurfeySessionID, process: bool = True, db=murfey_db
) -> None:
    session = db.exec(select(Session).where(session_id == session_id)).one()
    session.process = process
    db.add(session)
    db.commit()
    return None


@router.delete("/sessions/{session_id}")
def remove_session(session_id: MurfeySessionID, db=murfey_db):
    remove_session_by_id(session_id, db)


@router.get("/sessions/{session_id}/upstream_visits")
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


@router.get("/instruments/{instrument_name}/visits/{visit_name}/sessions")
def get_sessions_with_visit(
    instrument_name: str, visit_name: str, db=murfey_db
) -> List[Session]:
    sessions = db.exec(
        select(Session)
        .where(Session.instrument_name == instrument_name)
        .where(Session.visit == visit_name)
    ).all()
    return sessions


@router.get("/instruments/{instrument_name}/sessions")
async def get_sessions_by_instrument_name(
    instrument_name: str, db=murfey_db
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


@router.get("/num_movies")
def count_number_of_movies(db=murfey_db) -> Dict[str, int]:
    res = db.exec(
        select(Movie.tag, func.count(Movie.murfey_id)).group_by(Movie.tag)
    ).all()
    return {r[0]: r[1] for r in res}


class CurrentGainRef(BaseModel):
    path: str


@router.put("/sessions/{session_id}/current_gain_ref")
def update_current_gain_ref(
    session_id: MurfeySessionID, new_gain_ref: CurrentGainRef, db=murfey_db
):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    session.current_gain_ref = new_gain_ref.path
    db.add(session)
    db.commit()


spa_router = APIRouter(
    prefix="/session_info/spa",
    dependencies=[Depends(validate_token)],
    tags=["session info for SPA"],
)


class ProcessingDetails(BaseModel):
    data_collection_group: DataCollectionGroup
    data_collections: List[DataCollection]
    processing_jobs: List[ProcessingJob]
    relion_params: SPARelionParameters
    feedback_params: SPAFeedbackParameters


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
            SPAFeedbackParameters,
        )
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.id == DataCollection.dcg_id)
        .where(DataCollection.id == ProcessingJob.dc_id)
        .where(SPARelionParameters.pj_id == ProcessingJob.id)
        .where(SPAFeedbackParameters.pj_id == ProcessingJob.id)
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
    tags=["session info for cryoET"],
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
