from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select

import murfey.server.ispyb
from murfey.server.api.auth import MurfeySessionID, validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import (
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    RsyncInstance,
    Session,
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)

router = APIRouter(
    prefix="/session_info",
    dependencies=[Depends(validate_token)],
    tags=["session info"],
)


class Visit(BaseModel):
    start: datetime
    end: datetime
    session_id: int
    name: str
    beamline: str
    proposal_title: str

    def __repr__(self) -> str:
        return (
            "Visit("
            f"start='{self.start:%Y-%m-%d %H:%M}', "
            f"end='{self.end:%Y-%m-%d %H:%M}', "
            f"session_id='{self.session_id!r}',"
            f"name={self.name!r}, "
            f"beamline={self.beamline!r}, "
            f"proposal_title={self.proposal_title!r}"
            ")"
        )


@router.get("/instruments/{instrument_name}/visits_raw", response_model=List[Visit])
def get_current_visits(instrument_name: str, db=murfey.server.ispyb.DB):
    return murfey.server.ispyb.get_all_ongoing_visits(instrument_name, db)


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
