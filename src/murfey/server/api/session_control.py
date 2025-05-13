from typing import Optional

from fastapi import APIRouter, Depends
from ispyb.sqlalchemy import AutoProcProgram as ISPyBAutoProcProgram
from pydantic import BaseModel
from sqlmodel import select

import murfey.server.ispyb
from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionID, validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    Session,
)

router = APIRouter(
    prefix="/session_control",
    dependencies=[Depends(validate_token)],
    tags=["session control"],
)


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


@router.post("/sessions/{session_id}/successful_processing")
def register_processing_success_in_ispyb(
    session_id: MurfeySessionID, db=murfey.server.ispyb.DB, murfey_db=murfey_db
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
        apps = db.query(ISPyBAutoProcProgram).filter(
            ISPyBAutoProcProgram.autoProcProgramId.in_(appids)
        )
        for updated in apps:
            updated.processingStatus = True
            _transport_object.do_update_processing_status(updated)
