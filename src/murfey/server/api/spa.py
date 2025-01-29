from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path

from fastapi import APIRouter
from sqlmodel import select

from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import Session as MurfeySession

# Create APIRouter class object
router = APIRouter()


@lru_cache(maxsize=5)
def _cryolo_model_path(visit: str, instrument_name: str) -> Path:
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.model_search_directory:
        visit_directory = (
            machine_config.rsync_basepath / str(datetime.now().year) / visit
        )
        possible_models = list(
            (visit_directory / machine_config.model_search_directory).glob("*.h5")
        )
        if possible_models:
            return sorted(possible_models, key=lambda x: x.stat().st_ctime)[-1]
    return machine_config.default_model


@router.get("/sessions/{session_id}/cryolo_model")
def get_cryolo_model_path(session_id: int, db=murfey_db):
    session = db.exec(select(MurfeySession).where(MurfeySession.id == session_id)).one()
    return {"model_path": _cryolo_model_path(session.visit, session.instrment_name)}
