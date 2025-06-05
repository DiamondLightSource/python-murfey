from logging import getLogger
from typing import Optional

import sqlalchemy
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

from murfey.server.api.auth import MurfeySessionIDFrontend as MurfeySessionID
from murfey.server.api.auth import validate_token
from murfey.server.murfey_db import murfey_db
from murfey.util.db import SessionProcessingParameters

logger = getLogger("murfey.server.api.processing_parameters")

router = APIRouter(
    prefix="/session_parameters",
    dependencies=[Depends(validate_token)],
    tags=["Processing Parameters"],
)


class EditableSessionProcessingParameters(BaseModel):
    gain_ref: str = ""
    dose_per_frame: Optional[float] = None
    eer_fractionation_file: str = ""
    symmetry: str = ""


@router.get("/sessions/{session_id}/session_processing_parameters")
def get_session_processing_parameters(
    session_id: MurfeySessionID, db: Session = murfey_db
) -> Optional[EditableSessionProcessingParameters]:
    try:
        proc_params = db.exec(
            select(SessionProcessingParameters).where(
                SessionProcessingParameters.session_id == session_id
            )
        ).one()
    except sqlalchemy.exc.NoResultFound:
        return None
    return EditableSessionProcessingParameters(
        gain_ref=proc_params.gain_ref,
        dose_per_frame=proc_params.dose_per_frame,
        eer_fractionation_file=proc_params.eer_fractionation_file,
        symmetry=proc_params.symmetry,
    )


@router.post("/sessions/{session_id}/session_processing_parameters")
def set_session_processing_parameters(
    session_id: MurfeySessionID,
    edited_parameters: EditableSessionProcessingParameters,
    db: Session = murfey_db,
) -> EditableSessionProcessingParameters:
    proc_params = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).one()
    proc_params.gain_ref = edited_parameters.gain_ref or proc_params.gain_ref
    proc_params.dose_per_frame = (
        edited_parameters.dose_per_frame or proc_params.dose_per_frame
    )
    proc_params.eer_fractionation_file = (
        edited_parameters.eer_fractionation_file or proc_params.eer_fractionation_file
    )
    proc_params.symmetry = edited_parameters.symmetry or proc_params.symmetry
    db.add(proc_params)
    db.commit()
    db.close()
    return edited_parameters
