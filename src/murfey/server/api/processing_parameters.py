from logging import getLogger

from fastapi import APIRouter
from pydantic import BaseModel
from sqlmodel import Session, select

from murfey.server.murfey_db import murfey_db
from murfey.util.db import SessionProcessingParameters

logger = getLogger("murfey.server.api.processing_parameters")

router = APIRouter()


class EditableSessionProcessingParameters(BaseModel):
    gain_ref: str
    dose_per_frame: float
    eer_fractionation_file: str
    symmetry: str


@router.get("sessions/{session_id}/session_processing_parameters")
def get_session_processing_parameters(
    session_id: int, db: Session = murfey_db
) -> EditableSessionProcessingParameters:
    proc_params = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).one()
    return EditableSessionProcessingParameters(
        gain_ref=proc_params.gain_ref,
        dose_per_frame=proc_params.dose_per_frame,
        eer_fractionation_file=proc_params.eer_fractionation_file,
        symmetry=proc_params.symmetry,
    )
