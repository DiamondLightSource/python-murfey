from logging import getLogger
from typing import List, Optional

import requests
import sqlalchemy
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

from murfey.server.api.auth import (
    MurfeySessionIDFrontend as MurfeySessionID,
    validate_token,
)
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import (
    AtlasOptics,
    Session as MurfeySession,
    SessionProcessingParameters,
)

logger = getLogger("murfey.server.api.processing_parameters")

router = APIRouter(
    prefix="/session_parameters",
    dependencies=[Depends(validate_token)],
    tags=["Processing Parameters"],
)


class GridParameters(BaseModel):
    id: Optional[str] = None
    atlas_x: int = 3
    atlas_y: int = 3
    square_x: int = 1
    square_y: int = 1
    squares_num: int = 3
    holes_per_square: int = -1
    bis_max_distance: float = 3
    min_bis_group_size: int = 1
    afis: bool = True
    target_defocus_min: float = -2
    target_defocus_max: float = -2
    step_defocus: float = 0
    drift_crit: float = -1
    tilt_angle: float = 0
    save_frames: bool = True
    force_process_from_average: bool = False
    offset_targeting: bool = True
    offset_distance: float = -1
    zeroloss_delay: int = -1
    hardwaredark_delay: int = -1
    coldfegflash_delay: int = -1
    multishot_per_hole: bool = True


class EditableSessionProcessingParameters(BaseModel):
    gain_ref: str = ""
    dose_per_frame: Optional[float] = None
    eer_fractionation_file: str = ""
    symmetry: str = ""
    run_class3d: Optional[bool] = None
    acquisition_parameters: List[GridParameters] = []


@router.get("/sessions/{session_id}/session_processing_parameters")
def get_session_processing_parameters(
    session_id: MurfeySessionID, db: Session = murfey_db
) -> Optional[EditableSessionProcessingParameters]:
    try:
        session = db.exec(
            select(MurfeySession).where(MurfeySession.id == session_id)
        ).one()
        instrument = session.instrument_name
        machine_config = get_machine_config(instrument_name=instrument)[instrument]
        available_acquisition_parameters = []
        if machine_config.smartscope_api_url:
            acquisition_parameter_response = requests.get(
                f"{machine_config.smartscope_api_url}/grid_parameters/"
            ).json()
            available_acquisition_parameters = [
                GridParameters(**param_set)
                for param_set in acquisition_parameter_response
            ]
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
        run_class3d=proc_params.run_class3d,
        acquisition_parameters=available_acquisition_parameters,
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
    if edited_parameters.run_class3d is not None:
        proc_params.run_class3d = edited_parameters.run_class3d
    db.add(proc_params)
    db.commit()
    db.close()
    return edited_parameters


@router.get("/atlas_optics")
def get_all_registered_atlas_optic_settings(
    db: Session = murfey_db,
) -> List[AtlasOptics]:
    return list(db.exec(select(AtlasOptics)).all())


@router.get("/sessions/{session_id}/atlas_optics")
def get_atlas_optics_for_session(
    session_id: int, db: Session = murfey_db
) -> AtlasOptics:
    return db.exec(
        select(MurfeySession, AtlasOptics)
        .where(MurfeySession.id == session_id)
        .where(MurfeySession.atlas_optics_id == AtlasOptics.id)
    ).one()[1]


@router.get("/acquisitions/{acquisition_uuid}/atlas_optics")
def get_atlas_optics_for_session_from_acquisition_uuid(
    acquisition_uuid: str, db: Session = murfey_db
) -> AtlasOptics:
    return db.exec(
        select(MurfeySession, AtlasOptics)
        .where(MurfeySession.acquisition_uuid == acquisition_uuid)
        .where(MurfeySession.atlas_optics_id == AtlasOptics.id)
    ).one()[1]


class AtlasOpticsData(BaseModel):
    mag: int
    tiles_x: int
    tiles_y: int
    spot_size: float
    c2_percentage: float
    name: str = ""


@router.post("/atlas_optics")
def add_atlas_optics_settings(
    atlas_optics: AtlasOpticsData, db: Session = murfey_db
) -> AtlasOptics:
    atlas_optics_row = AtlasOptics(**atlas_optics.model_dump())
    db.add(atlas_optics_row)
    db.commit()
    return atlas_optics_row


@router.post("/session/{session_id}/atlas_optics/{atlas_optics_id}")
def link_session_to_atlas_optics(
    session_id: int, atlas_optics_id: int, db: Session = murfey_db
) -> None:
    session = db.exec(select(MurfeySession).where(MurfeySession.id == session_id)).one()
    session.atlas_optics_id = atlas_optics_id
    db.add(session)
    db.commit()
    return None
