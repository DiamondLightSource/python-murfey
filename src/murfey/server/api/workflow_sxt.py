from logging import getLogger
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from murfey.server import _transport_object
from murfey.server.api.auth import (
    MurfeySessionIDInstrument as MurfeySessionID,
    validate_instrument_token,
)
from murfey.server.murfey_db import murfey_db
from murfey.workflows.sxt.process_sxt_tilt_series import (
    SXTTiltSeriesInfo,
    process_sxt_tilt_series_workflow,
)

logger = getLogger("murfey.server.api.workflow_sxt")


router = APIRouter(
    prefix="/workflow/sxt",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: Soft x-ray tomography"],
)


@router.post("/visits/{visit_name}/sessions/{session_id}/sxt_tilt_series")
def process_sxt_tilt_series(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_info: SXTTiltSeriesInfo,
    db=murfey_db,
):
    return process_sxt_tilt_series_workflow(
        visit_name, session_id, tilt_series_info, db
    )


class XrmFile(BaseModel):
    xrm_path: Path
    tiff_path: Path


@router.post("/convert_xrm_to_tiff")
def convert_xrm_to_tiff(xrm_file: XrmFile, db=murfey_db):
    if _transport_object:
        logger.info("Sending xrm conversion to images service")
        _transport_object.send(
            "images",
            {
                "image_command": "xrm_to_jpeg",
                "xrm_file": str(xrm_file.xrm_path),
                "tiff_destination": str(xrm_file.tiff_path),
                "annotate": True,
            },
            new_connection=True,
        )


class SxtRoiInfo(BaseModel):
    tag: str
    name: str
    x_stage_position: float
    y_stage_position: float
    pixel_size: float
    height: int
    width: int
    image: Path


@router.post("/visits/{visit_name}/sessions/{session_id}/register_sxt_roi")
def register_sxt_roi(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_info: SXTTiltSeriesInfo,
    db=murfey_db,
):
    # TODO
    return
