from logging import getLogger
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel

import murfey.server
from murfey.server.api.auth import (
    MurfeySessionIDInstrument as MurfeySessionID,
    validate_instrument_token,
)
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise
from murfey.util.models import SearchMapParameters
from murfey.workflows.sxt.process_sxt_tilt_series import SXTTiltSeriesInfo

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
    if murfey.server._transport_object:
        murfey.server._transport_object.send(
            murfey.server._transport_object.feedback_queue,
            {
                "register": "sxt.process_tilt_series",
                "session_id": session_id,
                "visit_name": visit_name,
                "tilt_series_info": tilt_series_info.model_dump(mode="json"),
            },
        )


class XrmFile(BaseModel):
    xrm_path: Path
    tiff_path: Path


@router.post("/convert_xrm_to_tiff")
def convert_xrm_to_tiff(xrm_file: XrmFile, db=murfey_db):
    if murfey.server._transport_object:
        logger.info("Sending xrm conversion to images service")
        murfey.server._transport_object.send(
            "images",
            {
                "image_command": "xrm_to_jpeg",
                "xrm_file": str(xrm_file.xrm_path),
                "tiff_destination": str(xrm_file.tiff_path),
                "annotate": True,
            },
            new_connection=True,
        )


@router.post("/sessions/{session_id}/sxt_roi/{roi_name}")
def register_sxt_roi(
    session_id: MurfeySessionID,
    roi_name: str,
    roi_info: SearchMapParameters,
    db=murfey_db,
):
    if murfey.server._transport_object:
        logger.info(f"Registering SXT region {sanitise(roi_name)}")
        murfey.server._transport_object.send(
            murfey.server._transport_object.feedback_queue,
            {
                "register": "sxt.register_roi",
                "session_id": session_id,
                "roi_name": roi_name,
                "roi_info": roi_info.model_dump(mode="json"),
            },
            new_connection=True,
        )
    else:
        logger.warning("No transport object for register_sxt_roi")
