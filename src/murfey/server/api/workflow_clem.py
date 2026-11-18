from __future__ import annotations

import json
from logging import getLogger
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel
from sqlmodel import Session, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config

# Set up logger
logger = getLogger("murfey.server.api.workflow_clem")

# Create APIRouter class object
router = APIRouter(
    prefix="/workflow/clem",
    tags=["Workflows: CLEM"],
)


class LifFileInfo(BaseModel):
    lif_file: Path


@router.post("/sessions/{session_id}/process_raw_lifs")  # API posts to this URL
def process_raw_lifs(
    session_id: int,
    lif_info: LifFileInfo,
    murfey_db: Session = murfey_db,
):
    if murfey.server._transport_object is None:
        logger.error("No TransportManager object was set up")
        return {"success": False}

    # Load the visit name from the database
    murfey_session = murfey_db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name

    # Load the machine config
    machine_config = get_machine_config(instrument_name)[instrument_name]

    # Get the visit directory, output directory, and job name
    visit_idx = lif_info.lif_file.parts.index(visit_name)
    visit_dir = Path(*lif_info.lif_file.parts[: visit_idx + 1])
    output_dir = visit_dir / machine_config.processed_directory_name
    job_name = str(
        (lif_info.lif_file.parent).relative_to(visit_dir.parent)
        / lif_info.lif_file.stem
    )

    # Construct recipe and submit it for processing
    recipe = {
        "recipes": ["clem-process-raw-lifs"],
        "parameters": {
            # Job parameters
            "lif_file": str(lif_info.lif_file),
            "output_dir": str(output_dir),
            # Other recipe parameters
            "session_dir": str(visit_dir),
            "session_id": session_id,
            "job_name": job_name,
            "feedback_queue": murfey.server._transport_object.feedback_queue,
        },
    }
    logger.info(
        f"Submitting LIF processing request to 'processing_recipe': \n"
        f"{json.dumps(recipe, indent=2, default=str)}"
    )
    murfey.server._transport_object.send(
        queue="processing_recipe",
        message=recipe,
        new_connection=True,
    )
    return {"success": True}


class TIFFSeriesInfo(BaseModel):
    series_name: str
    tiff_files: list[Path]
    series_metadata: Path


@router.post("/sessions/{session_id}/process_raw_tiffs")
def process_raw_tiffs(
    session_id: int,
    tiff_info: TIFFSeriesInfo,
    murfey_db: Session = murfey_db,
):
    if murfey.server._transport_object is None:
        logger.error("No TransportManager object was set up")
        return {"success": False}
    if not tiff_info.tiff_files:
        logger.error("No TIFF files were included in the incoming message")
        return {"success": False}

    # Load the visit name from the database
    murfey_session = murfey_db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name

    # Load the machine config
    machine_config = get_machine_config(instrument_name)[instrument_name]

    # Get the visit directory, output directory, series name, and job name
    tiff_file = tiff_info.tiff_files[0]
    visit_idx = tiff_file.parts.index(visit_name)
    raw_dir = Path(*tiff_file.parts[: visit_idx + 2])
    visit_dir = raw_dir.parent
    series_name = str(
        tiff_file.parent.relative_to(raw_dir) / tiff_file.stem.split("--")[0]
    )
    output_dir = (
        visit_dir / machine_config.processed_directory_name / series_name
    ).parent
    job_name = str(
        (tiff_file.parent).relative_to(visit_dir.parent) / tiff_file.stem.split("--")[0]
    )

    # Construct recipe and submit it for processing
    recipe = {
        "recipes": ["clem-process-raw-tiffs"],
        "parameters": {
            # Job parameters
            "tiff_list": None,
            "tiff_file": str(tiff_file),
            "metadata": str(tiff_info.series_metadata),
            "output_dir": str(output_dir),
            "series_name": series_name,
            # Other recipe parameters
            "session_dir": str(visit_dir),
            "session_id": session_id,
            "job_name": job_name,
            "feedback_queue": murfey.server._transport_object.feedback_queue,
        },
    }
    logger.info(
        f"Submitting TIFF processing request to 'processing_recipe': \n"
        f"{json.dumps(recipe, indent=2, default=str)}"
    )
    murfey.server._transport_object.send(
        queue="processing_recipe",
        message=recipe,
        new_connection=True,
    )
    return {"success": True}
