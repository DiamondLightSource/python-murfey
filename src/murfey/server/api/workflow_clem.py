from __future__ import annotations

from ast import literal_eval
from importlib.metadata import (
    EntryPoint,  # type hinting only
    entry_points,
)
from logging import getLogger
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise_path

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
    lif_file: LifFileInfo,
    murfey_db: Session = murfey_db,
):
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return False

    # Load the visit name from the database
    try:
        murfey_session = murfey_db.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one()
        visit_name = murfey_session.visit
    except Exception as e:
        logger.error("Error querying session information from database", exc_info=True)
        print(e)
        return False

    # Find the visit directory, the raw directory name, and the job name
    try:
        visit_idx = lif_file.lif_file.parts.index(visit_name)
        visit_dir = Path(
            "/".join(
                ""
                if part == "/"  # Replace root "/" with "" for Linux paths
                else part
                for part in lif_file.lif_file.parts[: visit_idx + 1]
            )
        )
        raw_dir = lif_file.lif_file.parts[visit_idx + 1]
        job_name = str(
            (lif_file.lif_file.parent / lif_file.lif_file.stem).relative_to(
                visit_dir.parent
            )
        )
    except Exception:
        logger.error(
            "Could not determine the visit directory from LIF file "
            f"{sanitise_path(lif_file.lif_file)}",
            exc_info=True,
        )
        return False

    # Construct recipe and submit it for processing
    recipe = {
        "recipes": ["clem-process-raw-lifs"],
        "parameters": {
            # Job parameters
            "lif_file": f"{str(lif_file.lif_file)}",
            "root_folder": raw_dir,
            # Other recipe parameters
            "session_dir": f"{str(visit_dir)}",
            "session_id": session_id,
            "job_name": job_name,
            "feedback_queue": _transport_object.feedback_queue,
        },
    }
    logger.debug(
        f"Submitting LIF processing request to {_transport_object.feedback_queue!r} "
        "with the following recipe: \n"
        f"{recipe}"
    )
    _transport_object.send(
        queue="processing_recipe",
        message=recipe,
        new_connection=True,
    )
    return True


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
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return False

    # Load the visit name from the database
    try:
        murfey_session = murfey_db.exec(
            select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
        ).one()
        visit_name = murfey_session.visit
    except Exception as e:
        logger.error("Error querying session information from database", exc_info=True)
        print(e)
        return False

    # Find the visit directory, the raw directory name, and the job name
    try:
        tiff_file = tiff_info.tiff_files[0]
        visit_idx = tiff_file.parts.index(visit_name)
        visit_dir = Path(
            "/".join(
                ""
                if part == "/"  # Replace root "/" with "" for Linux paths
                else part
                for part in tiff_file.parts[: visit_idx + 1]
            )
        )
        raw_dir = tiff_file.parts[visit_idx + 1]
        job_name = str(
            (tiff_file.parent / tiff_file.stem.split("--")[0]).relative_to(
                visit_dir.parent
            )
        )
    except Exception:
        logger.error(
            "Could not determine the visit directory from TIFF file "
            f"{sanitise_path(tiff_file)}",
            exc_info=True,
        )
        return False

    # Construct recipe and submit it for processing
    recipe = {
        "recipes": ["clem-process-raw-tiffs"],
        "parameters": {
            # Job parameters
            "tiff_list": "null",
            "tiff_file": f"{str(tiff_file)}",
            "root_folder": raw_dir,
            "metadata": f"{str(tiff_info.series_metadata)}",
            # Other recipe parameters
            "session_dir": f"{str(visit_dir)}",
            "session_id": session_id,
            "job_name": job_name,
            "feedback_queue": _transport_object.feedback_queue,
        },
    }
    logger.debug(
        f"Submitting TIFF processing request to {_transport_object.feedback_queue!r} "
        "with the following recipe: \n"
        f"{recipe}"
    )
    _transport_object.send(
        queue="processing_recipe",
        message=recipe,
        new_connection=True,
    )
    return True


class AlignAndMergeParams(BaseModel):
    # Processing parameters
    series_name: str
    images: list[Path]
    metadata: Path
    # Optional processing parameters
    crop_to_n_frames: Optional[int] = None
    align_self: Literal["enabled", ""] = ""
    flatten: Literal["mean", "min", "max", ""] = ""
    align_across: Literal["enabled", ""] = ""

    @field_validator("images", mode="before")
    @classmethod
    def parse_stringified_list(cls, value):
        if isinstance(value, str):
            try:
                eval_result = literal_eval(value)
                if isinstance(eval_result, list):
                    parent_tiffs = [Path(p) for p in eval_result]
                    return parent_tiffs
            except (SyntaxError, ValueError):
                raise ValueError("Unable to parse input")
        # Return value as-is; if it fails, it fails
        return value


@router.post("/sessions/{session_id}/align_and_merge_stacks")
def align_and_merge_stacks(
    session_id: int,
    align_and_merge_params: AlignAndMergeParams,
    db: Session = murfey_db,
):
    try:
        # Try and load relevant Murfey workflow
        workflow: EntryPoint = list(
            entry_points(group="murfey.workflows", name="clem.align_and_merge")
        )[0]
    except IndexError:
        raise RuntimeError("The relevant Murfey workflow was not found")

    # Get instrument name from the database to load the correct config file
    session_row: MurfeyDB.Session = db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    instrument_name = session_row.instrument_name

    # Pass arguments to correct workflow
    workflow.load()(
        # Match the arguments found in murfey.workflows.clem.align_and_merge
        # Session parameters
        session_id=session_id,
        instrument_name=instrument_name,
        # Processing parameters
        series_name=align_and_merge_params.series_name,
        images=align_and_merge_params.images,
        metadata=align_and_merge_params.metadata,
        # Optional processing parameters
        crop_to_n_frames=align_and_merge_params.crop_to_n_frames,
        align_self=align_and_merge_params.align_self,
        flatten=align_and_merge_params.flatten,
        align_across=align_and_merge_params.align_across,
        # Optional session parameters
        messenger=_transport_object,
    )
    return True
