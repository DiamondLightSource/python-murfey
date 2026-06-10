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

# Set up logger
logger = getLogger("murfey.server.api.clem")

# Create APIRouter class object
router = APIRouter(
    prefix="/workflow/clem",
    tags=["Workflows: CLEM"],
)


class LifInfo(BaseModel):
    lif_file: Path


@router.post("/sessions/{session_id}/process_raw_lifs")  # API posts to this URL
def process_raw_lifs(
    session_id: int,
    lif_file: LifInfo,
    db: Session = murfey_db,
):
    try:
        # Try and load relevant Murfey workflow
        workflow: EntryPoint = list(
            entry_points(group="murfey.workflows", name="clem.process_raw_lifs")
        )[0]
    except IndexError:
        raise RuntimeError("The relevant Murfey workflow was not found")

    # Get instrument name from the database to load the correct config file
    session_row: MurfeyDB.Session = db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    instrument_name = session_row.instrument_name

    # Pass arguments along to the correct workflow
    workflow.load()(
        # Match the arguments found in murfey.workflows.clem.process_raw_lifs
        file=lif_file.lif_file,
        root_folder="images",
        session_id=session_id,
        instrument_name=instrument_name,
        messenger=_transport_object,
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
    db: Session = murfey_db,
):
    try:
        # Try and load relevant Murfey workflow
        workflow: EntryPoint = list(
            entry_points(group="murfey.workflows", name="clem.process_raw_tiffs")
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
        # Match the arguments found in murfey.workflows.clem.process_raw_tiffs
        tiff_list=tiff_info.tiff_files,
        root_folder="images",
        session_id=session_id,
        instrument_name=instrument_name,
        metadata=tiff_info.series_metadata,
        messenger=_transport_object,
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
