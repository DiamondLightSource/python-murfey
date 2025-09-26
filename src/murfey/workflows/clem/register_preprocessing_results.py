"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
import re
import traceback
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.util.db import (
    CLEMImageMetadata,
    CLEMImageSeries,
    CLEMImageStack,
    CLEMLIFFile,
    CLEMTIFFFile,
)
from murfey.util.db import Session as MurfeySession
from murfey.util.processing_params import (
    default_clem_align_and_merge_parameters as processing_params,
)
from murfey.workflows.clem import get_db_entry
from murfey.workflows.clem.align_and_merge import submit_cluster_request

logger = logging.getLogger("murfey.workflows.clem.register_preprocessing_results")


class CLEMPreprocessingResult(BaseModel):
    series_name: str
    number_of_members: int
    is_stack: bool
    is_montage: bool
    output_files: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], Path
    ]
    metadata: Path
    parent_lif: Optional[Path] = None
    parent_tiffs: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], list[Path]
    ] = {}
    pixels_x: int
    pixels_y: int
    units: str
    pixel_size: float
    resolution: float
    extent: list[float]


def run(message: dict, murfey_db: Session, demo: bool = False) -> bool:

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )
    try:
        if isinstance(message["result"], str):
            json_obj: dict = json.loads(message["result"])
            result = CLEMPreprocessingResult(**json_obj)
        elif isinstance(message["result"], dict):
            result = CLEMPreprocessingResult(**message["result"])
        else:
            logger.error(
                f"Invalid type for TIFF preprocessing result: {type(message['result'])}"
            )
            return False
    except Exception:
        logger.error(
            "Exception encountered when parsing TIFF preprocessing result: \n"
            f"{traceback.format_exc()}"
        )
        return False

    # Outer try-finally block for tidying up database-related section of function
    try:
        # Register items in database if not already present
        try:
            clem_img_series: CLEMImageSeries = get_db_entry(
                db=murfey_db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=result.series_name,
            )
            clem_metadata: CLEMImageMetadata = get_db_entry(
                db=murfey_db,
                table=CLEMImageMetadata,
                session_id=session_id,
                file_path=result.metadata,
            )
            # Register and link parent LIF file if present
            if result.parent_lif is not None:
                clem_lif_file: CLEMLIFFile = get_db_entry(
                    db=murfey_db,
                    table=CLEMLIFFile,
                    session_id=session_id,
                    file_path=result.parent_lif,
                )
                clem_img_series.parent_lif = clem_lif_file
                clem_metadata.parent_lif = clem_lif_file

            # Link and commit series and metadata tables first
            clem_img_series.associated_metadata = clem_metadata
            clem_img_series.number_of_members = result.number_of_members
            murfey_db.add_all([clem_img_series, clem_metadata])
            murfey_db.commit()

            # Iteratively register the output image stacks
            for c, (channel, output_file) in enumerate(result.output_files.items()):
                clem_img_stk: CLEMImageStack = get_db_entry(
                    db=murfey_db,
                    table=CLEMImageStack,
                    session_id=session_id,
                    file_path=output_file,
                )

                # Link associated metadata
                clem_img_stk.associated_metadata = clem_metadata
                clem_img_stk.parent_series = clem_img_series
                clem_img_stk.channel_name = channel
                if result.parent_lif is not None:
                    clem_img_stk.parent_lif = clem_lif_file
                murfey_db.add(clem_img_stk)
                murfey_db.commit()

                # Register and link parent TIFF files if present
                if result.parent_tiffs:
                    seed_file = result.parent_tiffs[channel][0]
                    if c == 0:
                        # Load list of files to register from seed file
                        series_identifier = seed_file.stem.split("--")[0] + "--"
                        tiff_list = list(
                            seed_file.parent.glob(f"{series_identifier}--")
                        )

                    # Load TIFF files by colour channel if "--C" in file stem
                    match = re.search(r"--C[\d]{2,3}", seed_file.stem)
                    tiff_file_subset = [
                        file
                        for file in tiff_list
                        if file.stem.startswith(series_identifier)
                        and (match.group(0) in file.stem if match else True)
                    ]
                    tiff_file_subset.sort()

                    # Register TIFF file subset
                    clem_tiff_files = []
                    for file in tiff_file_subset:
                        clem_tiff_file: CLEMTIFFFile = get_db_entry(
                            db=murfey_db,
                            table=CLEMTIFFFile,
                            session_id=session_id,
                            file_path=file,
                        )

                        # Link associated metadata
                        clem_tiff_file.associated_metadata = clem_metadata
                        clem_tiff_file.child_series = clem_img_series
                        clem_tiff_file.child_stack = clem_img_stk

                        clem_tiff_files.append(clem_tiff_file)

                    murfey_db.add_all(clem_tiff_files)
                    murfey_db.commit()

            logger.info(
                f"CLEM preprocessing results registered for {result.series_name!r} "
            )

        except Exception:
            logger.error(
                "Exception encountered when registering CLEM preprocessing result for "
                f"{result.series_name!r}: \n"
                f"{traceback.format_exc()}"
            )
            return False

        # Load instrument name
        try:
            instrument_name = (
                murfey_db.exec(
                    select(MurfeySession).where(MurfeySession.id == session_id)
                )
                .one()
                .instrument_name
            )
        except Exception:
            logger.error(
                f"Error requesting data from database for {result.series_name!r} series: \n"
                f"{traceback.format_exc()}"
            )
            return False

        # Construct list of files to use for image alignment and merging steps
        image_combos_to_process = [
            list(result.output_files.values())  # Composite image of all channels
        ]
        # Create additional job for fluorescent-only composite image if fluorescent channels are present
        if ("gray" in result.output_files.keys()) and len(result.output_files) > 1:
            image_combos_to_process.append(
                [
                    file
                    for channel, file in result.output_files.items()
                    if channel != "gray"
                ]
            )

        # Request for image alignment and processing for the requested combinations
        for image_combo in image_combos_to_process:
            try:
                submit_cluster_request(
                    session_id=session_id,
                    instrument_name=instrument_name,
                    series_name=result.series_name,
                    images=image_combo,
                    metadata=result.metadata,
                    crop_to_n_frames=processing_params.crop_to_n_frames,
                    align_self=processing_params.align_self,
                    flatten=processing_params.flatten,
                    align_across=processing_params.align_across,
                    messenger=_transport_object,
                )
            except Exception:
                logger.error(
                    "Error requesting image alignment and merging job for "
                    f"{result.series_name!r} series",
                    exc_info=True,
                )
                return False
        logger.info(
            "Successfully requested image alignment and merging job for "
            f"{result.series_name!r} series"
        )
        return True

    finally:
        murfey_db.close()
