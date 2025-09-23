"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
import traceback
from pathlib import Path
from typing import Literal

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


class LIFPreprocessingResult(BaseModel):
    series_name: str
    number_of_members: int
    is_stack: bool
    is_montage: bool
    output_files: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], Path
    ]
    metadata: Path
    parent_lif: Path
    pixels_x: int
    pixels_y: int
    units: str
    pixel_size: float
    resolution: float
    extent: list[float]


def register_lif_preprocessing_result(
    message: dict, murfey_db: Session, demo: bool = False
) -> bool:
    """
    session_id (recipe)
    register (wrapper)
    result (wrapper)
        key1
        key2
        ...
    """

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )

    # Validate message and try and load results
    try:
        if isinstance(message["result"], str):
            json_obj: dict = json.loads(message["result"])
            result = LIFPreprocessingResult(**json_obj)
        elif isinstance(message["result"], dict):
            result = LIFPreprocessingResult(**message["result"])
        else:
            logger.error(
                f"Invalid type for LIF preprocessing result: {type(message['result'])}"
            )
            return False
    except Exception:
        logger.error(
            "Exception encountered when parsing LIF preprocessing result: \n"
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

            clem_lif_file: CLEMLIFFile = get_db_entry(
                db=murfey_db,
                table=CLEMLIFFile,
                session_id=session_id,
                file_path=result.parent_lif,
            )

            # Iterate through image stacks and start populating them first
            for channel, output_file in result.output_files.items():
                clem_img_stk: CLEMImageStack = get_db_entry(
                    db=murfey_db,
                    table=CLEMImageStack,
                    session_id=session_id,
                    file_path=output_file,
                )

                # Link tables to one another and populate fields
                clem_img_stk.associated_metadata = clem_metadata
                clem_img_stk.parent_lif = clem_lif_file
                clem_img_stk.parent_series = clem_img_series
                clem_img_stk.channel_name = channel
                murfey_db.add(clem_img_stk)
                murfey_db.commit()

            # Link other tables together
            clem_img_series.associated_metadata = clem_metadata
            clem_img_series.parent_lif = clem_lif_file
            clem_img_series.number_of_members = result.number_of_members
            murfey_db.add(clem_img_series)
            murfey_db.commit()

            clem_metadata.parent_lif = clem_lif_file
            murfey_db.add(clem_metadata)
            murfey_db.commit()

            logger.info(
                f"LIF preprocessing results registered for {result.series_name!r} "
            )

        except Exception:
            logger.error(
                "Exception encountered when registering LIF preprocessing result for "
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

        # Request for next stage of processing if all members are present
        cluster_response = submit_cluster_request(
            session_id=session_id,
            instrument_name=instrument_name,
            series_name=result.series_name,
            images=list(result.output_files.values()),
            metadata=result.metadata,
            crop_to_n_frames=processing_params.crop_to_n_frames,
            align_self=processing_params.align_self,
            flatten=processing_params.flatten,
            align_across=processing_params.align_across,
            messenger=_transport_object,
        )
        if cluster_response is False:
            logger.error(
                "Error requesting align-and-merge processing job for "
                f"{result.series_name!r} series"
            )
            return False
        logger.info(
            "Successfully requested align-and-merge processing job for "
            f"{result.series_name!r} series"
        )
        return True

    finally:
        murfey_db.close()


class TIFFPreprocessingResult(BaseModel):
    series_name: str
    number_of_members: int
    is_stack: bool
    is_montage: bool
    output_files: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], Path
    ]
    metadata: Path
    parent_tiffs: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], list[Path]
    ]
    pixels_x: int
    pixels_y: int
    units: str
    pixel_size: float
    resolution: float
    extent: list[float]


def register_tiff_preprocessing_result(
    message: dict, murfey_db: Session, demo: bool = False
) -> bool:

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )
    try:
        if isinstance(message["result"], str):
            json_obj: dict = json.loads(message["result"])
            result = TIFFPreprocessingResult(**json_obj)
        elif isinstance(message["result"], dict):
            result = TIFFPreprocessingResult(**message["result"])
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
            # Iteratively register the output image stacks
            for channel, output_file in result.output_files.items():
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
                murfey_db.add(clem_img_stk)
                murfey_db.commit()

                # Register parent TIFF files iteratively for each channel
                for file in result.parent_tiffs[channel]:
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
                    murfey_db.add(clem_tiff_file)
                    murfey_db.commit()

            clem_img_series.associated_metadata = clem_metadata
            clem_img_series.number_of_members = result.number_of_members
            murfey_db.add(clem_img_series)
            murfey_db.commit()

            logger.info(
                f"TIFF preprocessing results registered for {result.series_name!r} "
            )

        except Exception:
            logger.error(
                "Exception encountered when registering TIFF preprocessing result for "
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

        # Request for next stage of processing if all members are present
        cluster_response = submit_cluster_request(
            session_id=session_id,
            instrument_name=instrument_name,
            series_name=result.series_name,
            images=list(result.output_files.values()),
            metadata=result.metadata,
            crop_to_n_frames=processing_params.crop_to_n_frames,
            align_self=processing_params.align_self,
            flatten=processing_params.flatten,
            align_across=processing_params.align_across,
            messenger=_transport_object,
        )
        if cluster_response is False:
            logger.error(
                "Error requesting align-and-merge processing job for "
                f"{result.series_name!r} series"
            )
            return False
        logger.info(
            "Successfully requested align-and-merge processing job for "
            f"{result.series_name!r} series"
        )
        return True

    finally:
        murfey_db.close()
