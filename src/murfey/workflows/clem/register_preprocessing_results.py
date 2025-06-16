"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
import traceback
from ast import literal_eval
from pathlib import Path

from pydantic import BaseModel, field_validator
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
    image_stack: Path
    metadata: Path
    series_name: str
    channel: str
    number_of_members: int
    parent_lif: Path


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
            clem_img_stk: CLEMImageStack = get_db_entry(
                db=murfey_db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=result.image_stack,
            )

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

            # Link tables to one another and populate fields
            clem_img_stk.associated_metadata = clem_metadata
            clem_img_stk.parent_lif = clem_lif_file
            clem_img_stk.parent_series = clem_img_series
            clem_img_stk.channel_name = result.channel
            murfey_db.add(clem_img_stk)
            murfey_db.commit()

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
                f"{result.channel!r} image stack"
            )

        except Exception:
            logger.error(
                "Exception encountered when registering LIF preprocessing result for "
                f"{result.series_name!r} {result.channel!r} image stack: \n"
                f"{traceback.format_exc()}"
            )
            return False

        # Load all image stacks associated with current series from database
        try:
            image_stacks = [
                Path(row)
                for row in murfey_db.exec(
                    select(CLEMImageStack.file_path).where(
                        CLEMImageStack.series_id == clem_img_series.id
                    )
                ).all()
            ]
            logger.debug(
                f"Found the following images: {[str(file) for file in image_stacks]}"
            )
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

        # Check if all image stacks for this series are accounted for
        if not len(image_stacks) == clem_img_series.number_of_members:
            logger.info(
                f"Members of the series {result.series_name!r} are still missing; "
                "the next stage of processing will not be triggered yet"
            )
            return True

        # Request for next stage of processing if all members are present
        cluster_response = submit_cluster_request(
            session_id=session_id,
            instrument_name=instrument_name,
            series_name=result.series_name,
            images=image_stacks,
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
    image_stack: Path
    metadata: Path
    series_name: str
    channel: str
    number_of_members: int
    parent_tiffs: list[Path]

    @field_validator("parent_tiffs", mode="before")
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
            clem_img_stk: CLEMImageStack = get_db_entry(
                db=murfey_db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=result.image_stack,
            )
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

            # Link tables to one another and populate fields
            # Register TIFF files and populate them iteratively first
            for file in result.parent_tiffs:
                clem_tiff_file: CLEMTIFFFile = get_db_entry(
                    db=murfey_db,
                    table=CLEMTIFFFile,
                    session_id=session_id,
                    file_path=file,
                )
                clem_tiff_file.associated_metadata = clem_metadata
                clem_tiff_file.child_series = clem_img_series
                clem_tiff_file.child_stack = clem_img_stk
                murfey_db.add(clem_tiff_file)
                murfey_db.commit()

            clem_img_stk.associated_metadata = clem_metadata
            clem_img_stk.parent_series = clem_img_series
            clem_img_stk.channel_name = result.channel
            murfey_db.add(clem_img_stk)
            murfey_db.commit()

            clem_img_series.associated_metadata = clem_metadata
            clem_img_series.number_of_members = result.number_of_members
            murfey_db.add(clem_img_series)
            murfey_db.commit()

            logger.info(
                f"TIFF preprocessing results registered for {result.series_name!r} "
                f"{result.channel!r} image stack"
            )

        except Exception:
            logger.error(
                "Exception encountered when registering TIFF preprocessing result for "
                f"{result.series_name!r} {result.channel!r} image stack: \n"
                f"{traceback.format_exc()}"
            )
            return False

        # Load all image stacks associated with current series from database
        try:
            image_stacks = [
                Path(row)
                for row in murfey_db.exec(
                    select(CLEMImageStack.file_path).where(
                        CLEMImageStack.series_id == clem_img_series.id
                    )
                ).all()
            ]
            logger.debug(
                f"Found the following images: {[str(file) for file in image_stacks]}"
            )
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

        # Check if all image stacks for this series are accounted for
        if not len(image_stacks) == clem_img_series.number_of_members:
            logger.info(
                f"Members of the series {result.series_name!r} are still missing; "
                "the next stage of processing will not be triggered yet"
            )
            return True

        # Request for next stage of processing if all members are present
        cluster_response = submit_cluster_request(
            session_id=session_id,
            instrument_name=instrument_name,
            series_name=result.series_name,
            images=image_stacks,
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
