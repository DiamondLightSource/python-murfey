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
from importlib.metadata import entry_points
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.server import _transport_object
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


def run(message: dict, murfey_db: Session, demo: bool = False) -> dict[str, bool]:
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
            return {"success": False, "requeue": False}
    except Exception:
        logger.error(
            "Exception encountered when parsing TIFF preprocessing result: \n"
            f"{traceback.format_exc()}"
        )
        return {"success": False, "requeue": False}

    # Outer try-finally block for tidying up database-related section of function
    try:
        # Register items in database if not already present
        try:
            clem_img_series: MurfeyDB.CLEMImageSeries = get_db_entry(
                db=murfey_db,
                table=MurfeyDB.CLEMImageSeries,
                session_id=session_id,
                series_name=result.series_name,
            )
            clem_metadata: MurfeyDB.CLEMImageMetadata = get_db_entry(
                db=murfey_db,
                table=MurfeyDB.CLEMImageMetadata,
                session_id=session_id,
                file_path=result.metadata,
            )
            # Register and link parent LIF file if present
            if result.parent_lif is not None:
                clem_lif_file: MurfeyDB.CLEMLIFFile = get_db_entry(
                    db=murfey_db,
                    table=MurfeyDB.CLEMLIFFile,
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
                clem_img_stk: MurfeyDB.CLEMImageStack = get_db_entry(
                    db=murfey_db,
                    table=MurfeyDB.CLEMImageStack,
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
                        clem_tiff_file: MurfeyDB.CLEMTIFFFile = get_db_entry(
                            db=murfey_db,
                            table=MurfeyDB.CLEMTIFFFile,
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

            # Add data type and image search string
            clem_img_series.search_string = str(output_file.parent / "*tiff")
            clem_img_series.data_type = (
                "atlas" if "Overview_" in result.series_name else "grid_square"
            )
            murfey_db.add(clem_img_series)
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
            return {"success": False, "requeue": False}

        try:
            # Load current session from database
            murfey_session = murfey_db.exec(
                select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
            ).one()

            # Determine variables to register data collection group and atlas with
            visit_name = murfey_session.visit
            proposal_code = "".join(
                char for char in visit_name.split("-")[0] if char.isalpha()
            )
            proposal_number = "".join(
                char for char in visit_name.split("-")[0] if char.isdigit()
            )
            visit_number = visit_name.split("-")[-1]

            # Generate name/tag for data colleciton group based on series name
            dcg_name = result.series_name.split("--")[0]
            if result.series_name.split("--")[1].isdigit():
                dcg_name += f"--{result.series_name.split('--')[1]}"

            # Determine values for atlas
            if "Overview_" in result.series_name:  # These are atlas datasets
                atlas_name = str(output_file.parent / "*.tiff")
                atlas_pixel_size = result.pixel_size
            else:
                atlas_name = ""
                atlas_pixel_size = 0.0

            registration_result: dict[str, bool]
            if dcg_search := murfey_db.exec(
                select(MurfeyDB.DataCollectionGroup)
                .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
                .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
            ).all():
                # Update atlas if registering atlas dataset
                # and data collection group already exists
                dcg_entry = dcg_search[0]
                if "Overview_" in result.series_name:
                    atlas_message = {
                        "session_id": session_id,
                        "dcgid": dcg_entry.id,
                        "atlas_id": dcg_entry.atlas_id,
                        "atlas": atlas_name,
                        "atlas_pixel_size": atlas_pixel_size,
                        "sample": dcg_entry.sample,
                    }
                    if entry_point_result := entry_points(
                        group="murfey.workflows", name="atlas_update"
                    ):
                        (workflow,) = entry_point_result
                        registration_result = workflow.load()(
                            message=atlas_message,
                            murfey_db=murfey_db,
                        )
                    else:
                        logger.warning("No workflow found for 'atlas_update'")
                        registration_result = {"success": False, "requeue": False}
                else:
                    registration_result = {"success": True}
            else:
                # Register data collection group
                dcg_message = {
                    "microscope": murfey_session.instrument_name,
                    "proposal_code": proposal_code,
                    "proposal_number": proposal_number,
                    "visit_number": visit_number,
                    "session_id": session_id,
                    "tag": dcg_name,
                    "experiment_type": "experiment",
                    "experiment_type_id": None,
                    "atlas": atlas_name,
                    "atlas_pixel_size": atlas_pixel_size,
                    "sample": None,
                }
                if entry_point_result := entry_points(
                    group="murfey.workflows", name="data_collection_group"
                ):
                    (workflow,) = entry_point_result
                    # Register grid square
                    registration_result = workflow.load()(
                        message=dcg_message,
                        murfey_db=murfey_db,
                    )
                else:
                    logger.warning("No workflow found for 'data_collection_group'")
                    registration_result = {"success": False, "requeue": False}
            if registration_result.get("success", False):
                logger.info(
                    "Successfully registered data collection group for CLEM workflow "
                    f"using{result.series_name!r}"
                )
            else:
                logger.warning(
                    "Failed to register data collection group for CLEM workflow "
                    f"using {result.series_name!r}"
                )

            # Store data collection group id in CLEM image series table
            dcg_entry = murfey_db.exec(
                select(MurfeyDB.DataCollectionGroup)
                .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
                .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
            ).one()
            clem_img_series.dcg_id = dcg_entry.id
            clem_img_series.dcg_name = dcg_entry.tag
            murfey_db.add(clem_img_series)
            murfey_db.commit()
        except Exception:
            logger.error(
                "Exception encountered when registering data collection group for CLEM workflow "
                f"using {result.series_name!r}: \n"
                f"{traceback.format_exc()}"
            )

        # Construct list of files to use for image alignment and merging steps
        image_combos_to_process = [
            list(result.output_files.values())  # Composite image of all channels
        ]
        # Create additional fluorescent-only and bright field-only jobs
        if ("gray" in result.output_files.keys()) and len(result.output_files) > 1:
            image_combos_to_process.append(
                [
                    file
                    for channel, file in result.output_files.items()
                    if channel != "gray"
                ]
            )
            image_combos_to_process.append(
                [
                    file
                    for channel, file in result.output_files.items()
                    if channel == "gray"
                ]
            )

        # Request for image alignment and processing for the requested combinations
        for image_combo in image_combos_to_process:
            try:
                submit_cluster_request(
                    session_id=session_id,
                    instrument_name=murfey_session.instrument_name,
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
                return {"success": False, "requeue": False}
        logger.info(
            "Successfully requested image alignment and merging job for "
            f"{result.series_name!r} series"
        )
        return {"success": True}

    finally:
        murfey_db.close()
