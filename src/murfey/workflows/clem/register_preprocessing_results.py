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
from murfey.util.models import GridSquareParameters
from murfey.util.processing_params import (
    default_clem_processing_parameters as processing_params,
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
    thumbnails: dict[
        Literal["gray", "red", "green", "blue", "cyan", "magenta", "yellow"], Path
    ] = {}
    thumbnail_size: Optional[tuple[int, int]] = None  # height, width
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
    extent: list[float]  # [x0, x1, y0, y1]


def _is_clem_atlas(result: CLEMPreprocessingResult):
    # If an image has a width/height of at least 1.5 mm, it should qualify as an atlas
    return (
        max(
            result.pixels_x * result.pixel_size,
            result.pixels_y * result.pixel_size,
        )
        >= processing_params.atlas_threshold
    )


def _register_clem_image_series(
    session_id: int,
    result: CLEMPreprocessingResult,
    murfey_db: Session,
):
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

    # Link and commit series and metadata tables
    clem_img_series.associated_metadata = clem_metadata
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
                tiff_list = list(seed_file.parent.glob(f"{series_identifier}--"))

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

    # Add metadata for this series
    clem_img_series.image_search_string = str(output_file.parent / "*tiff")
    clem_img_series.data_type = "atlas" if _is_clem_atlas(result) else "grid_square"
    clem_img_series.number_of_members = result.number_of_members
    clem_img_series.image_pixels_x = result.pixels_x
    clem_img_series.image_pixels_y = result.pixels_y
    clem_img_series.image_pixel_size = result.pixel_size
    clem_img_series.units = result.units
    clem_img_series.x0 = result.extent[0]
    clem_img_series.x1 = result.extent[1]
    clem_img_series.y0 = result.extent[2]
    clem_img_series.y1 = result.extent[3]
    # Register thumbnails if they are present
    if result.thumbnails and result.thumbnail_size:
        thumbnail = list(result.thumbnails.values())[0]
        clem_img_series.thumbnail_search_string = str(thumbnail.parent / "*.png")

        thumbnail_height, thumbnail_width = result.thumbnail_size
        scaling_factor = min(
            thumbnail_height / result.pixels_y, thumbnail_width / result.pixels_x
        )
        clem_img_series.thumbnail_pixel_size = result.pixel_size / scaling_factor
        clem_img_series.thumbnail_pixels_x = int(result.pixels_x * scaling_factor)
        clem_img_series.thumbnail_pixels_y = int(result.pixels_y * scaling_factor)
    murfey_db.add(clem_img_series)
    murfey_db.commit()
    murfey_db.close()

    logger.info(f"CLEM preprocessing results registered for {result.series_name!r} ")


def _register_dcg_and_atlas(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    result: CLEMPreprocessingResult,
    murfey_db: Session,
):
    # Determine variables to register data collection group and atlas with
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Generate name/tag for data colleciton group based on series name
    dcg_name = result.series_name.split("--")[0]
    if result.series_name.split("--")[1].isdigit():
        dcg_name += f"--{result.series_name.split('--')[1]}"

    # Determine values for atlas
    if _is_clem_atlas(result):
        output_file = list(result.output_files.values())[0]
        # Register the thumbnail entries if they are provided
        if result.thumbnails and result.thumbnail_size is not None:
            # Glob path to the thumbnail files
            thumbnail = list(result.thumbnails.values())[0]
            atlas_name = str(thumbnail.parent / "*.png")

            # Work out the scaling factor used
            thumbnail_height, thumbnail_width = result.thumbnail_size
            scaling_factor = min(
                thumbnail_width / result.pixels_x,
                thumbnail_height / result.pixels_y,
            )
            atlas_pixel_size = result.pixel_size / scaling_factor
        # Otherwise, register the TIFF files themselves
        else:
            atlas_name = str(output_file.parent / "*.tiff")
            atlas_pixel_size = result.pixel_size
    else:
        atlas_name = ""
        atlas_pixel_size = 0.0

    if dcg_search := murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
    ).all():
        dcg_entry = dcg_search[0]
        # Update atlas if registering atlas dataset
        # and data collection group already exists
        if _is_clem_atlas(result):
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
                _ = workflow.load()(
                    message=atlas_message,
                    murfey_db=murfey_db,
                )
            else:
                logger.warning("No workflow found for 'atlas_update'")
    else:
        # Register data collection group and placeholder for the atlas
        dcg_message = {
            "microscope": instrument_name,
            "proposal_code": proposal_code,
            "proposal_number": proposal_number,
            "visit_number": visit_number,
            "session_id": session_id,
            "tag": dcg_name,
            "experiment_type_id": 45,
            "atlas": atlas_name,
            "atlas_pixel_size": atlas_pixel_size,
            "sample": None,
        }
        if entry_point_result := entry_points(
            group="murfey.workflows", name="data_collection_group"
        ):
            (workflow,) = entry_point_result
            # Register grid square
            _ = workflow.load()(
                message=dcg_message,
                murfey_db=murfey_db,
            )
        else:
            logger.warning("No workflow found for 'data_collection_group'")

    # Store data collection group id in CLEM image series table
    dcg_entry = murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
    ).one()

    clem_img_series: MurfeyDB.CLEMImageSeries = get_db_entry(
        db=murfey_db,
        table=MurfeyDB.CLEMImageSeries,
        session_id=session_id,
        series_name=result.series_name,
    )
    clem_img_series.dcg_id = dcg_entry.id
    clem_img_series.dcg_name = dcg_entry.tag
    murfey_db.add(clem_img_series)
    murfey_db.commit()
    murfey_db.close()


def _register_grid_square(
    session_id: int,
    result: CLEMPreprocessingResult,
    murfey_db: Session,
):
    # Skip this step if no transport manager object is configured
    if _transport_object is None:
        logger.error("Unable to find transport manager")
        return
    # Load all entries for the current data collection group
    dcg_name = result.series_name.split("--")[0]
    if result.series_name.split("--")[1].isdigit():
        dcg_name += f"--{result.series_name.split('--')[1]}"

    # Check if an atlas has been registered
    if atlas_search := murfey_db.exec(
        select(MurfeyDB.CLEMImageSeries)
        .where(MurfeyDB.CLEMImageSeries.session_id == session_id)
        .where(MurfeyDB.CLEMImageSeries.dcg_name == dcg_name)
        .where(MurfeyDB.CLEMImageSeries.data_type == "atlas")
    ).all():
        atlas_entry = atlas_search[0]
    else:
        logger.info(
            f"No atlas has been registered for data collection group {dcg_name!r} yet"
        )
        return

    # Check if there are CLEM entries to register
    if clem_img_series_to_register := murfey_db.exec(
        select(MurfeyDB.CLEMImageSeries)
        .where(MurfeyDB.CLEMImageSeries.session_id == session_id)
        .where(MurfeyDB.CLEMImageSeries.dcg_name == dcg_name)
        .where(MurfeyDB.CLEMImageSeries.data_type == "grid_square")
    ):
        if (
            atlas_entry.x0 is not None
            and atlas_entry.x1 is not None
            and atlas_entry.y0 is not None
            and atlas_entry.y1 is not None
        ):
            atlas_width_real = atlas_entry.x1 - atlas_entry.x0
            atlas_height_real = atlas_entry.y1 - atlas_entry.y0
        else:
            logger.warning("Atlas entry not populated with required values")
            return

        for clem_img_series in clem_img_series_to_register:
            # Register datasets using thumbnail sizes and scales
            if (
                clem_img_series.x0 is not None
                and clem_img_series.x1 is not None
                and clem_img_series.y0 is not None
                and clem_img_series.y1 is not None
                and clem_img_series.thumbnail_pixels_x is not None
                and clem_img_series.thumbnail_pixels_y is not None
                and clem_img_series.thumbnail_pixel_size is not None
            ):
                # Find pixel corresponding to image midpoint on atlas
                x_mid_real = (
                    0.5 * (clem_img_series.x0 + clem_img_series.x1) - atlas_entry.x0
                )
                x_mid_px = int(
                    x_mid_real / atlas_width_real * clem_img_series.thumbnail_pixels_x
                )
                y_mid_real = (
                    0.5 * (clem_img_series.y0 + clem_img_series.y1) - atlas_entry.y0
                )
                y_mid_px = int(
                    y_mid_real / atlas_height_real * clem_img_series.thumbnail_pixels_y
                )

                # Find the size of the image, in pixels, when overlaid the atlas
                width_scaled = int(
                    (clem_img_series.x1 - clem_img_series.x0)
                    / atlas_width_real
                    * clem_img_series.thumbnail_pixels_x
                )
                height_scaled = int(
                    (clem_img_series.y1 - clem_img_series.y0)
                    / atlas_height_real
                    * clem_img_series.thumbnail_pixels_y
                )
            else:
                logger.warning(
                    f"Image series {clem_img_series.series_name!r} not populated with required values"
                )
                continue

            # Populate grid square Pydantic model
            grid_square_params = GridSquareParameters(
                tag=dcg_name,
                x_location=clem_img_series.x0,
                x_location_scaled=x_mid_px,
                y_location=clem_img_series.y0,
                y_location_scaled=y_mid_px,
                readout_area_x=clem_img_series.image_pixels_x,
                readout_area_y=clem_img_series.image_pixels_y,
                thumbnail_size_x=clem_img_series.thumbnail_pixels_x,
                thumbnail_size_y=clem_img_series.thumbnail_pixels_y,
                width=clem_img_series.image_pixels_x,
                width_scaled=width_scaled,
                height=clem_img_series.image_pixels_y,
                height_scaled=height_scaled,
                x_stage_position=0.5 * (clem_img_series.x0 + clem_img_series.x1),
                y_stage_position=0.5 * (clem_img_series.y0 + clem_img_series.y1),
                pixel_size=clem_img_series.image_pixel_size,
                image=clem_img_series.thumbnail_search_string,
            )
            # Register or update the grid square entry as required
            if grid_square_result := murfey_db.exec(
                select(MurfeyDB.GridSquare)
                .where(MurfeyDB.GridSquare.name == clem_img_series.id)
                .where(MurfeyDB.GridSquare.tag == grid_square_params.tag)
                .where(MurfeyDB.GridSquare.session_id == session_id)
            ).all():
                # Update existing grid square entry on Murfey
                grid_square_entry = grid_square_result[0]
                grid_square_entry.x_location = grid_square_params.x_location
                grid_square_entry.y_location = grid_square_params.y_location
                grid_square_entry.x_stage_position = grid_square_params.x_stage_position
                grid_square_entry.y_stage_position = grid_square_params.y_stage_position
                grid_square_entry.readout_area_x = grid_square_params.readout_area_x
                grid_square_entry.readout_area_y = grid_square_params.readout_area_y
                grid_square_entry.thumbnail_size_x = grid_square_params.thumbnail_size_x
                grid_square_entry.thumbnail_size_y = grid_square_params.thumbnail_size_y
                grid_square_entry.pixel_size = grid_square_params.pixel_size
                grid_square_entry.image = grid_square_params.image

                # Update existing entry on ISPyB
                _transport_object.do_update_grid_square(
                    grid_square_id=grid_square_entry.id,
                    grid_square_parameters=grid_square_params,
                )
            else:
                # Look up data collection group for current series
                dcg_entry = murfey_db.exec(
                    select(MurfeyDB.DataCollectionGroup)
                    .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
                    .where(MurfeyDB.DataCollectionGroup.tag == grid_square_params.tag)
                ).one()
                # Register to ISPyB
                grid_square_ispyb_result = _transport_object.do_insert_grid_square(
                    atlas_id=dcg_entry.atlas_id,
                    grid_square_id=clem_img_series.id,
                    grid_square_parameters=grid_square_params,
                )
                # Register to Murfey
                grid_square_entry = MurfeyDB.GridSquare(
                    id=grid_square_ispyb_result.get("return_value", None),
                    name=clem_img_series.id,
                    session_id=session_id,
                    tag=grid_square_params.tag,
                    x_location=grid_square_params.x_location,
                    y_location=grid_square_params.y_location,
                    x_stage_position=grid_square_params.x_stage_position,
                    y_stage_position=grid_square_params.y_stage_position,
                    readout_area_x=grid_square_params.readout_area_x,
                    readout_area_y=grid_square_params.readout_area_y,
                    thumbnail_size_x=grid_square_params.thumbnail_size_x,
                    thumbnail_size_y=grid_square_params.thumbnail_size_y,
                    pixel_size=grid_square_params.pixel_size,
                    image=grid_square_params.image,
                )
            murfey_db.add(grid_square_entry)
            murfey_db.commit()

            # Add grid square ID to existing CLEM image series entry
            clem_img_series.grid_square_id = grid_square_entry.id
            murfey_db.add(clem_img_series)
            murfey_db.commit()
    else:
        logger.info(
            f"No grid squares to register for data collection group {dcg_name!r} yet"
        )
    murfey_db.close()
    return


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
        try:
            # Load current session from database
            murfey_session = murfey_db.exec(
                select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
            ).one()
        except Exception:
            logger.error(
                "Exception encountered when loading Murfey session information: \n",
                f"{traceback.format_exc()}",
            )
            return {"success": False, "requeue": False}
        try:
            # Register items in Murfey database
            _register_clem_image_series(
                session_id=session_id,
                result=result,
                murfey_db=murfey_db,
            )
        except Exception:
            logger.error(
                "Exception encountered when registering CLEM preprocessing result for "
                f"{result.series_name!r}: \n"
                f"{traceback.format_exc()}"
            )
            return {"success": False, "requeue": False}
        try:
            # Register data collection group and atlas in ISPyB
            _register_dcg_and_atlas(
                session_id=session_id,
                instrument_name=murfey_session.instrument_name,
                visit_name=murfey_session.visit,
                result=result,
                murfey_db=murfey_db,
            )
        except Exception:
            # Log error but allow workflow to proceed
            logger.error(
                "Exception encountered when registering data collection group for CLEM workflow "
                f"using {result.series_name!r}: \n"
                f"{traceback.format_exc()}"
            )

        try:
            # Register CLEM image series as grid squares
            _register_grid_square(
                session_id=session_id,
                result=result,
                murfey_db=murfey_db,
            )
        except Exception:
            # Log error but allow workflow to proceed
            logger.error(
                f"Exception encountered when registering grid square for {result.series_name}: \n"
                f"{traceback.format_exc()}"
            )

        # Construct list of files to use for image alignment and merging steps
        image_combos_to_process = [
            list(result.output_files.values())  # Composite image of all channels
        ]
        if ("gray" in result.output_files.keys()) and len(result.output_files) > 1:
            # Create additional fluorescent-only composite image
            image_combos_to_process.append(
                [
                    file
                    for channel, file in result.output_files.items()
                    if channel != "gray"
                ]
            )
            # Create additional bright field-only image
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
