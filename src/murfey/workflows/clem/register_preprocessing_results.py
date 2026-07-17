"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
import traceback
from collections.abc import Collection
from functools import cached_property
from importlib.metadata import entry_points
from pathlib import Path
from typing import Literal, Optional, TypeAlias

from pydantic import BaseModel, computed_field
from sqlmodel import Session, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.util.models import GridSquareParameters
from murfey.util.processing_params import (
    default_clem_processing_parameters as processing_params,
)
from murfey.workflows.clem.align_and_merge import run as run_align_and_merge

logger = logging.getLogger("murfey.workflows.clem.register_preprocessing_results")

ColorChannels: TypeAlias = Literal[
    "gray", "red", "green", "blue", "cyan", "magenta", "yellow"
]

CC_MODES = ("_ICC", "_Lng_LVCC", "_Lng_SVCC")


class CLEMPreprocessingResult(BaseModel):
    series_name: str
    number_of_members: int
    is_stack: bool
    is_montage: bool
    output_files: dict[ColorChannels, Path]
    thumbnails: dict[ColorChannels, Path] = {}
    thumbnail_size: Optional[tuple[int, int]] = None  # height, width
    metadata: Path
    parent_lif: Optional[Path] = None
    parent_tiffs: dict[ColorChannels, list[Path]] = {}
    pixels_x: int
    pixels_y: int
    units: str
    pixel_size: float
    resolution: float
    extent: list[float]  # [x0, x1, y0, y1]

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def is_cc(self) -> bool:
        """
        The "_ICC", "_Lng_LVCC", and "_Lng_SVCC" suffixes appended to a CLEM dataset's
        position name indicate that it's a computationally cleared image set of the
        same position. They should override or supersede the original ones if present.
        """
        return any(self.series_name.endswith(pattern) for pattern in CC_MODES)

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def cc_mode(self) -> str | None:
        """
        Store the computational clearing mode used as an attribute
        """
        for pattern in CC_MODES:
            if self.series_name.endswith(pattern):
                return pattern[1:]
        return None

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def site_name(self) -> str:
        """
        Extract just the name of the site by removing the clearing mode suffix from
        the series name.
        """
        if self.cc_mode is not None:
            return self.series_name[: -(len(self.cc_mode) + 1)]
        return self.series_name

    # Valid Pydantic decorator not supported by MyPy
    @computed_field  # type: ignore
    @cached_property
    def is_atlas(self) -> bool:
        """
        Incoming image sets with a width/height greater/equal to the pre-set threshold
        should qualify as an atlas.
        """
        return (
            max(
                self.pixels_x * self.pixel_size,
                self.pixels_y * self.pixel_size,
            )
            >= processing_params.atlas_threshold
        )


COLOR_FLAGS_MURFEY = {
    "gray": "has_grey",
    "red": "has_red",
    "green": "has_green",
    "blue": "has_blue",
    "cyan": "has_cyan",
    "magenta": "has_magenta",
    "yellow": "has_yellow",
}


def _get_color_flags(
    colors: Collection[str] | None = None,
):
    colors = colors or []
    color_flags = dict.fromkeys(COLOR_FLAGS_MURFEY.values(), False)
    for color in colors:
        color_flags[COLOR_FLAGS_MURFEY[color]] = True
    return color_flags


def _register_clem_imaging_site(
    session_id: int,
    result: CLEMPreprocessingResult,
    murfey_db: Session,
):
    """
    Creates an ImagingSite database entry for the current CLEM preprocessing result
    if one doesn't already exist, or modifies the existing one if it does. Each entry
    corresponds to a unique site on the sample grid, and results containing cleared
    data will supersede existing rows for the same position that contain only raw
    data. Returns the created/queried entry.
    """

    def _populate(
        entry: MurfeyDB.ImagingSite,
        result: CLEMPreprocessingResult,
    ):
        """
        Helper function to populate the ImagingSite column values.
        """

        # Is this an atlas or grid square
        entry.data_type = "atlas" if result.is_atlas else "grid_square"
        # Register file paths
        output_file = list(result.output_files.values())[0]
        entry.image_path = str(output_file.parent / "*.tiff")
        # Shape and resolution information
        entry.image_pixels_x = result.pixels_x
        entry.image_pixels_y = result.pixels_y
        entry.image_pixel_size = result.pixel_size
        entry.units = result.units
        # Extent of imaged area in real space
        entry.x0 = result.extent[0]
        entry.x1 = result.extent[1]
        entry.y0 = result.extent[2]
        entry.y1 = result.extent[3]

        # Iteratively add colour channel information
        entry.number_of_members = result.number_of_members
        for col_name, value in _get_color_flags(result.output_files.keys()).items():
            setattr(entry, col_name, value)
        entry.collection_mode = _determine_collection_mode(result.output_files.keys())

        # Register thumbnail information if present
        if result.thumbnails and result.thumbnail_size:
            thumbnail = list(result.thumbnails.values())[0]
            entry.thumbnail_path = str(thumbnail.parent / "*.png")

            thumbnail_height, thumbnail_width = result.thumbnail_size
            scaling_factor = min(
                thumbnail_height / result.pixels_y, thumbnail_width / result.pixels_x
            )
            entry.thumbnail_pixel_size = result.pixel_size / scaling_factor
            entry.thumbnail_pixels_x = int(round(result.pixels_x * scaling_factor)) or 1
            entry.thumbnail_pixels_y = int(round(result.pixels_y * scaling_factor)) or 1
        return entry

    # Create a new entry if one doesn't already exist
    if not (
        clem_img_site := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.site_name == result.site_name)
        ).one_or_none()
    ):
        clem_img_site = MurfeyDB.ImagingSite(
            session_id=session_id,
            site_name=result.site_name,
        )
        clem_img_site = _populate(clem_img_site, result)

    # Prepare to overwrite existing entry if current result is a cleared dataset
    if result.is_cc:
        # Proceed with overwrite if current result is different from existing entry
        output_file = list(result.output_files.values())[0]
        if str(output_file.parent / "*.tiff") != clem_img_site.image_path:
            clem_img_site = _populate(clem_img_site, result)

    # Commit changes and return entry
    murfey_db.add(clem_img_site)
    murfey_db.commit()
    logger.info(f"CLEM preprocessing results registered for {result.series_name!r} ")
    return clem_img_site


def _determine_collection_mode(
    colors: Collection[str] | None = None,
):
    if not colors:
        logger.warning("No colours were present in returned result")
        return None
    if "gray" in colors:
        if len(colors) == 1:
            return "Bright Field"
        else:
            return "Bright Field and Fluorescent"
    else:
        return "Fluorescent"


def _snake_to_camel_case(string: str):
    parts = string.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


COLOR_FLAGS_MURFEY_TO_ISPYB = {
    value: _snake_to_camel_case(value) for value in COLOR_FLAGS_MURFEY.values()
}


def _register_dcg_and_atlas(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    imaging_site: MurfeyDB.ImagingSite,
    murfey_db: Session,
):
    """
    Takes an ImagingSite entry and uses it to create and register DataCollectionGroup
    entries in ISPyB if they don't already exist, or to populate existing entries.
    After doing so, it will register the DataCollectionGroup ID in Murfey and add it
    to the ImagingSite entry.
    """
    # Determine variables to register data collection group and atlas with
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Generate name/tag for data colleciton group based on series name
    dcg_name = imaging_site.site_name.split("--")[0]
    if imaging_site.site_name.split("--")[1].isdigit():
        dcg_name += f"--{imaging_site.site_name.split('--')[1]}"

    # Determine values for atlas
    if is_atlas := imaging_site.data_type == "atlas":
        # Register using thumbnail values if they are provided
        if (
            imaging_site.thumbnail_path is not None
            and imaging_site.thumbnail_pixel_size is not None
        ):
            atlas_name: str | None = imaging_site.thumbnail_path
            atlas_pixel_size: float | None = imaging_site.thumbnail_pixel_size
        # Otherwise, register the TIFF files themselves
        else:
            atlas_name = imaging_site.image_path
            atlas_pixel_size = imaging_site.image_pixel_size
        # Translate colour flags into ISPyB convention
        color_flags = {
            COLOR_FLAGS_MURFEY_TO_ISPYB[key]: getattr(imaging_site, key, 0)
            for key in COLOR_FLAGS_MURFEY_TO_ISPYB.keys()
        }
        collection_mode = imaging_site.collection_mode
    else:
        atlas_name = ""
        atlas_pixel_size = 0.0
        color_flags = None
        collection_mode = None

    if dcg_search := murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
    ).all():
        dcg_entry = dcg_search[0]
        # Update if current dataset is atlas and data collection group exists
        if is_atlas:
            atlas_message = {
                "session_id": session_id,
                "dcgid": dcg_entry.id,
                "atlas_id": dcg_entry.atlas_id,
                "atlas": atlas_name,
                "atlas_pixel_size": atlas_pixel_size,
                "sample": dcg_entry.sample,
                "color_flags": color_flags,
                "collection_mode": collection_mode,
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
            "color_flags": color_flags,
            "collection_mode": collection_mode,
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

    imaging_site.dcg_id = dcg_entry.id
    imaging_site.dcg_name = dcg_entry.tag
    murfey_db.add(imaging_site)
    murfey_db.commit()


def _register_grid_square(
    session_id: int,
    imaging_site: MurfeyDB.ImagingSite,
    murfey_db: Session,
):
    # Skip this step if no transport manager object is configured
    if murfey.server._transport_object is None:
        logger.error("Unable to find transport manager")
        return
    if (dcg_name := imaging_site.dcg_name) is None:
        logger.warning("Current imaging site has no data collection group name")
        return

    # Check if an atlas has been registered
    if not (
        # Sort by ascending insertion order
        atlas_results := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
            .where(MurfeyDB.ImagingSite.data_type == "atlas")
            .order_by(MurfeyDB.ImagingSite.id)
        ).all()
    ):
        logger.info(
            f"No atlas has been registered for data collection group {dcg_name!r} yet"
        )
        return
    atlas_entry = atlas_results[-1]  # Use the latest registered atlas

    # Check if there are CLEM entries to register
    if clem_img_site_to_register := murfey_db.exec(
        select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == session_id)
        .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
        .where(MurfeyDB.ImagingSite.data_type == "grid_square")
    ).all():
        if (
            atlas_entry.x0 is not None
            and atlas_entry.x1 is not None
            and atlas_entry.y0 is not None
            and atlas_entry.y1 is not None
            and atlas_entry.thumbnail_pixels_x is not None
            and atlas_entry.thumbnail_pixels_y is not None
        ):
            atlas_width_real = atlas_entry.x1 - atlas_entry.x0
            atlas_height_real = atlas_entry.y1 - atlas_entry.y0
        else:
            logger.warning("Atlas entry not populated with required values")
            return

        for clem_img_site in clem_img_site_to_register:
            # Register datasets using thumbnail sizes and scales
            if (
                clem_img_site.x0 is not None
                and clem_img_site.x1 is not None
                and clem_img_site.y0 is not None
                and clem_img_site.y1 is not None
            ):
                # Find the real coordinates of the image midpoint
                x_mid_real = 0.5 * (clem_img_site.x0 + clem_img_site.x1)
                y_mid_real = 0.5 * (clem_img_site.y0 + clem_img_site.y1)

                # Find pixel coordinates corresponding to image midpoint on atlas
                x_mid_px = int(
                    round(
                        (x_mid_real - atlas_entry.x0)
                        / atlas_width_real
                        * atlas_entry.thumbnail_pixels_x
                    )
                )
                y_mid_px = int(
                    round(
                        (y_mid_real - atlas_entry.y0)
                        / atlas_height_real
                        * atlas_entry.thumbnail_pixels_y
                    )
                )

                # Find the size of the image, in pixels, when overlaid on the atlas
                width_scaled = int(
                    round(
                        (clem_img_site.x1 - clem_img_site.x0)
                        / atlas_width_real
                        * atlas_entry.thumbnail_pixels_x
                    )
                    or 1
                )
                height_scaled = int(
                    round(
                        (clem_img_site.y1 - clem_img_site.y0)
                        / atlas_height_real
                        * atlas_entry.thumbnail_pixels_y
                    )
                    or 1
                )
            else:
                logger.warning(
                    f"Image series {clem_img_site.site_name!r} not populated with required values"
                )
                continue

            # Populate grid square Pydantic model
            grid_square_params = GridSquareParameters(
                tag=dcg_name,
                x_location=clem_img_site.x0,
                x_location_scaled=x_mid_px,
                y_location=clem_img_site.y0,
                y_location_scaled=y_mid_px,
                readout_area_x=clem_img_site.image_pixels_x,
                readout_area_y=clem_img_site.image_pixels_y,
                thumbnail_size_x=clem_img_site.thumbnail_pixels_x,
                thumbnail_size_y=clem_img_site.thumbnail_pixels_y,
                width=clem_img_site.image_pixels_x,
                width_scaled=width_scaled,
                height=clem_img_site.image_pixels_y,
                height_scaled=height_scaled,
                x_stage_position=0.5 * (clem_img_site.x0 + clem_img_site.x1),
                y_stage_position=0.5 * (clem_img_site.y0 + clem_img_site.y1),
                pixel_size=clem_img_site.image_pixel_size,
                image=clem_img_site.thumbnail_path,
                collection_mode=clem_img_site.collection_mode,
            )
            # Construct colour flags for ISPyB
            color_flags = {
                ispyb_color_flags: int(getattr(clem_img_site, murfey_color_flags, 0))
                for murfey_color_flags, ispyb_color_flags in COLOR_FLAGS_MURFEY_TO_ISPYB.items()
            }
            # Register or update the grid square entry as required
            if grid_square_entry := murfey_db.exec(
                select(MurfeyDB.GridSquare)
                .where(MurfeyDB.GridSquare.name == clem_img_site.id)
                .where(MurfeyDB.GridSquare.tag == grid_square_params.tag)
                .where(MurfeyDB.GridSquare.session_id == session_id)
            ).one_or_none():
                # Update existing grid square entry on Murfey
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
                murfey.server._transport_object.do_update_grid_square(
                    grid_square_id=grid_square_entry.id,
                    grid_square_parameters=grid_square_params,
                    color_flags=color_flags,
                )
            else:
                # Look up data collection group for current series
                dcg_entry = murfey_db.exec(
                    select(MurfeyDB.DataCollectionGroup)
                    .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
                    .where(MurfeyDB.DataCollectionGroup.tag == grid_square_params.tag)
                ).one()
                # Register to ISPyB
                grid_square_ispyb_result = (
                    murfey.server._transport_object.do_insert_grid_square(
                        atlas_id=dcg_entry.atlas_id,
                        grid_square_id=clem_img_site.id,
                        grid_square_parameters=grid_square_params,
                        color_flags=color_flags,
                    )
                )
                # Register to Murfey
                grid_square_entry = MurfeyDB.GridSquare(
                    id=grid_square_ispyb_result.get("return_value", None),
                    name=clem_img_site.id,
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

            # Add grid square ID to existing CLEM image series entry
            clem_img_site.grid_square_id = grid_square_entry.id
            murfey_db.add(clem_img_site)

        # Do one commit at the end
        murfey_db.commit()
    else:
        logger.info(
            f"No grid squares to register for data collection group {dcg_name!r} yet"
        )
    return


def run(message: dict, murfey_db: Session) -> dict[str, bool]:
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
            clem_img_site = _register_clem_imaging_site(
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
                imaging_site=clem_img_site,
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
                imaging_site=clem_img_site,
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
                run_align_and_merge(
                    session_id=session_id,
                    instrument_name=murfey_session.instrument_name,
                    series_name=result.series_name,
                    images=image_combo,
                    metadata=result.metadata,
                    messenger=murfey.server._transport_object,
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
