"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Optional, TypeAlias

from pydantic import BaseModel, computed_field
from sqlmodel import Session, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.util.models import GridSquareParameters
from murfey.util.processing_params import (
    default_clem_processing_parameters as processing_params,
)
from murfey.workflows.register_data_collection_group import register_dcg

if TYPE_CHECKING:
    from murfey.server.ispyb import TransportManager

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


def _populate_clem_imaging_site_entry(
    entry: MurfeyDB.ImagingSite,
    result: CLEMPreprocessingResult,
):
    """
    Helper function to populate the ImagingSite column values with CLEM data.
    """
    # Is this an atlas or grid square
    entry.data_type = "atlas" if result.is_atlas else "grid_square"
    # Register expected DataCollectionGroup name
    entry.dcg_name = result.site_name.split("/")[0]
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
        clem_img_site = _populate_clem_imaging_site_entry(clem_img_site, result)

    # Prepare to overwrite existing entry if current result is a cleared dataset
    if result.is_cc:
        # Proceed with overwrite if current result is different from existing entry
        output_file = list(result.output_files.values())[0]
        if str(output_file.parent / "*.tiff") != clem_img_site.image_path:
            clem_img_site = _populate_clem_imaging_site_entry(clem_img_site, result)

    # Commit changes and return entry
    murfey_db.add(clem_img_site)
    murfey_db.commit()
    logger.info(f"CLEM preprocessing results registered for {result.series_name!r} ")
    return clem_img_site


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
    transport_object: TransportManager,
    murfey_db: Session,
):
    """
    Takes an ImagingSite entry and uses it to create and register DataCollectionGroup
    entries in ISPyB if they don't already exist, or to populate existing entries.
    After doing so, it will register the DataCollectionGroup ID in Murfey and add it
    to the ImagingSite entry.
    """
    # 'dcg_name' should already be populated as part of initial ImagingSite registration
    if (dcg_name := imaging_site.dcg_name) is None:
        raise ValueError(
            f"'dcg_name' not assigned to ImagingSite entry for {imaging_site.image_path}"
        )

    # Determine variables to register data collection group and atlas with
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Check if there are already atlas candidates for this DataCollectionGroup
    # Only the largest of these should be retained as the atlas
    if atlas_candidates := list(
        murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
            .where(MurfeyDB.ImagingSite.data_type == "atlas")
            .order_by(MurfeyDB.ImagingSite.id)
        ).all()
    ):
        atlas_candidates.append(imaging_site)
        atlas_areas: dict[int, float] = {}
        for i, atlas_candidate in enumerate(atlas_candidates):
            # Ensure that the fields needed are not empty
            if not (
                atlas_candidate.x0 is not None
                and atlas_candidate.x1 is not None
                and atlas_candidate.y0 is not None
                and atlas_candidate.y1 is not None
            ):
                continue
            atlas_areas[i] = (atlas_candidate.x1 - atlas_candidate.x0) * (
                atlas_candidate.y1 - atlas_candidate.y0
            )
        # Find index of the largest atlas
        largest_atlas = int(max(atlas_areas, key=lambda k: atlas_areas[k]))

        # Re-label all other atlas candidates as "grid_squares"
        # This will also update the 'imaging_site' entry that was appended
        for i, atlas_candidate in enumerate(atlas_candidates):
            if i == largest_atlas:
                continue
            atlas_candidate.data_type == "grid_square"
            murfey_db.add(atlas_candidate)

    # Determine values for atlas
    is_atlas = imaging_site.data_type == "atlas"
    if is_atlas:
        # Register using thumbnail values
        if not (
            imaging_site.thumbnail_path is not None
            and imaging_site.thumbnail_pixel_size is not None
        ):
            raise ValueError(
                f"ImagingSIte entry for {imaging_site.image_path} missing one or more required values:\n"
                "  'thumbnail_path'\n"
                "  'thumbnail_pixel_size'\n"
            )
        atlas_path = imaging_site.thumbnail_path
        atlas_pixel_size = imaging_site.thumbnail_pixel_size

        # Translate colour flags into ISPyB convention
        color_flags = {
            COLOR_FLAGS_MURFEY_TO_ISPYB[key]: getattr(imaging_site, key, 0)
            for key in COLOR_FLAGS_MURFEY_TO_ISPYB.keys()
        }
        collection_mode = imaging_site.collection_mode
    else:
        atlas_path = ""
        atlas_pixel_size = 0.0
        color_flags = None
        collection_mode = None

    if dcg_search := murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
        .order_by(MurfeyDB.DataCollectionGroup.id)
    ).all():
        dcg_entry = dcg_search[0]
        # Update if current dataset is atlas and data collection group exists
        if is_atlas:
            transport_object.do_update_atlas(
                atlas_id=dcg_entry.atlas_id,
                atlas_image=atlas_path,
                pixel_size=atlas_pixel_size,
                slot=dcg_entry.sample,
                # Extract optional parameters
                collection_mode=collection_mode,
                color_flags=color_flags,
            )
            # Update atlas path on Murfey DataCollectionGroup record as well
            dcg_entry.atlas = atlas_path
            murfey_db.add(dcg_entry)
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
            "atlas": atlas_path,
            "atlas_pixel_size": atlas_pixel_size,
            "sample": None,
            "color_flags": color_flags,
            "collection_mode": collection_mode,
        }
        dcg_entry = register_dcg(
            message=dcg_message,
            murfey_db=murfey_db,
        )
        if not dcg_entry:
            raise RuntimeError(
                f"Could not register DataCollectionGroup entry for {imaging_site.image_path}"
            )
    # Update imaging site with new information
    imaging_site.dcg_id = dcg_entry.id
    imaging_site.dcg_name = dcg_entry.tag
    murfey_db.add(imaging_site)
    murfey_db.commit()

    return imaging_site


def _register_single_grid_square(
    session_id: int,
    dcg_name: str,
    atlas_entry: MurfeyDB.ImagingSite,
    imaging_site: MurfeyDB.ImagingSite,
    transport_object: TransportManager,
    murfey_db: Session,
):
    """
    Helper function containing the logic for registering/updating a single GridSquare
    entry in ISPyB, and creating/updating the accompanying record in Murfey.
    """
    if not (
        atlas_entry.x0 is not None
        and atlas_entry.x1 is not None
        and atlas_entry.y0 is not None
        and atlas_entry.y1 is not None
        and atlas_entry.thumbnail_pixels_x is not None
        and atlas_entry.thumbnail_pixels_y is not None
    ):
        raise ValueError(
            f"Atlas ImagingSite {imaging_site.image_path} not populated with required values"
        )
    atlas_width_real = atlas_entry.x1 - atlas_entry.x0
    atlas_height_real = atlas_entry.y1 - atlas_entry.y0

    # Register datasets using thumbnail sizes and scales
    if not (
        imaging_site.id is not None
        and imaging_site.x0 is not None
        and imaging_site.x1 is not None
        and imaging_site.y0 is not None
        and imaging_site.y1 is not None
    ):
        raise ValueError(
            f"ImagingSite for {imaging_site.image_path} not populated with required values"
        )
    # Find the real coordinates of the image midpoint
    x_mid_real = 0.5 * (imaging_site.x0 + imaging_site.x1)
    y_mid_real = 0.5 * (imaging_site.y0 + imaging_site.y1)

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
            (imaging_site.x1 - imaging_site.x0)
            / atlas_width_real
            * atlas_entry.thumbnail_pixels_x
        )
        or 1
    )
    height_scaled = int(
        round(
            (imaging_site.y1 - imaging_site.y0)
            / atlas_height_real
            * atlas_entry.thumbnail_pixels_y
        )
        or 1
    )

    # Populate grid square Pydantic model
    grid_square_params = GridSquareParameters(
        tag=dcg_name,
        x_location=imaging_site.x0,
        x_location_scaled=x_mid_px,
        y_location=imaging_site.y0,
        y_location_scaled=y_mid_px,
        readout_area_x=imaging_site.image_pixels_x,
        readout_area_y=imaging_site.image_pixels_y,
        thumbnail_size_x=imaging_site.thumbnail_pixels_x,
        thumbnail_size_y=imaging_site.thumbnail_pixels_y,
        width=imaging_site.image_pixels_x,
        width_scaled=width_scaled,
        height=imaging_site.image_pixels_y,
        height_scaled=height_scaled,
        x_stage_position=0.5 * (imaging_site.x0 + imaging_site.x1),
        y_stage_position=0.5 * (imaging_site.y0 + imaging_site.y1),
        pixel_size=imaging_site.image_pixel_size,
        image=imaging_site.thumbnail_path,
        collection_mode=imaging_site.collection_mode,
    )
    # Construct colour flags for ISPyB
    color_flags = {
        ispyb_color_flags: int(getattr(imaging_site, murfey_color_flags, 0))
        for murfey_color_flags, ispyb_color_flags in COLOR_FLAGS_MURFEY_TO_ISPYB.items()
    }
    # Register or update the grid square entry as required
    if grid_square_entry := murfey_db.exec(
        select(MurfeyDB.GridSquare)
        .where(MurfeyDB.GridSquare.name == imaging_site.id)
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
        transport_object.do_update_grid_square(
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
        grid_square_ispyb_result = transport_object.do_insert_grid_square(
            atlas_id=dcg_entry.atlas_id,
            grid_square_id=imaging_site.id,
            grid_square_parameters=grid_square_params,
            color_flags=color_flags,
        )
        # Register to Murfey
        grid_square_entry = MurfeyDB.GridSquare(
            id=grid_square_ispyb_result.get("return_value", None),
            name=imaging_site.id,
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
    imaging_site.grid_square_id = grid_square_entry.id
    murfey_db.add(imaging_site)

    # Commit once at the end
    murfey_db.commit()
    return imaging_site


def _register_grid_square(
    session_id: int,
    imaging_site: MurfeyDB.ImagingSite,
    transport_object: TransportManager,
    murfey_db: Session,
):
    if (dcg_name := imaging_site.dcg_name) is None:
        raise ValueError(
            f"'dcg_name' not assigned to ImagingSite entry for {imaging_site.image_path}"
        )

    if imaging_site.data_type == "atlas":
        # If the current ImagingSite is an atlas
        # Iterate over known grid square ImagingSite entries for this DataCollectionGroup
        # and register/update them with reference to this atlas
        for grid_square_candidate in murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
            .where(MurfeyDB.ImagingSite.data_type == "grid_square")
        ).all():
            try:
                _register_single_grid_square(
                    session_id=session_id,
                    dcg_name=dcg_name,
                    atlas_entry=imaging_site,
                    imaging_site=grid_square_candidate,
                    transport_object=transport_object,
                    murfey_db=murfey_db,
                )
            except Exception:
                logger.warning(
                    f"Could not add/update GridSquare entry for ImagingSite {grid_square_candidate.image_path}",
                    exc_info=True,
                )
                continue
    elif imaging_site.data_type == "grid_square":
        # Otherwise, load the atlas associated with this DataCollectionGroup
        # and register/update this one
        if atlas_search := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
            .where(MurfeyDB.ImagingSite.data_type == "atlas")
            .order_by(MurfeyDB.ImagingSite.id)
        ).all():
            imaging_site = _register_single_grid_square(
                session_id=session_id,
                dcg_name=dcg_name,
                atlas_entry=atlas_search[0],
                imaging_site=imaging_site,
                transport_object=transport_object,
                murfey_db=murfey_db,
            )
        else:
            logger.info(
                "No atlas ImagingSite entry has been registered for this DataCollectionGroup yet"
            )
    else:
        raise ValueError(
            f"Unexpected 'data_type' value for ImagingSite entry for {imaging_site.image_path}"
        )
    return imaging_site


def run(message: dict, murfey_db: Session) -> dict[str, bool]:
    # Early exit if no TransportManager object is configured
    if not murfey.server._transport_object:
        raise RuntimeError("No TransportManager object was set up")

    # Parse the incoming message
    session_id = int(message["session_id"])
    if isinstance(message["result"], str):
        json_obj: dict = json.loads(message["result"])
        result = CLEMPreprocessingResult(**json_obj)
    elif isinstance(message["result"], dict):
        result = CLEMPreprocessingResult(**message["result"])
    else:
        raise ValueError(
            f"Invalid type for TIFF preprocessing result: {type(message['result'])}"
        )

    # Check that output files were included
    if not result.output_files:
        raise ValueError("No files were provided in the incoming message")

    # Load current session from database
    murfey_session = murfey_db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
    ).one()
    instrument_name = murfey_session.instrument_name
    visit_name = murfey_session.visit

    # Register items in Murfey database
    clem_img_site = _register_clem_imaging_site(
        session_id=session_id,
        result=result,
        murfey_db=murfey_db,
    )

    # Register data collection group and atlas in ISPyB
    clem_img_site = _register_dcg_and_atlas(
        session_id=session_id,
        instrument_name=instrument_name,
        visit_name=visit_name,
        imaging_site=clem_img_site,
        transport_object=murfey.server._transport_object,
        murfey_db=murfey_db,
    )

    # Register CLEM image series as grid squares
    clem_img_site = _register_grid_square(
        session_id=session_id,
        imaging_site=clem_img_site,
        transport_object=murfey.server._transport_object,
        murfey_db=murfey_db,
    )

    # Construct list of files to use for image alignment and merging steps
    image_combos_to_process = [
        list(result.output_files.values())  # Composite image of all channels
    ]
    if ("gray" in result.output_files.keys()) and len(result.output_files) > 1:
        # Create additional fluorescent-only composite image
        image_combos_to_process.append(
            [file for channel, file in result.output_files.items() if channel != "gray"]
        )
        # Create additional bright field-only image
        image_combos_to_process.append(
            [file for channel, file in result.output_files.items() if channel == "gray"]
        )

    # Request for image alignment and processing for the requested combinations
    ref_file = list(result.output_files.values())[0]
    visit_idx = ref_file.parts.index(visit_name)
    visit_dir = Path(
        "/".join(
            ""
            if part == "/"  # Replace root "/" with "" for Linux paths
            else part
            for part in ref_file.parts[: visit_idx + 1]
        )
    )
    for image_combo in image_combos_to_process:
        murfey.server._transport_object.send(
            "processing_recipe",
            {
                "recipes": ["clem-align-and-merge"],
                "parameters": {
                    # Job parameters
                    "series_name": result.series_name,
                    "images": [str(file) for file in image_combo],
                    "metadata": str(result.metadata),
                    # Other recipe parameters
                    "session_dir": str(visit_dir),
                    "session_id": session_id,
                    "job_name": result.series_name,
                    "feedback_queue": murfey.server._transport_object.feedback_queue,
                },
            },
            new_connection=True,
        )
    logger.info(
        "Successfully requested image alignment and merging job for "
        f"{result.series_name!r} series"
    )
    return {"success": True}
