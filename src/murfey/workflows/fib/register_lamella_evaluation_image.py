import json
import logging
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import PIL.Image
from pydantic import BaseModel
from sqlmodel import Session as SQLModelSession, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.models import FIBImageMetadata, GridSquareParameters
from murfey.workflows.fib.shared import (
    parse_image_metadata,
    populate_fib_imaging_site_entry,
)
from murfey.workflows.register_data_collection_group import register_dcg

logger = logging.getLogger(__name__)


# The timestamp in the lamella evaluation image follows the pattern
# yyyy-mm-dd-HH-MM-SS
# E.g.
#   2026-03-09-18-24-51_drift_corrected_image_Finer Milling - Electron Image.png
#   2026-03-10-16-06-25_drift_corrected_image_Polishing 2 - Electron Image.png
# This can be searched for using regex
# (?<!\d) --> Character prior to pattern CANNOT be a digit
# (?!\d)  --> Character after pattern CANNOT be a digit
pattern = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}(?!\d)")


def _get_timestamp(name: str):
    """
    Helper function to extract the datetime information from the lamella evaluation
    image file name.
    """
    if (match := pattern.search(name)) is not None:
        return datetime.strptime(match.group(), "%Y-%m-%d-%H-%M-%S")
    raise ValueError(f"No datetime match found in {name}")


def _make_thumbnail(file: Path, metadata: FIBImageMetadata, visit_name: str):
    # Find the visit directory
    visit_idx = file.parts.index(visit_name)
    visit_dir = Path(*file.parts[: visit_idx + 1])

    # Lamella number field should have been populated
    if not metadata.lamella_number:
        raise ValueError("No lamella number associated with this visit")

    # Extract parts of the file name to retain
    timestamp, step_name = file.stem.split("_drift_corrected_image_")
    step_name = step_name.split(" - ")[0].replace(" ", "_").lower()

    # Add parts to the thumbnail name
    thumbnail_name = f"lamella_{metadata.lamella_number}_{timestamp}_{step_name}.png"

    # Construct full path to the thumbnail image
    save_path = (
        visit_dir
        / "processed"
        / metadata.project_name
        / f"grid_{metadata.slot_number}"
        / "lamella_evaluation_images"
        / thumbnail_name
    )
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Save the thumbnail image
    with PIL.Image.open(file) as img:
        img.thumbnail((512, 512))  # Shrink to fit within 512 x 512
        img.save(save_path)
    return save_path


def _register_fib_imaging_site(
    session_id: int,
    metadata: FIBImageMetadata,
    murfey_db: SQLModelSession,
):
    """
    Register FIB atlas in Murfey database or update existing entry.
    """
    if (
        fib_imaging_site := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.site_name == metadata.site_name)
            .where(MurfeyDB.ImagingSite.data_type == "grid_square")
        ).one_or_none()
    ) is None:
        # Create new entry if one doesn't already exist
        fib_imaging_site = MurfeyDB.ImagingSite(
            session_id=session_id,
            site_name=metadata.site_name,
            image_path=str(metadata.file),
            data_type="grid_square",
        )
        fib_imaging_site = populate_fib_imaging_site_entry(fib_imaging_site, metadata)
    else:
        # Check if image was acquired after the current one
        incoming_timestamp = _get_timestamp(metadata.file.stem)
        # Handle empty string
        current_timestamp = datetime.min
        if fib_imaging_site.image_path:
            current_timestamp = _get_timestamp(Path(fib_imaging_site.image_path).stem)
        # Update if incoming one is newer
        if incoming_timestamp >= current_timestamp:
            fib_imaging_site = populate_fib_imaging_site_entry(
                fib_imaging_site, metadata
            )
    murfey_db.add(fib_imaging_site)
    murfey_db.commit()
    return fib_imaging_site


def _register_dcg(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    imaging_site: MurfeyDB.ImagingSite,
    murfey_db: SQLModelSession,
):
    """
    Takes an ImagingSite entry and uses it to create and register a DataCollectionGroup
    entry in ISPyB if one doesn't already exist, or to populate an existing entry.
    After doing so, it will register the DataCollectionGroup ID in Murfey and add it to
    the ImagingSite entry.
    """
    # Determine variables to register data collection group and atlas with
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Generate a name/tag for the data collection group
    # The name will be the site name minus the "/lamella..." specifier
    dcg_name = "/".join(imaging_site.site_name.split("/")[:-1])

    # Check if a DataCollectionGroup entry with this session and tag already exists
    dcg_entry = murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == dcg_name)
    ).one_or_none()
    if not dcg_entry:
        # Create a placeholder DataCollectionGroup and Atlas if not
        dcg_message = {
            "microscope": instrument_name,
            "proposal_code": proposal_code,
            "proposal_number": proposal_number,
            "visit_number": visit_number,
            "session_id": session_id,
            "tag": dcg_name,
            "experiment_type_id": 46,
            "atlas": "",
            "atlas_pixel_size": 0.0,
            "sample": None,
        }
        dcg_entry = register_dcg(
            message=dcg_message,
            murfey_db=murfey_db,
        )
        if not dcg_entry:
            raise RuntimeError(
                "Failed to create DataCollectionGroup entry for "
                f"{imaging_site.image_path}"
            )

    # Update the ImagingSite with the DataCollectionGroup ID
    imaging_site.dcg_id = dcg_entry.id
    imaging_site.dcg_name = dcg_entry.tag
    murfey_db.add(imaging_site)
    murfey_db.commit()

    logger.info(
        f"Return ImagingSite with values: {json.dumps(imaging_site.model_dump(), indent=2, default=str)}"
    )
    return imaging_site


def _register_grid_square(
    session_id: int,
    imaging_site: MurfeyDB.ImagingSite,
    site_number: int,
    murfey_db: SQLModelSession,
):
    """
    Helper function to create a GridSquare entry in ISPyB if one doesn't already
    exist, and to link it to the corresopnding ImagingSite entry.
    """
    # Early exits if values are missing/not configured
    if murfey.server._transport_object is None:
        raise RuntimeError("No TransportManager object was set up")
    dcg_name = imaging_site.dcg_name
    if dcg_name is None:
        raise ValueError(
            f"'dcg_name' field in ImagingSite entry for {imaging_site.image_path} is empty"
        )

    # Check if an atlas has been registered
    atlas_search = murfey_db.exec(
        select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == session_id)
        .where(MurfeyDB.ImagingSite.dcg_name == dcg_name)
        .where(MurfeyDB.ImagingSite.data_type == "atlas")
        .order_by(MurfeyDB.ImagingSite.id)  # Sort in ascending insertion order
    ).all()
    if not atlas_search:
        logger.info(
            f"No atlas has been registered for data collection group {dcg_name!r} yet"
        )
        return imaging_site
    atlas = atlas_search[-1]

    logger.info(
        f"Found atlas ImagingSite: {json.dumps(atlas.model_dump(), indent=2, default=str)}"
    )

    # Check if the atlas has the required values for the GridSquare registration
    if not (
        atlas.pos_x is not None
        and atlas.pos_y is not None
        and atlas.pos_z is not None
        and atlas.rotation is not None
        and atlas.tilt_alpha is not None
        and atlas.len_x is not None
        and atlas.len_y is not None
        and atlas.thumbnail_pixels_x is not None
        and atlas.thumbnail_pixels_y is not None
    ):
        logger.warning(f"Atlas {atlas.image_path} not populated with required values")
        return imaging_site
    atlas_x1 = atlas.pos_x + (atlas.len_x / 2)
    atlas_y0 = atlas.pos_y - (atlas.len_y / 2)

    # Check that imaging site has the required values for registration
    if not (
        imaging_site.pos_x is not None
        and imaging_site.pos_y is not None
        and imaging_site.pos_z is not None
        and imaging_site.rotation is not None
        and imaging_site.tilt_alpha is not None
        and imaging_site.len_x is not None
        and imaging_site.len_y is not None
    ):
        logger.warning(
            f"ImagingSite for {imaging_site.image_path} not populated with required values"
        )
        return imaging_site

    # Transform the imaging site coordinates into the atlas' frame of reference
    # NOTE: This will require further investigation and tweaking, given the
    # many axes and centres of rotation present in the FIB stage system.
    # We start with a simple 2D rotation for now, and will adjust it as we observe
    # the alignment accuracy
    theta = math.radians(imaging_site.rotation - atlas.rotation)
    sin = math.sin(theta)
    cos = math.cos(theta)
    x_transformed = (imaging_site.pos_x * cos) - (imaging_site.pos_y * sin)
    y_transformed = (imaging_site.pos_x * sin) + (imaging_site.pos_y * cos)

    # Find the pixel coordinates of the image on the atlas
    # NOTE: On the atlas image, positive directions are LEFT (x) and DOWN (y)
    x_mid_px = int(
        round((atlas_x1 - x_transformed) / atlas.len_x * atlas.thumbnail_pixels_x) or 1
    )
    y_mid_px = int(
        round((y_transformed - atlas_y0) / atlas.len_y * atlas.thumbnail_pixels_y) or 1
    )

    # Find the pixel width and height of the lamella image on the atlas
    width_scaled = int(
        round((imaging_site.len_x / atlas.len_x) * atlas.thumbnail_pixels_x) or 1
    )
    height_scaled = int(
        round((imaging_site.len_y / atlas.len_y) * atlas.thumbnail_pixels_y) or 1
    )

    # Populate GridSquareParameters model
    grid_square_params = GridSquareParameters(
        tag=dcg_name,
        x_location=x_transformed,
        x_location_scaled=x_mid_px,
        y_location=y_transformed,
        y_location_scaled=y_mid_px,
        readout_area_x=imaging_site.image_pixels_x,
        readout_area_y=imaging_site.image_pixels_y,
        thumbnail_size_x=imaging_site.thumbnail_pixels_x,
        thumbnail_size_y=imaging_site.thumbnail_pixels_y,
        width=imaging_site.image_pixels_x,
        width_scaled=width_scaled,
        height=imaging_site.image_pixels_y,
        height_scaled=height_scaled,
        x_stage_position=x_transformed,
        y_stage_position=y_transformed,
        pixel_size=imaging_site.image_pixel_size,
        image=imaging_site.thumbnail_path,
    )

    # Register or update the grid square entry as required
    if grid_square_entry := murfey_db.exec(
        select(MurfeyDB.GridSquare)
        .where(MurfeyDB.GridSquare.name == site_number)
        .where(MurfeyDB.GridSquare.session_id == session_id)
        .where(MurfeyDB.GridSquare.tag == grid_square_params.tag)
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

        logger.info(
            f"Updated Murfey GridSquare entry: {json.dumps(grid_square_entry.model_dump(), indent=2, default=str)}"
        )

        # Update existing entry on ISPyB
        murfey.server._transport_object.do_update_grid_square(
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
        grid_square_ispyb_result = (
            murfey.server._transport_object.do_insert_grid_square(
                atlas_id=dcg_entry.atlas_id,
                grid_square_id=site_number,
                grid_square_parameters=grid_square_params,
            )
        )
        # Create matching record in Murfey
        grid_square_entry = MurfeyDB.GridSquare(
            id=grid_square_ispyb_result.get("return_value", None),
            name=site_number,
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
        logger.info(
            f"Creating new Murfey GridSquare entry: {json.dumps(grid_square_entry.model_dump(), indent=2, default=str)}"
        )
    murfey_db.add(grid_square_entry)

    # Add grid square ID to existing CLEM image series entry
    imaging_site.grid_square_id = grid_square_entry.id
    murfey_db.add(imaging_site)
    murfey_db.commit()

    logger.info(
        f"Updated ImagingSite after GridSquare registration: {json.dumps(imaging_site.model_dump(), indent=2, default=str)}"
    )
    return imaging_site


class FIBLamellaImageInfo(BaseModel):
    session_id: int
    lamella_image_file: Path


def run(
    message: dict[str, Any],
    murfey_db: SQLModelSession,
):
    # Outer try-finally block to ensure the database connection is closed
    logger.info(
        f"Received the following message:\n{json.dumps(message, indent=2, default=str)}"
    )

    # Validate incoming message
    fib_info = FIBLamellaImageInfo(**message)

    # Load visit information
    murfey_session = murfey_db.exec(
        select(MurfeyDB.Session).where(MurfeyDB.Session.id == fib_info.session_id)
    ).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name

    # Load the machine config
    machine_config = get_machine_config(instrument_name)[instrument_name]
    rotation_offset: float = cast(
        float, machine_config.calibrations.get("rotation_offset", 0)
    )

    # Extract metadata from the image
    metadata = FIBImageMetadata(
        visit_name=visit_name,
        file=fib_info.lamella_image_file,
        **parse_image_metadata(
            file=fib_info.lamella_image_file,
            rotation_offset=rotation_offset,
        ),
    )
    if metadata.lamella_number is None:
        raise ValueError(
            f"No lamella number associated with lamella image {fib_info.lamella_image_file}"
        )
    logger.info(
        "Extracted the following metadata from the image:\n"
        f"{json.dumps(metadata.model_dump(), indent=2, default=str)}"
    )

    # Make a thumbnail of the image and update metadata accordingly
    metadata.thumbnail_path = _make_thumbnail(
        file=fib_info.lamella_image_file,
        metadata=metadata,
        visit_name=visit_name,
    )

    # Register imaging site to Murfey, or update existing one
    fib_img_site = _register_fib_imaging_site(fib_info.session_id, metadata, murfey_db)

    # Register data collection group and atlas in ISPyB
    fib_img_site = _register_dcg(
        session_id=fib_info.session_id,
        instrument_name=instrument_name,
        visit_name=visit_name,
        imaging_site=fib_img_site,
        murfey_db=murfey_db,
    )

    # Register grid square in ISPyB
    fib_img_site = _register_grid_square(
        session_id=fib_info.session_id,
        imaging_site=fib_img_site,
        site_number=metadata.lamella_number,
        murfey_db=murfey_db,
    )

    logger.info(
        f"Registered lamella evaluation image {fib_info.lamella_image_file} "
        f"for slot {metadata.slot_number} in Murfey database"
    )
    return {"success": True}
