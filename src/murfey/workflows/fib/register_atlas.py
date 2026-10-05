import logging
import math
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, cast

import PIL.Image
from pydantic import BaseModel
from sqlmodel import Session, select

import murfey.server
import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.fib import number_from_name
from murfey.util.models import FIBImageMetadata, GridSquareParameters
from murfey.workflows.fib.shared import (
    parse_image_metadata,
    populate_fib_imaging_site_entry,
)
from murfey.workflows.register_data_collection_group import register_dcg

logger = logging.getLogger("murfey.workflows.fib.register_atlas")


def _make_thumbnail(file: Path, metadata: FIBImageMetadata, visit_name: str):
    # Find visit directory path
    visit_idx = file.parts.index(visit_name)
    visit_dir = Path(*file.parts[: visit_idx + 1])

    # Construct path to thumbnail
    image_number = number_from_name(file.stem)
    save_path = (
        visit_dir
        / "processed"
        / metadata.project_name
        / f"grid_{metadata.slot_number}"
        / "atlas"
        / f"atlas_{str(image_number).zfill(2)}.png"
    )
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Save the thumbnail
    with PIL.Image.open(file) as img:
        img.thumbnail((512, 512))
        img.save(save_path)
    return save_path


def _register_fib_imaging_site(
    session_id: int,
    metadata: FIBImageMetadata,
    murfey_db: Session,
):
    """
    Register FIB atlas in Murfey database or update existing entry.
    """
    if (
        fib_imaging_site := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.site_name == metadata.site_name)
            .where(MurfeyDB.ImagingSite.data_type == "atlas")
        ).one_or_none()
    ) is None:
        # Create new entry if one doesn't already exist
        fib_imaging_site = MurfeyDB.ImagingSite(
            session_id=session_id,
            site_name=metadata.site_name,
            image_path=str(metadata.file),
            data_type="atlas",
        )
        fib_imaging_site = populate_fib_imaging_site_entry(fib_imaging_site, metadata)
    else:
        # Check if the entry is new or newer than the current stored one
        incoming_number = number_from_name(metadata.file.stem)
        # Handle empty string
        if not fib_imaging_site.image_path:
            current_number = 0
        # Read 'maps' atlases in one way
        elif "maps" in (curr_path := Path(fib_imaging_site.image_path)).parts:
            current_number = number_from_name(curr_path.stem)
        else:
            current_number = 0
        # Update if incoming one is newer
        if incoming_number >= current_number:
            fib_imaging_site = populate_fib_imaging_site_entry(
                fib_imaging_site, metadata
            )

    murfey_db.add(fib_imaging_site)
    murfey_db.commit()

    return fib_imaging_site


def _register_dcg_and_atlas(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    imaging_site: MurfeyDB.ImagingSite,
    metadata: FIBImageMetadata,
    murfey_db: Session,
):
    proposal_code = "".join(char for char in visit_name.split("-")[0] if char.isalpha())
    proposal_number = "".join(
        char for char in visit_name.split("-")[0] if char.isdigit()
    )
    visit_number = visit_name.split("-")[-1]

    # Register using thumbnail values if they are provided
    if (
        imaging_site.thumbnail_path is not None
        and imaging_site.thumbnail_pixel_size is not None
    ):
        atlas_name: str | None = imaging_site.thumbnail_path
        atlas_pixel_size: float | None = imaging_site.thumbnail_pixel_size
    else:
        atlas_name = imaging_site.image_path
        atlas_pixel_size = imaging_site.image_pixel_size

    if dcg_search := murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == imaging_site.site_name)
    ).all():
        dcg_entry = dcg_search[0]
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
        dcg_message = {
            "microscope": instrument_name,
            "proposal_code": proposal_code,
            "proposal_number": proposal_number,
            "visit_number": visit_number,
            "session_id": session_id,
            "tag": imaging_site.site_name,
            "experiment_type_id": 46,
            "atlas": atlas_name,
            "atlas_pixel_size": atlas_pixel_size,
            "sample": metadata.slot_number,
        }
        dcg_entry = register_dcg(
            message=dcg_message,
            murfey_db=murfey_db,
        )
        if not dcg_entry:
            raise RuntimeError(
                f"Could not register DataCollectionGroup entry for {imaging_site.image_path}"
            )
    imaging_site.dcg_id = dcg_entry.id
    imaging_site.dcg_name = dcg_entry.tag
    murfey_db.add(imaging_site)
    murfey_db.commit()

    return imaging_site


def _update_grid_squares(
    session_id: int,
    atlas: MurfeyDB.ImagingSite,
    murfey_db: Session,
):
    """
    Searches for any grid square-type ImagingSites associated with this atlas, and
    uses them to recalculate the relative positions of the images on the current
    atlas image.
    """
    # Early errors if variables are not set correctly
    if murfey.server._transport_object is None:
        raise RuntimeError("No TransportManager object was set up")
    dcg_name = atlas.dcg_name
    if dcg_name is None:
        raise ValueError(
            f"'dcg_name' field in ImagingSite entry for {atlas.image_path} is empty"
        )

    # Early exit if no lamella images have been registered
    lamella_imaging_sites = murfey_db.exec(
        select(MurfeyDB.ImagingSite)
        .where(MurfeyDB.ImagingSite.session_id == session_id)
        .where(MurfeyDB.ImagingSite.dcg_name == atlas.dcg_name)
        .where(MurfeyDB.ImagingSite.data_type == "grid_square")
    ).all()
    if not lamella_imaging_sites:
        logger.info(
            "No lamella imaging sites associated with this atlas have been registered"
        )
        return atlas

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
        return atlas

    atlas_x1 = atlas.pos_x + (atlas.len_x / 2)
    atlas_y0 = atlas.pos_y - (atlas.len_y / 2)

    # Iterate across lamella imaging sites and register/update them
    for lamella in lamella_imaging_sites:
        # Extract site number from site name
        try:
            site_number = int(lamella.site_name.split("lamella_")[-1])
        except Exception:
            logger.warning(
                "Unable to get lamella number from site name of "
                f"ImagingSite entry {lamella.image_path}",
                exc_info=True,
            )
            continue

        # Check that the lamella ImagingSite entry has the required values
        if not (
            lamella.pos_x is not None
            and lamella.pos_y is not None
            and lamella.pos_z is not None
            and lamella.rotation is not None
            and lamella.tilt_alpha is not None
            and lamella.len_x is not None
            and lamella.len_y is not None
        ):
            logger.warning(
                f"ImagingSite for {lamella.image_path} not populated with required values"
            )
            continue

        # Transform the imaging site coordinates into the atlas' frame of reference
        # NOTE: This will require further investigation and tweaking, given the
        # many axes and centres of rotation present in the FIB stage system.
        # We start with a simple 2D rotation for now, and will adjust it as we observe
        # the alignment accuracy
        theta = math.radians(lamella.rotation - atlas.rotation)
        sin = math.sin(theta)
        cos = math.cos(theta)
        x_transformed = (lamella.pos_x * cos) - (lamella.pos_y * sin)
        y_transformed = (lamella.pos_x * sin) + (lamella.pos_y * cos)

        # Find the pixel coordinates of the image on the atlas
        # NOTE: On the atlas image, positive directions are LEFT (x) and DOWN (y)
        x_mid_px = int(
            round((atlas_x1 - x_transformed) / atlas.len_x * atlas.thumbnail_pixels_x)
            or 1
        )
        y_mid_px = int(
            round((y_transformed - atlas_y0) / atlas.len_y * atlas.thumbnail_pixels_y)
            or 1
        )

        # Find the pixel width and height of the lamella image on the atlas
        width_scaled = int(
            round((lamella.len_x / atlas.len_x) * atlas.thumbnail_pixels_x) or 1
        )
        height_scaled = int(
            round((lamella.len_y / atlas.len_y) * atlas.thumbnail_pixels_y) or 1
        )

        # Populate GridSquareParameters model
        grid_square_params = GridSquareParameters(
            tag=dcg_name,
            x_location=x_transformed,
            x_location_scaled=x_mid_px,
            y_location=y_transformed,
            y_location_scaled=y_mid_px,
            readout_area_x=lamella.image_pixels_x,
            readout_area_y=lamella.image_pixels_y,
            thumbnail_size_x=lamella.thumbnail_pixels_x,
            thumbnail_size_y=lamella.thumbnail_pixels_y,
            width=lamella.image_pixels_x,
            width_scaled=width_scaled,
            height=lamella.image_pixels_y,
            height_scaled=height_scaled,
            x_stage_position=x_transformed,
            y_stage_position=y_transformed,
            pixel_size=lamella.image_pixel_size,
            image=lamella.thumbnail_path,
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
        murfey_db.add(grid_square_entry)

    # Commit all changes at the end
    murfey_db.commit()

    return atlas


class FIBAtlasRegistrationInfo(BaseModel):
    session_id: int
    atlas_file: Path


def run(
    message: dict[str, Any],
    murfey_db: Session,
):
    # Validate incoming message
    fib_info = FIBAtlasRegistrationInfo(**message)

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

    # Extract metadata from Electron Snapshot image
    metadata = FIBImageMetadata(
        visit_name=visit_name,
        file=fib_info.atlas_file,
        **parse_image_metadata(
            fib_info.atlas_file,
            rotation_offset=rotation_offset,
        ),
    )

    # Make a thumbnail of the image and update metadata accordingly
    metadata.thumbnail_path = _make_thumbnail(
        file=metadata.file,
        metadata=metadata,
        visit_name=visit_name,
    )

    # Register imaging site in Murfey, or update existing one
    fib_imaging_site = _register_fib_imaging_site(
        fib_info.session_id, metadata, murfey_db
    )
    logger.info(
        f"Registered FIB atlas image {fib_info.atlas_file} "
        f"for slot {metadata.slot_number} in Murfey database"
    )

    # Register data collection group and atlas in ISPyB
    fib_imaging_site = _register_dcg_and_atlas(
        session_id=fib_info.session_id,
        instrument_name=murfey_session.instrument_name,
        visit_name=murfey_session.visit,
        imaging_site=fib_imaging_site,
        metadata=metadata,
        murfey_db=murfey_db,
    )

    # Update any existing lammella image (grid square) entries
    fib_imaging_site = _update_grid_squares(
        session_id=fib_info.session_id,
        atlas=fib_imaging_site,
        murfey_db=murfey_db,
    )

    return {"success": True, "requeue": False}
