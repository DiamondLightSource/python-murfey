import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import PIL.Image
from pydantic import BaseModel
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.models import FIBImageMetadata
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
    logger.info(
        f"Registered lamella evaluation image {fib_info.lamella_image_file} "
        f"for slot {metadata.slot_number} in Murfey database"
    )

    # Register data collection group and atlas in ISPyB
    _register_dcg(
        session_id=fib_info.session_id,
        instrument_name=instrument_name,
        visit_name=visit_name,
        imaging_site=fib_img_site,
        murfey_db=murfey_db,
    )

    return {"success": True}
