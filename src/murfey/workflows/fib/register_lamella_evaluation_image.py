import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
from sqlmodel import Session as SQLModelSession, select

import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.models import FIBImageMetadata
from murfey.workflows.fib.shared import (
    parse_image_metadata,
    populate_fib_imaging_site_entry,
)

logger = logging.getLogger(__name__)


# The timestamp in the lamella evaluation image follows the pattern
# yyyy-mm-dd-HH-MM-SS
# This can be searched for using regex
# (?<!\d) --> Character prior to pattern CANNOT be a digit
# (?!\d)  --> Character after pattern CANNOT be a digit
pattern = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}(?!\d)")


def _get_timestamp(name: str):
    """
    Helper functino to extract the datetime information from the lamella evaluation
    image file name.
    """
    if (match := pattern.search(name)) is not None:
        return datetime.strptime(match.group(), "%Y-%m-%d-%H-%M-%S")
    raise ValueError(f"No datetime match found in {name}")


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
            data_type="atlas",
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
    try:
        try:
            # Validate incoming message
            fib_info = FIBLamellaImageInfo(**message)
        except Exception:
            logger.error("Could not validate incoming message", exc_info=True)
            return {"success": False, "requeue": False}

        try:
            # Load visit information
            murfey_session = murfey_db.exec(
                select(MurfeyDB.Session).where(
                    MurfeyDB.Session.id == fib_info.session_id
                )
            ).one()
            visit_name = murfey_session.visit
            instrument_name = murfey_session.instrument_name
        except Exception:
            logger.error(
                "Exception encountered while querying Murfey database", exc_info=True
            )
            return {"success": False, "requeue": False}

        try:
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
        except Exception:
            logger.error(
                f"Error extracting metadata from file {fib_info.lamella_image_file}",
                exc_info=True,
            )
            return {"success": False, "requeue": False}

        try:
            # Register imaging site to Murfey, or update existing one
            _ = _register_fib_imaging_site(fib_info.session_id, metadata, murfey_db)
            logger.info(
                f"Register lamella evaluation image {fib_info.lamella_image_file} "
                f"for slot {metadata.slot_number} in Murfey database"
            )
        except Exception:
            logger.error(
                "Error registering lamella evaluation image "
                f"{fib_info.lamella_image_file} in Murfey database",
                exc_info=True,
            )
            return {"success": False, "requeue": False}

        return {"success": True}
    finally:
        murfey_db.close()
