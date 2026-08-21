import json
import logging
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.fib import parse_image_metadata
from murfey.util.models import FIBImageMetadata

logger = logging.getLogger(__name__)


class FIBLamellaImageInfo(BaseModel):
    session_id: int
    lamella_image_file: Path


def run(
    message: dict[str, Any],
    murfey_db: Session,
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
                file=fib_info.lamella_image_file
                ** parse_image_metadata(
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

        return {"success": True}
    finally:
        murfey_db.close()
