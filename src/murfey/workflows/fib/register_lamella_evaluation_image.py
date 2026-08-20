import json
import logging
import math
import xml.etree.ElementTree as ET
from functools import cached_property
from pathlib import Path
from typing import Any, cast

import PIL.Image
from pydantic import BaseModel, computed_field, model_validator
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.util.config import get_machine_config
from murfey.util.fib import get_slot_number

logger = logging.getLogger(__name__)


class FIBImageMetadata(BaseModel):
    """
    These fields should ALL be present in the Electron Snapshot image.
    Positions and pixel sizes are in metres, whereas angles are in radians.
    """

    visit_name: str
    file: Path
    thumbnail_path: Path | None = None
    # Acceleration voltage
    voltage: float
    # Beam shifts
    shift_x: float
    shift_y: float
    # Actual field of view
    len_x: float
    len_y: float
    # Stage position
    pos_x: float
    pos_y: float
    pos_z: float
    rotation: float  # Radians
    slot_number: int
    tilt_alpha: float  # Radians
    tilt_beta: float  # Radians
    # Image dimensions
    pixels_x: int
    pixels_y: int
    # Pixel size
    pixel_size_x: float
    pixel_size_y: float

    @model_validator(mode="after")
    def check_pixel_size_tolerance(self):
        """
        The pixel size values for x and y should be nigh-identical
        """
        if abs(self.pixel_size_x - self.pixel_size_y) > 1e-18:
            raise ValueError
        return self

    # mypy doesn't support decorators on @property
    @computed_field  # type: ignore
    @cached_property
    def pixel_size(self) -> float:
        """
        Return an average of pixel sizes along the x- and y-axes
        """
        return 0.5 * (self.pixel_size_x + self.pixel_size_y)

    # mypy doesn't support decorators on @property
    @computed_field  # type: ignore
    @cached_property
    def project_name(self) -> str:
        """
        Extract the project name from the file path. This assumes a specific
        folder structure of '{visit_name}/maps/{project_name}'.
        """
        path_parts = self.file.parts
        visit_idx = path_parts.index(self.visit_name)
        return path_parts[visit_idx + 2]  # {visit}/maps/{project_name}

    # mypy doesn't support decorators on @property
    @computed_field  # type: ignore
    @cached_property
    def site_name(self) -> str:
        """
        Create a site name for the current image based on the project name
        and its slot number.
        """
        return f"{self.project_name}--slot_{self.slot_number}"


def _parse_metadata(file: Path, visit_name: str, rotation_offset: float):
    """
    Parses through the atlas image's tags to extract the relevant metadata
    """

    # Search for the XML metadata in the tags (34683 is the default key)
    img = PIL.Image.open(file)
    tags = dict(img.text)
    xml_metadata = None
    if (
        isinstance((tag_contents := tags.get("Metadata")), str)
        and "xml version" in tag_contents
    ):
        xml_metadata = ET.fromstring(tag_contents)
    if xml_metadata is None:
        raise ValueError(f"Could not find required metadata in file {file}")

    # Extract key values from metadata
    extracted: dict[str, Any] = {
        key: node.text if (node := xml_metadata.find(node_path)) is not None else None
        for key, node_path in (
            ("voltage", ".//Optics/AccelerationVoltage"),
            ("shift_x", ".//Optics/BeamShift/X"),
            ("shift_y", ".//Optics/BeamShift/Y"),
            ("len_x", ".//Optics/ScanFieldOfView/X"),
            ("len_y", ".//Optics/ScanFieldOfView/Y"),
            ("pos_x", ".//StageSettings/StagePosition/X"),
            ("pos_y", ".//StageSettings/StagePosition/Y"),
            ("pos_z", ".//StageSettings/StagePosition/Z"),
            # Angles are in radians
            ("rotation", ".//StageSettings/StagePosition/Rotation"),
            ("tilt_alpha", ".//StageSettings/StagePosition/Tilt/Alpha"),
            ("tilt_beta", ".//StageSettings/StagePosition/Tilt/Beta"),
            ("pixels_x", ".//BinaryResult/ImageSize/X"),
            ("pixels_y", ".//BinaryResult/ImageSize/Y"),
            ("pixel_size_x", ".//BinaryResult/PixelSize/X"),
            ("pixel_size_y", ".//BinaryResult/PixelSize/Y"),
        )
    }
    # Calculate the slot number
    extracted["slot_number"] = get_slot_number(
        x=float(extracted["pos_x"]),
        y=float(extracted["pos_y"]),
        rotation=math.degrees(float(extracted["rotation"])),  # Convert to degrees
        rotation_offset=rotation_offset,
    )
    # Return the parsed Pydantic model
    return FIBImageMetadata(
        visit_name=visit_name,
        file=file,
        **extracted,
    )


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
            metadata = _parse_metadata(
                file=fib_info.lamella_image_file,
                visit_name=visit_name,
                rotation_offset=rotation_offset,
            )
            logger.info(
                "Extracted the following metadata from the image:\n",
                metadata.model_dump_json(indent=2),
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
