import logging
import xml.etree.ElementTree as ET
from functools import cached_property
from pathlib import Path

import numpy as np
import PIL.Image
from pydantic import BaseModel, computed_field, model_validator
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB

logger = logging.getLogger("murfey.workflows.fib.register_atlas")


class FIBAtlasMetadata(BaseModel):
    """
    These fields should ALL be present in the Electron Snapshot image.
    Positions and pixel sizes are in metres, whereas angles are in radians.
    """

    visit_name: str
    file: Path
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
    def slot_number(self) -> int:
        """
        Decide on a slot number for the site being inspected. From observation,
        the x-position is entirely negative for one slot and entirely positive
        for the other.
        """
        return 1 if self.pos_x < 0 else 2

    # mypy doesn't support decorators on @property
    @computed_field  # type: ignore
    @cached_property
    def site_name(self) -> str:
        """
        Create a site name for the current image based on the project name
        and its slot number. This assumes a specific folder structure of
        {visit_name}/maps/{project_name}
        """
        path_parts = self.file.parts
        visit_idx = path_parts.index(self.visit_name)
        project_name = path_parts[visit_idx + 2]  # {visit}/maps/{project_name}
        return f"{project_name}--slot_{self.slot_number}"


def _parse_metadata(file: Path, visit_name: str):
    """
    Parses through the atlas image's tags to extract the relevant metadata
    """

    # Metadata is stored in the TIFF file under tag number 34683
    img = PIL.Image.open(file)
    tags = dict(img.tag_v2)
    xml_metadata = ET.fromstring(str(tags.get(34683)))

    # Extract key values from metadata
    return FIBAtlasMetadata(
        visit_name=visit_name,
        file=file,
        **{
            key: node.text
            if (node := xml_metadata.find(node_path)) is not None
            else None
            for key, node_path in (
                ("voltage", ".//Optics/AccelerationVoltage"),
                ("shift_x", ".//Optics/BeamShift/X"),
                ("shift_y", ".//Optics/BeamShift/Y"),
                ("len_x", ".//Optics/ScanFieldOfView/X"),
                ("len_y", ".//Optics/ScanFieldOfView/Y"),
                ("pos_x", ".//StageSettings/StagePosition/X"),
                ("pos_y", ".//StageSettings/StagePosition/Y"),
                ("pos_z", ".//StageSettings/StagePosition/Z"),
                ("rotation", ".//StageSettings/StagePosition/Rotation"),
                ("tilt_alpha", ".//StageSettings/StagePosition/Tilt/Alpha"),
                ("tilt_beta", ".//StageSettings/StagePosition/Tilt/Beta"),
                ("pixels_x", ".//BinaryResult/ImageSize/X"),
                ("pixels_y", ".//BinaryResult/ImageSize/Y"),
                ("pixel_size_x", ".//BinaryResult/PixelSize/X"),
                ("pixel_size_y", ".//BinaryResult/PixelSize/Y"),
            )
        },
    )


def _register_fib_imaging_site(
    session_id: int,
    metadata: FIBAtlasMetadata,
    murfey_db: Session,
):
    """
    Register FIB atlas in Murfey database or update existing entry.
    """
    # Create new entry if one doesn't already exist
    if not (
        fib_imaging_site := murfey_db.exec(
            select(MurfeyDB.ImagingSite)
            .where(MurfeyDB.ImagingSite.session_id == session_id)
            .where(MurfeyDB.ImagingSite.image_path == str(metadata.file))
        ).one_or_none()
    ):
        fib_imaging_site = MurfeyDB.ImagingSite(
            session_id=session_id,
            image_path=str(metadata.file),
            data_type="atlas",
        )
    # Add/update entries
    fib_imaging_site.site_name = metadata.site_name
    fib_imaging_site.pos_x = metadata.pos_x
    fib_imaging_site.pos_y = metadata.pos_y
    fib_imaging_site.pos_z = metadata.pos_z
    fib_imaging_site.rotation = float(np.rad2deg(metadata.rotation))
    fib_imaging_site.tilt_alpha = float(np.rad2deg(metadata.tilt_alpha))
    fib_imaging_site.tilt_beta = float(np.rad2deg(metadata.tilt_beta))
    fib_imaging_site.len_x = metadata.len_x
    fib_imaging_site.len_y = metadata.len_y
    fib_imaging_site.image_pixels_x = metadata.pixels_x
    fib_imaging_site.image_pixels_y = metadata.pixels_y
    fib_imaging_site.image_pixel_size = metadata.pixel_size

    murfey_db.add(fib_imaging_site)
    murfey_db.commit()


def run(
    session_id: int,
    file: Path,
    murfey_db: Session,
):
    # Outer try-finally block to ensure database connection closes
    try:
        # Load visit information
        try:
            session_entry = murfey_db.exec(
                select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
            ).one()
            visit_name = session_entry.visit
        except Exception:
            logger.error(
                "Exception encountered while querying Murfey database", exc_info=True
            )
            return False

        # Extract metadata from Electron Snapshot image
        try:
            metadata = _parse_metadata(file, visit_name)
        except Exception:
            logger.error(f"Error extracting metadata from file {file}", exc_info=True)
            return False

        # Register imaging site in Murfey, or update existing one
        try:
            _register_fib_imaging_site(session_id, metadata, murfey_db)
            logger.info(
                f"Registered FIB atlas image {file} for slot {metadata.slot_number} in Murfey database"
            )
        except Exception:
            logger.error(
                f"Error registering FIB atlas image {file} in Murfey database",
                exc_info=True,
            )
            return False
        return True
    finally:
        murfey_db.close()
