import logging
import traceback
import xml.etree.ElementTree as ET
from functools import cached_property
from importlib.metadata import entry_points
from pathlib import Path

import numpy as np
import PIL.Image
from pydantic import BaseModel, computed_field, model_validator
from sqlmodel import Session, select

import murfey.util.db as MurfeyDB
from murfey.util.fib import number_from_name

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

    # Search for the XML metadata in the tags (34683 is the default key)
    img = PIL.Image.open(file)
    tags = dict(img.tag_v2)
    xml_metadata = None
    if (
        isinstance((tag_contents := tags.get(34683)), str)
        and "xml version" in tag_contents
    ):
        xml_metadata = ET.fromstring(tag_contents)
    else:
        logger.warning(
            "Could not find metadata under tag key 34683, iterating through tags"
        )
        for key, value in tags.items():
            if key == 34683:  # Already inspected
                continue
            if isinstance(value, str) and "xml version" in value:
                xml_metadata = ET.fromstring(value)
    if xml_metadata is None:
        raise ValueError(f"Could not find required metadata in file {file}")

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

    def _update_entry(
        imaging_site: MurfeyDB.ImagingSite,
        metadata: FIBAtlasMetadata,
    ):
        imaging_site.image_path = str(metadata.file)
        imaging_site.pos_x = metadata.pos_x
        imaging_site.pos_y = metadata.pos_y
        imaging_site.pos_z = metadata.pos_z
        imaging_site.rotation = float(np.rad2deg(metadata.rotation))
        imaging_site.tilt_alpha = float(np.rad2deg(metadata.tilt_alpha))
        imaging_site.tilt_beta = float(np.rad2deg(metadata.tilt_beta))
        imaging_site.len_x = metadata.len_x
        imaging_site.len_y = metadata.len_y
        imaging_site.image_pixels_x = metadata.pixels_x
        imaging_site.image_pixels_y = metadata.pixels_y
        imaging_site.image_pixel_size = metadata.pixel_size

        return imaging_site

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
        fib_imaging_site = _update_entry(fib_imaging_site, metadata)
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
        if incoming_number > current_number:
            fib_imaging_site = _update_entry(fib_imaging_site, metadata)

    murfey_db.add(fib_imaging_site)
    murfey_db.commit()

    return fib_imaging_site


def _register_dcg_and_atlas(
    session_id: int,
    instrument_name: str,
    visit_name: str,
    imaging_site: MurfeyDB.ImagingSite,
    metadata: FIBAtlasMetadata,
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
    dcg_entry = murfey_db.exec(
        select(MurfeyDB.DataCollectionGroup)
        .where(MurfeyDB.DataCollectionGroup.session_id == session_id)
        .where(MurfeyDB.DataCollectionGroup.tag == imaging_site.site_name)
    ).one()

    imaging_site.dcg_id = dcg_entry.id
    imaging_site.dcg_name = dcg_entry.tag
    murfey_db.add(imaging_site)
    murfey_db.commit()


def run(
    session_id: int,
    file: Path,
    murfey_db: Session,
):
    # Outer try-finally block to ensure database connection closes
    try:
        try:
            # Load visit information
            murfey_session = murfey_db.exec(
                select(MurfeyDB.Session).where(MurfeyDB.Session.id == session_id)
            ).one()
            visit_name = murfey_session.visit
        except Exception:
            logger.error(
                "Exception encountered while querying Murfey database", exc_info=True
            )
            return False

        try:
            # Extract metadata from Electron Snapshot image
            metadata = _parse_metadata(file, visit_name)
        except Exception:
            logger.error(f"Error extracting metadata from file {file}", exc_info=True)
            return False

        try:
            # Register imaging site in Murfey, or update existing one
            fib_imaging_site = _register_fib_imaging_site(
                session_id, metadata, murfey_db
            )
            logger.info(
                f"Registered FIB atlas image {file} for slot {metadata.slot_number} in Murfey database"
            )
        except Exception:
            logger.error(
                f"Error registering FIB atlas image {file} in Murfey database",
                exc_info=True,
            )
            return False

        try:
            # Register data collection group and atlas in ISPyB
            _register_dcg_and_atlas(
                session_id=session_id,
                instrument_name=murfey_session.instrument_name,
                visit_name=murfey_session.visit,
                imaging_site=fib_imaging_site,
                metadata=metadata,
                murfey_db=murfey_db,
            )
        except Exception:
            # Log error but allow workflow to proceed
            logger.error(
                "Exception encountered when registering data collection group for FIB workflow "
                f"for {metadata.site_name!r}: \n"
                f"{traceback.format_exc()}"
            )

        return True

    finally:
        murfey_db.close()
