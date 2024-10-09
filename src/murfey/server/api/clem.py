from __future__ import annotations

import re
import sys
import traceback
from logging import getLogger
from pathlib import Path
from typing import Optional, Type, Union

from fastapi import APIRouter
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import (
    CLEMImageMetadata,
    CLEMImageSeries,
    CLEMImageStack,
    CLEMLIFFile,
    CLEMTIFFFile,
)
from murfey.util.db import Session as MurfeySession
from murfey.util.models import TiffSeriesInfo

# Use backport from importlib_metadata for Python <3.10
if sys.version_info.major == 3 and sys.version_info.minor < 10:
    from importlib_metadata import EntryPoint, entry_points
else:
    from importlib.metadata import EntryPoint, entry_points

# Set up logger
logger = getLogger("murfey.server.api.clem")

# Create APIRouter class object
router = APIRouter()

# Valid file types
valid_file_types = (
    ".lif",
    ".tif",
    ".tiff",
    ".xlif",
    ".xml",
)


"""
HELPER FUNCTIONS
"""


def validate_and_sanitise(
    file: Path,
    session_id: int,
    db: Session,
) -> Path:
    """
    Performs validation and sanitisation on the incoming file paths, ensuring that
    no forbidden characters are present and that the the path points only to allowed
    sections of the file server.

    Returns the file path as a sanitised string that can be converted into a Path
    object again.

    NOTE: Due to the instrument name query, 'db' now needs to be passed as an
    explicit variable to this function from within a FastAPI endpoint, as using the
    instance that was imported directly won't load it in the correct state.
    """

    # Resolve symlinks and directory changes to get full file path
    full_path = Path(file).resolve()

    # Use machine configuration to validate which file base paths are accepted from
    instrument_name = (
        db.exec(select(MurfeySession).where(MurfeySession.id == session_id))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    rsync_basepath = machine_config.rsync_basepath
    try:
        base_path = list(rsync_basepath.parents)[-2].as_posix()
    except IndexError:
        # Print to troubleshoot
        logger.warning(f"Base path {rsync_basepath!r} is too short")
        base_path = rsync_basepath.as_posix()
    except Exception:
        raise Exception("Unexpected exception occurred when loading the file base path")

    # Check that full file path doesn't contain unallowed characters
    # Currently allows only:
    # - words (alphanumerics and "_"; \w),
    # - spaces (\s),
    # - periods,
    # - dashes,
    # - forward slashes ("/")
    if bool(re.fullmatch(r"^[\w\s\.\-/]+$", str(full_path))) is False:
        raise ValueError(f"Unallowed characters present in {file}")

    # Check that it's not accessing somehwere it's not allowed
    if not str(full_path).startswith(str(base_path)):
        raise ValueError(f"{file} points to a directory that is not permitted")

    # Check that it's a file, not a directory
    if full_path.is_file() is False:
        raise ValueError(f"{file} is not a file")

    # Check that it is of a permitted file type
    if f"{full_path.suffix}" not in valid_file_types:
        raise ValueError(f"{full_path.suffix} is not a permitted file format")

    return full_path


def get_db_entry(
    db: Session,
    # With the database search funcion having been moved out of the FastAPI
    # endpoint, the database now has to be explicitly passed within the FastAPI
    # endpoint function in order for it to be loaded in the correct state.
    table: Type[
        Union[
            CLEMImageMetadata,
            CLEMImageSeries,
            CLEMImageStack,
            CLEMLIFFile,
            CLEMTIFFFile,
        ]
    ],
    session_id: int,
    file_path: Optional[Path] = None,
    series_name: Optional[str] = None,
) -> Union[
    CLEMImageMetadata,
    CLEMImageSeries,
    CLEMImageStack,
    CLEMLIFFile,
    CLEMTIFFFile,
]:
    """
    Searches the CLEM workflow-related tables in the Murfey database for an entry that
    matches the file path or series name within a given session. Returns the entry if
    a match is found, otherwise register it as a new entry in the database.
    """

    # Validate that parameters are provided correctly
    if file_path is None and series_name is None:
        raise ValueError(
            "One of either 'file_path' or 'series_name' has to be provided"
        )
    if file_path is not None and series_name is not None:
        raise ValueError("Only one of 'file_path' or 'series_name' should be provided")

    # Validate file path if provided
    if file_path is not None:
        try:
            file_path = validate_and_sanitise(file_path, session_id, db)
        except Exception:
            raise Exception

    # Validate series name to use
    if series_name is not None:
        if bool(re.fullmatch(r"^[\w\s\.\-/]+$", series_name)) is False:
            raise ValueError("One or more characters in the string are not permitted")

    # Return database entry if it exists
    try:
        db_entry = (
            db.exec(
                select(table)
                .where(table.session_id == session_id)
                .where(table.file_path == str(file_path))
            ).one()
            if file_path is not None
            else db.exec(
                select(table)
                .where(table.session_id == session_id)
                .where(table.series_name == series_name)
            ).one()
        )
    # Create and register new entry if not present
    except NoResultFound:
        db_entry = (
            table(
                file_path=str(file_path),
                session_id=session_id,
            )
            if file_path is not None
            else table(
                series_name=series_name,
                session_id=session_id,
            )
        )
        db.add(db_entry)
        db.commit()
        db.refresh(db_entry)
    except Exception:
        raise Exception

    return db_entry


"""
API ENDPOINTS FOR FILE REGISTRATION
"""


@router.post("/sessions/{session_id}/clem/lif_files")
def register_lif_file(
    lif_file: Path,
    session_id: int,
    master_metadata: Optional[Path] = None,
    child_metadata: list[Path] = [],
    child_series: list[str] = [],
    child_stacks: list[Path] = [],
    db: Session = murfey_db,
):
    # Return or register the LIF file entry
    try:
        clem_lif_file: CLEMLIFFile = get_db_entry(
            db=db,
            table=CLEMLIFFile,
            session_id=session_id,
            file_path=lif_file,
        )
    except Exception:
        logger.error(traceback.format_exc())
        return False

    # Add metadata information if provided
    if master_metadata is not None:
        try:
            master_metadata = validate_and_sanitise(master_metadata, session_id, db)
            clem_lif_file.master_metadata = str(master_metadata)
        except Exception:
            logger.warning(traceback.format_exc())

    # Register child metadata if provided
    for metadata in child_metadata:
        try:
            metadata_db_entry: CLEMImageMetadata = get_db_entry(
                db=db,
                table=CLEMImageMetadata,
                session_id=session_id,
                file_path=metadata,
            )
            # Append to database entry
            clem_lif_file.child_metadata.append(metadata_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Register child image series if provided
    for series in child_series:
        try:
            series_db_entry: CLEMImageSeries = get_db_entry(
                db=db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=series,
            )
            # Append to database entry
            clem_lif_file.child_series.append(series_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Register child image stacks if provided
    for stack in child_stacks:
        try:
            stack_db_entry: CLEMImageStack = get_db_entry(
                db=db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=stack,
            )
            # Append to database entry
            clem_lif_file.child_stacks.append(stack_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Commit to database
    db.add(clem_lif_file)
    db.commit()
    db.close()
    return True


@router.post("/sessions/{session_id}/clem/tiff_files")
def register_tiff_file(
    tiff_file: Path,
    session_id: int,
    associated_metadata: Optional[Path] = None,
    associated_series: Optional[str] = None,
    associated_stack: Optional[Path] = None,
    db: Session = murfey_db,
):
    # Get or register the database entry
    try:
        clem_tiff_file: CLEMTIFFFile = get_db_entry(
            db=db,
            table=CLEMTIFFFile,
            session_id=session_id,
            file_path=tiff_file,
        )
    except Exception:
        logger.error(traceback.format_exc())
        return False

    # Add metadata if provided
    if associated_metadata is not None:
        try:
            metadata_db_entry: CLEMImageMetadata = get_db_entry(
                db=db,
                table=CLEMImageMetadata,
                session_id=session_id,
                file_path=associated_metadata,
            )
            # Link database entries
            clem_tiff_file.associated_metadata = metadata_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Add series information if provided
    if associated_series is not None:
        try:
            series_db_entry: CLEMImageSeries = get_db_entry(
                db=db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=associated_series,
            )
            # Link database entries
            clem_tiff_file.child_series = series_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Add image stack information if provided
    if associated_stack is not None:
        try:
            stack_db_entry: CLEMImageStack = get_db_entry(
                db=db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=associated_stack,
            )
            # Link database entries
            clem_tiff_file.child_stack = stack_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Commit to database
    db.add(clem_tiff_file)
    db.commit()
    db.close()
    return True


@router.post("/sessions/{session_id}/clem/metadata_files")
def register_clem_metadata(
    metadata_file: Path,
    session_id: int,
    parent_lif: Optional[Path] = None,
    associated_tiffs: list[Path] = [],
    associated_series: Optional[str] = None,
    associated_stacks: list[Path] = [],
    db: Session = murfey_db,
):

    # Return database entry if it already exists
    try:
        clem_metadata: CLEMImageMetadata = get_db_entry(
            db=db,
            table=CLEMImageMetadata,
            session_id=session_id,
            file_path=metadata_file,
        )
    except Exception:
        logger.error(traceback.format_exc())
        return False

    # Register a parent LIF file if provided
    if parent_lif is not None:
        try:
            lif_db_entry: CLEMLIFFile = get_db_entry(
                db=db,
                table=CLEMLIFFile,
                session_id=session_id,
                file_path=parent_lif,
            )
            # Link database entries
            clem_metadata.parent_lif = lif_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register associated TIFF files if provided
    for tiff in associated_tiffs:
        try:
            tiff_db_entry: CLEMTIFFFile = get_db_entry(
                db=db,
                table=CLEMTIFFFile,
                session_id=session_id,
                file_path=tiff,
            )
            # Append entry
            clem_metadata.associated_tiffs.append(tiff_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Register associated image series if provided
    if associated_series is not None:
        try:
            series_db_entry: CLEMImageSeries = get_db_entry(
                db=db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=associated_series,
            )
            # The link can only be made from series-side; not sure why
            series_db_entry.associated_metadata = clem_metadata
            db.add(series_db_entry)
            db.commit()
        except Exception:
            logger.warning(traceback.format_exc())

    # Register associated image stacks if provided
    for stack in associated_stacks:
        try:
            stack_db_entry: CLEMImageStack = get_db_entry(
                db=db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=stack,
            )
            clem_metadata.associated_stacks.append(stack_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Commit to database
    db.add(clem_metadata)
    db.commit()
    db.close()
    return True


@router.post("/sessions/{session_id}/clem/image_series")
def register_image_series(
    series_name: str,
    session_id: int,
    parent_lif: Optional[Path] = None,
    parent_tiffs: list[Path] = [],
    associated_metadata: Optional[Path] = None,
    child_stacks: list[Path] = [],
    db: Session = murfey_db,
):
    # Get or register series
    try:
        clem_image_series: CLEMImageSeries = get_db_entry(
            db=db,
            table=CLEMImageSeries,
            session_id=session_id,
            series_name=series_name,
        )
    except Exception:
        logger.error(traceback.format_exc())
        return False

    # Register parent LIF file if provided
    if parent_lif is not None:
        try:
            lif_db_entry: CLEMLIFFile = get_db_entry(
                db=db,
                table=CLEMLIFFile,
                session_id=session_id,
                file_path=parent_lif,
            )
            # Link entries
            clem_image_series.parent_lif = lif_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register parent TIFFs if provided
    for tiff in parent_tiffs:
        try:
            tiff_db_entry: CLEMTIFFFile = get_db_entry(
                db=db,
                table=CLEMTIFFFile,
                session_id=session_id,
                file_path=tiff,
            )
            # Append entry
            clem_image_series.parent_tiffs.append(tiff_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue  # Try next item in loop

    # Register associated metadata if provided
    if associated_metadata is not None:
        try:
            metadata_db_entry: CLEMImageMetadata = get_db_entry(
                db=db,
                table=CLEMImageMetadata,
                session_id=session_id,
                file_path=associated_metadata,
            )
            # Link entries
            clem_image_series.associated_metadata = metadata_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register child image stacks if provided
    for stack in child_stacks:
        try:
            stack_db_entry: CLEMImageStack = get_db_entry(
                db=db,
                table=CLEMImageStack,
                session_id=session_id,
                file_path=stack,
            )
            # Append entry
            clem_image_series.child_stacks.append(stack_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Register
    db.add(clem_image_series)
    db.commit()
    db.close()
    return True


@router.post("/sessions/{session_id}/clem/image_stacks")
def register_image_stack(
    image_stack: Path,
    session_id: int,
    channel: Optional[str] = None,
    parent_lif: Optional[Path] = None,
    parent_tiffs: list[Path] = [],
    associated_metadata: Optional[Path] = None,
    parent_series: Optional[str] = None,
    db: Session = murfey_db,
):
    # Get or register image stack entry
    try:
        clem_image_stack: CLEMImageStack = get_db_entry(
            db=db,
            table=CLEMImageStack,
            session_id=session_id,
            file_path=image_stack,
        )
    except Exception:
        logger.error(traceback.format_exc())
        return False

    # Register channel name if provided
    if channel is not None:
        clem_image_stack.channel_name = channel

    # Register parent LIF file if provided
    if parent_lif is not None:
        try:
            lif_db_entry: CLEMLIFFile = get_db_entry(
                db=db,
                table=CLEMLIFFile,
                session_id=session_id,
                file_path=parent_lif,
            )
            clem_image_stack.parent_lif = lif_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register parent TIFF files if provided
    for tiff in parent_tiffs:
        try:
            tiff_db_entry: CLEMTIFFFile = get_db_entry(
                db=db,
                table=CLEMTIFFFile,
                session_id=session_id,
                file_path=tiff,
            )
            # Append entry
            clem_image_stack.parent_tiffs.append(tiff_db_entry)
        except Exception:
            logger.warning(traceback.format_exc())
            continue

    # Register associated metadata if provided
    if associated_metadata is not None:
        try:
            metadata_db_entry: CLEMImageMetadata = get_db_entry(
                db=db,
                table=CLEMImageMetadata,
                session_id=session_id,
                file_path=associated_metadata,
            )
            # Link entries
            clem_image_stack.associated_metadata = metadata_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register parent series if provided
    if parent_series is not None:
        try:
            series_db_entry: CLEMImageSeries = get_db_entry(
                db=db,
                table=CLEMImageSeries,
                session_id=session_id,
                series_name=parent_series,
            )
            # Link entries
            clem_image_stack.parent_series = series_db_entry
        except Exception:
            logger.warning(traceback.format_exc())

    # Register updates to entry
    db.add(clem_image_stack)
    db.commit()
    db.close()
    return True


"""
API ENDPOINTS FOR FILE PROCESSING
"""


@router.post("/sessions/{session_id}/lif_to_stack")  # API posts to this URL
def lif_to_stack(
    session_id: int,  # Used by the decorator
    lif_file: Path,
):
    # Get command line entry point
    murfey_workflows = entry_points().select(
        group="murfey.workflows", name="lif_to_stack"
    )

    # Use entry point if found
    if len(murfey_workflows) == 1:
        workflow: EntryPoint = list(murfey_workflows)[0]
        workflow.load()(
            # Match the arguments found in murfey.workflows.lif_to_stack
            file=lif_file,
            root_folder="images",
            messenger=_transport_object,
        )
        return True
    # Raise error if Murfey workflow not found
    else:
        raise RuntimeError("The relevant Murfey workflow was not found")


@router.post("/sessions/{session_id}/tiff_to_stack")
def tiff_to_stack(
    session_id: int,  # Used by the decorator
    tiff_info: TiffSeriesInfo,
):
    # Get command line entry point
    murfey_workflows = entry_points().select(
        group="murfey.workflows", name="tiff_to_stack"
    )

    # Use entry point if found
    if murfey_workflows:
        workflow: EntryPoint = list(murfey_workflows)[0]
        workflow.load()(
            # Match the arguments found in murfey.workflows.tiff_to_stack
            file=tiff_info.tiff_files[0],  # Pass it only one file from the list
            root_folder="images",
            metadata=tiff_info.series_metadata,
            messenger=_transport_object,
        )
    # Raise error if Murfey workflow not found
    else:
        raise RuntimeError("The relevant Murfey workflow was not found")
