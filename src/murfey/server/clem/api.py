from __future__ import annotations

import sys
from os.path import normpath
from pathlib import Path
from typing import Optional, Type, Union

from fastapi import APIRouter
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from murfey.server import _transport_object
from murfey.server.config import get_machine_config
from murfey.server.murfey_db import murfey_db
from murfey.util.db import (
    CLEMImageMetadata,
    CLEMImageSeries,
    CLEMImageStack,
    CLEMLIFFile,
    CLEMTIFFFile,
)
from murfey.util.models import LifFileInfo, TiffSeriesInfo

# Use backport from importlib_metadata for Python <3.10
if sys.version_info.major == 3 and sys.version_info.minor < 10:
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


# Create APIRouter class object
router = APIRouter()


# Use machine configuration to validate file paths used here
machine_config = get_machine_config()


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


def validate_file_path(file: Path) -> Path:
    """
    Validates the input file paths used for the CLEM workflow, making sure that they
    are safe to be executed and passed on to subsequent stages of the workflow. Returns
    a validated, sanitised version of the file path.
    """

    file = Path(file) if isinstance(file, str) else file
    file = file.resolve()  # Get full path for inspection

    # Validate file types
    if not file.exists():
        raise Exception(f"{file!r} doesn't exist")
    if not file.is_file():
        raise Exception(f"{file!r} is not a file")
    if file.suffix not in valid_file_types:
        raise Exception(f"{file!r} is not a valid file type")

    # Try validating with os.path functions
    full_path = normpath(str(file))

    # Use path to storage location as reference to verify basepath is correct
    base_path = normpath(list(machine_config.rsync_basepath.parents)[-3])
    if str(full_path).startswith(str(base_path)):
        return Path(full_path).resolve()
    else:
        raise Exception(f"{file!r} points to a directory that is not permitted")


def get_db_entry(
    db: Session,
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
    # Validate that parameters are provided correctly
    if file_path is None and series_name is None:
        raise ValueError(
            "One of either 'file_path' or 'series_name' has to be provided"
        )
    if file_path is not None and series_name is not None:
        raise ValueError("Only one of 'file_path' or 'series_name' can be provided")

    # Validate file path if provided
    if file_path is not None:
        try:
            file_path = validate_file_path(file_path)
        except Exception:
            raise Exception

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
        raise Exception

    # Add metadata information if provided
    if master_metadata is not None:
        try:
            master_metadata = validate_file_path(master_metadata)
            clem_lif_file.master_metadata = str(master_metadata)
        except Exception:
            print(Exception)

    # Register child metadata if provided
    if len(child_metadata) > 0:
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
                print(Exception)
                continue

    # Register child image series if provided
    if len(child_series) > 0:
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
                print(Exception)
                continue

    # Register child image stacks if provided
    if len(child_stacks) > 0:
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
                print(Exception)
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
        raise Exception

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
            print(Exception)

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
            print(Exception)

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
            print(Exception)

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
        raise Exception

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
            print(Exception)

    # Register associated TIFF files if provided
    if len(associated_tiffs) > 0:
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
                print(Exception)
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
            clem_metadata.associated_series = series_db_entry
        except Exception:
            print(Exception)

    # Register associated image stacks if provided
    if len(associated_stacks) > 0:
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
                print(Exception)
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
        raise Exception

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
            print(Exception)

    # Register parent TIFFs if provided
    if len(parent_tiffs) > 0:
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
                print(Exception)
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
            print(Exception)

    # Register child image stacks if provided
    if len(child_stacks) > 0:
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
                print(Exception)
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
        raise Exception

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
            print(Exception)

    # Register parent TIFF files if provided
    if len(parent_tiffs) > 0:
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
                print(Exception)
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
            print(Exception)

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
            print(Exception)

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
    lif_info: LifFileInfo,
):
    # Get command line entry point
    murfey_workflows = entry_points().select(
        group="murfey.workflows", name="lif_to_stack"
    )

    # Use entry point if found
    if murfey_workflows:
        murfey_workflows[0].load()(
            # Match the arguments found in murfey.workflows.lif_to_stack
            file=lif_info.name,
            root_folder="images",
            messenger=_transport_object,
        )
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
        murfey_workflows[0].load()(
            # Match the arguments found in murfey.workflows.tiff_to_stack
            file=tiff_info.tiff_files[0],  # Pass it only one file from the list
            root_folder="images",
            metadata=tiff_info.series_metadata,
            messenger=_transport_object,
        )
    # Raise error if Murfey workflow not found
    else:
        raise RuntimeError("The relevant Murfey workflow was not found")
