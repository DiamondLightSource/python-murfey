from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

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


"""
ITEM REGISTRATION WITH THE DATABASE
"""


def validate_file(file: Path) -> bool:
    # Pre-empt accidental string input
    file = Path(file) if isinstance(file, str) else file
    file = file.resolve()  # Get full path

    # Use path to storage location as reference to verify basepath is correct
    basepath = list(machine_config.rsync_basepath.parents)[-2]
    if str(file).startswith(str(basepath)):
        return True
    else:
        return False


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

    if validate_file(lif_file) is True:
        lif_file = Path(lif_file) if isinstance(lif_file, str) else lif_file
        lif_file = lif_file.resolve()  # Get full path

        # Return the database entry to update if it already exists
        try:
            clem_lif_file = db.exec(
                select(CLEMLIFFile)
                .where(CLEMLIFFile.session_id == session_id)
                .where(CLEMLIFFile.file_path == str(lif_file))
            ).one()
        # Create new entry if no result found
        except NoResultFound:
            clem_lif_file = CLEMLIFFile(
                file_path=str(lif_file),
                session_id=session_id,
            )
        # Raise unexpected exceptions
        except Exception:
            return "Something went wrong when registering the LIF file\n" f"{Exception}"
    else:
        raise Exception("The file did not pass the validation check")

    # Add metadata information if provided
    if master_metadata is not None:
        # Add metadata if it exists
        if validate_file(master_metadata) is True:
            master_metadata = (
                Path(master_metadata)
                if isinstance(master_metadata, str)
                else master_metadata
            )
            clem_lif_file.master_metadata = str(master_metadata.resolve())

    # Register child metadata if provided
    if len(child_metadata) > 0:
        for metadata in child_metadata:
            if validate_file(metadata) is True:
                metadata = Path(metadata) if isinstance(metadata, str) else metadata
                metadata = metadata.resolve()  # Get full path

                # Return existing database entry if it already exists
                try:
                    metadata_db_entry = db.exec(
                        select(CLEMImageMetadata)
                        .where(CLEMImageMetadata.session_id == session_id)
                        .where(CLEMImageMetadata.file_path == str(metadata))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    metadata_db_entry = CLEMImageMetadata(
                        file_path=str(metadata),
                        session_id=session_id,
                    )
                    # Register
                    db.add(metadata_db_entry)
                    db.commit()
                    db.refresh(metadata_db_entry)
                # Raise unexpected exceptions
                except Exception:
                    print(
                        "Something went wrong when registering the child metadata files\n"
                        f"{Exception}"
                    )
                # Append to entry
                clem_lif_file.child_metadata.append(metadata_db_entry)
            else:
                print("The file failed the validation check")

    # Register child image series if provided
    if len(child_series) > 0:
        for series in child_series:
            # Return existing database entry if it already exists
            try:
                series_db_entry = db.exec(
                    select(CLEMImageSeries)
                    .where(CLEMImageSeries.session_id == session_id)
                    .where(CLEMImageSeries.name == series)
                ).one()
            # Create new entry if no result found
            except NoResultFound:
                series_db_entry = CLEMImageSeries(
                    name=series,
                    session_id=session_id,
                )
                # Register entry
                db.add(series_db_entry)
                db.commit()
                db.refresh(series_db_entry)
            # Raise unexpected exceptions
            except Exception:
                print(
                    "Something went wrong when registering the child image series\n"
                    f"{Exception}"
                )
            # Append to database entry
            clem_lif_file.child_series.append(series_db_entry)

    # Register child image stacks if provided
    if len(child_stacks) > 0:
        for stack in child_stacks:
            if validate_file(stack) is True:
                stack = Path(stack) if isinstance(stack, str) else stack
                stack = stack.resolve()  # Get full path

                # Return exisiting databse entry if it already exists
                try:
                    stack_db_entry = db.exec(
                        select(CLEMImageStack)
                        .where(CLEMImageStack.session_id == session_id)
                        .where(CLEMImageStack.file_path == str(stack))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    stack_db_entry = CLEMImageStack(
                        session_id=session_id,
                        file_path=str(stack),
                    )
                    db.add(stack_db_entry)
                    db.commit()
                    db.refresh(stack_db_entry)
                # Raise unexpected exceptions
                except Exception:
                    print(
                        "Something went wrong when registering the child image stacks\n"
                        f"{Exception}"
                    )

                # Append to database entry
                clem_lif_file.child_stacks.append(stack_db_entry)
            else:
                print("The file failed the validation check")

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
    if validate_file(tiff_file) is True:
        tiff_file = Path(tiff_file) if isinstance(tiff_file, str) else tiff_file
        tiff_file = tiff_file.resolve()  # Get full file path

        # Returns the database entry if already registered
        try:
            clem_tiff_file = db.exec(
                select(CLEMTIFFFile)
                .where(CLEMTIFFFile.session_id == session_id)
                .where(CLEMTIFFFile.file_path == str(tiff_file))
            ).one()
        # Create new entry if no result found
        except NoResultFound:
            clem_tiff_file = CLEMTIFFFile(
                file_path=str(tiff_file),
                session_id=session_id,
            )
        # Raise unexpected exceptions
        except Exception:
            return (
                "Something went wrong when registering the TIFF file\n" f"{Exception}"
            )
    else:
        raise Exception("The file did not pass the validation check")

    # Add metadata if provided
    if associated_metadata is not None:
        if validate_file(associated_metadata) is True:
            associated_metadata = (
                Path(associated_metadata)
                if isinstance(associated_metadata, str)
                else associated_metadata
            )
            associated_metadata = associated_metadata.resolve()

            # Return database entry if already registered
            try:
                metadata_db_entry = db.exec(
                    select(CLEMImageMetadata)
                    .where(CLEMImageMetadata.session_id == session_id)
                    .where(CLEMImageMetadata.file_path == str(associated_metadata))
                ).one()
                # Link database entries
                clem_tiff_file.metadata_id = metadata_db_entry.id
            # Create new entry if no result found
            except NoResultFound:
                metadata_db_entry = CLEMImageMetadata(
                    file_path=str(associated_metadata),
                    session_id=session_id,
                )
                clem_tiff_file.associated_metadata = metadata_db_entry

            except Exception:
                print(
                    "Something went wrong when registering the associated metadata file"
                )
        else:
            print("The file failed the validation check")

    # Add series information if provided
    if associated_series is not None:
        # Return database entry if already registered
        try:
            series_db_entry = db.exec(
                select(CLEMImageSeries)
                .where(CLEMImageSeries.session_id == session_id)
                .where(CLEMImageSeries.name == associated_series)
            ).one()
            # Link database entries
            clem_tiff_file.series_id = series_db_entry.id
        # Create new entry if no result found
        except NoResultFound:
            series_db_entry = CLEMImageSeries(
                name=associated_series,
                session_id=session_id,
            )
            clem_tiff_file.child_series = series_db_entry

        except Exception:
            print(
                "Something went wrong when registering the associated image series\n"
                f"{Exception}"
            )

    # Add image stack information if provided
    if associated_stack is not None:
        if validate_file(associated_stack) is True:
            associated_stack = (
                Path(associated_stack)
                if isinstance(associated_stack, str)
                else associated_stack
            )
            associated_stack = associated_stack.resolve()

            # Return database entry if already registered
            try:
                stack_db_entry = db.exec(
                    select(CLEMImageStack)
                    .where(CLEMImageStack.session_id == session_id)
                    .where(CLEMImageStack.file_path == str(associated_stack))
                ).one()
                # Link database entries
                clem_tiff_file.stack_id = stack_db_entry.id
            except NoResultFound:
                stack_db_entry = CLEMImageStack(
                    file_path=str(associated_stack), session_id=session_id
                )
                clem_tiff_file.child_stack = stack_db_entry

            except Exception:
                print(
                    "Something went wrong when registering the associated image stack\n"
                    f"{Exception}"
                )
        else:
            print("The file failed the validation check")

    # Commit to database
    db.add(clem_tiff_file)
    db.commit()
    db.close()
    return True


@router.post("/sessions/{session_id}/clem/metadata")
def register_clem_metadata(
    metadata_file: Path,
    session_id: int,
    parent_lif: Optional[Path] = None,
    associated_tiffs: list[Path] = [],
    associated_series: Optional[str] = None,
    associated_stacks: list[Path] = [],
    db: Session = murfey_db,
):
    if validate_file(metadata_file) is True:
        # Convert incoming file paths into absolute ones
        metadata_file = (
            Path(metadata_file) if isinstance(metadata_file, str) else metadata_file
        )
        metadata_file = metadata_file.resolve()

        # Return database entry if it already exists
        try:
            clem_metadata = db.exec(
                select(CLEMImageMetadata)
                .where(CLEMImageMetadata.session_id == session_id)
                .where(CLEMImageMetadata.file_path == str(metadata_file))
            ).one()
        # Create new entry if no result found
        except NoResultFound:
            clem_metadata = CLEMImageMetadata(
                file_path=str(metadata_file),
                session_id=session_id,
            )
        except Exception:
            return (
                "Something went wrong when registering the metadata file\n"
                f"{Exception}"
            )
    else:
        raise Exception("The file failed the validation check")

    # Register a parent LIF file if provided
    if parent_lif is not None:
        if validate_file(parent_lif) is True:
            parent_lif = Path(parent_lif) if isinstance(parent_lif, str) else parent_lif
            parent_lif = parent_lif.resolve()  # Get full path

            # Return database entry if it exists
            try:
                lif_file_db_entry = db.exec(
                    select(CLEMLIFFile)
                    .where(CLEMLIFFile.session_id == session_id)
                    .where(CLEMLIFFile.file_path == str(parent_lif))
                ).one()
                # Link ID
                clem_metadata.parent_lif_id = lif_file_db_entry.id
            # Create new entry if no result found
            except NoResultFound:
                lif_file_db_entry = CLEMLIFFile(
                    file_path=str(parent_lif),
                    session_id=session_id,
                )
                # Register entry
                clem_metadata.parent_lif = lif_file_db_entry
            except Exception:
                print(
                    "Something went wrong when registering the parent LIF file\n"
                    f"{Exception}"
                )
        else:
            print("The file didn't pass the validation check")

    # Register associated TIFF files if provided
    if len(associated_tiffs) > 0:
        for tiff in associated_tiffs:
            if validate_file(tiff) is True:
                tiff = Path(tiff) if isinstance(tiff, str) else tiff
                tiff = tiff.resolve()

                # Return database entry if it exists
                try:
                    tiff_db_entry = db.exec(
                        select(CLEMTIFFFile)
                        .where(CLEMTIFFFile.session_id == session_id)
                        .where(CLEMTIFFFile.file_path == str(tiff))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    tiff_db_entry = CLEMTIFFFile(
                        session_id=session_id,
                        file_path=str(tiff),
                    )
                    # Register entry
                    db.add(tiff_db_entry)
                    db.commit()
                    db.refresh(tiff_db_entry)
                except Exception:
                    print(
                        "Something went wrong when registering the TIFF file\n"
                        f"{Exception}"
                    )
                    continue
                # Append entry
                clem_metadata.associated_tiffs.append(tiff_db_entry)
            else:
                print("The file failed the validation check")

    # Register associated image series if provided
    if associated_series is not None:
        # Return database entry if it already exists
        try:
            series_db_entry = db.exec(
                select(CLEMImageSeries)
                .where(CLEMImageSeries.session_id == session_id)
                .where(CLEMImageSeries.name == associated_series)
            ).one()
            clem_metadata.associated_series = series_db_entry
        # Create new entry if no result found
        except NoResultFound:
            series_db_entry = CLEMImageSeries(
                name=associated_series,
                session_id=session_id,
            )
            clem_metadata.associated_series = series_db_entry
        except Exception:
            print(
                "Something went wrong when registering the associated series\n"
                f"{Exception}"
            )

    # Register associated image stacks if provided
    if len(associated_stacks) > 0:
        for stack in associated_stacks:
            if validate_file(stack) is True:
                stack = Path(stack) if isinstance(stack, str) else stack
                stack = stack.resolve()

                # Return database entry if it already exists
                try:
                    stack_db_entry = db.exec(
                        select(CLEMImageStack)
                        .where(CLEMImageStack.session_id == session_id)
                        .where(CLEMImageStack.file_path == str(stack))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    stack_db_entry = CLEMImageStack(
                        file_path=str(stack),
                        session_id=session_id,
                    )
                    db.add(stack_db_entry)
                    db.commit()
                    db.refresh(stack_db_entry)
                except Exception:
                    return "Something went wrong when registering the associated image stacks"
                # Append entry
                clem_metadata.associated_stacks.append(stack_db_entry)
            else:
                print("The file failed the validation check")

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
    # Register series name if not already present in databaes
    try:
        clem_image_series = db.exec(
            select(CLEMImageSeries)
            .where(CLEMImageSeries.session_id == session_id)
            .where(CLEMImageSeries.name == series_name)
        ).one()
    # Create new entry if no result found
    except NoResultFound:
        clem_image_series = CLEMImageSeries(
            name=series_name,
            session_id=session_id,
        )
    except Exception:
        return "Something went wrong when registering the series\n" f"{Exception}"

    # Register parent LIF file
    if parent_lif is not None:
        if validate_file(parent_lif) is True:
            parent_lif = Path(parent_lif) if isinstance(parent_lif, str) else parent_lif
            parent_lif = parent_lif.resolve()  # Get full path

            # Return database entry if already registered
            try:
                lif_db_entry = db.exec(
                    select(CLEMLIFFile)
                    .where(CLEMLIFFile.session_id == session_id)
                    .where(CLEMLIFFile.file_path == str(parent_lif))
                ).one()
                clem_image_series.parent_lif_id = lif_db_entry.id
            # Create new entry if no result found
            except NoResultFound:
                lif_db_entry = CLEMLIFFile(
                    file_path=str(parent_lif),
                    session_id=session_id,
                )
                clem_image_series.parent_lif = lif_db_entry
            except Exception:
                print(
                    "Something went wrong when registering the LIF file\n"
                    f"{Exception}"
                )
        else:
            print("The file did not pass the validation check")

    # Register parent TIFFs
    if len(parent_tiffs) > 0:
        for tiff in parent_tiffs:
            if validate_file(tiff) is True:
                tiff = Path(tiff) if isinstance(tiff, str) else tiff
                tiff = tiff.resolve()

                # Return database entry if it exists
                try:
                    tiff_db_entry = db.exec(
                        select(CLEMTIFFFile)
                        .where(CLEMTIFFFile.session_id == session_id)
                        .where(CLEMTIFFFile.file_path == str(tiff))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    tiff_db_entry = CLEMTIFFFile(
                        file_path=str(tiff),
                        session_id=session_id,
                    )
                except Exception:
                    print("Something went wrong when registering the TIFF file")
                    continue  # Try next item in loop
                # Append entry
                db.add(tiff_db_entry)
                db.commit()
                db.refresh(tiff_db_entry)
                clem_image_series.parent_tiffs.append(tiff_db_entry)
            else:
                print("The file didn't pass the validation check")

    # Register associated metadata
    if associated_metadata is not None:
        if validate_file(associated_metadata) is True:
            associated_metadata = (
                Path(associated_metadata)
                if isinstance(associated_metadata, str)
                else associated_metadata
            )
            associated_metadata = associated_metadata.resolve()  # Get full path

            # Return database entry if it already exists
            try:
                metadata_db_entry = db.exec(
                    select(CLEMImageMetadata)
                    .where(CLEMImageMetadata.session_id == session_id)
                    .where(CLEMImageMetadata.file_path == str(associated_metadata))
                ).one()
                clem_image_series.metadata_id = metadata_db_entry.id
            # Create new entry if no result found
            except NoResultFound:
                metadata_db_entry = CLEMImageMetadata(
                    file_path=str(associated_metadata),
                    session_id=session_id,
                )
                clem_image_series.associated_metadata = metadata_db_entry
            except Exception:
                print(
                    "Something went wrong when registering the metadata file\n"
                    f"{Exception}"
                )

    # Register child image stacks
    if len(child_stacks) > 0:
        for stack in child_stacks:
            if validate_file(stack) is True:
                stack = Path(stack) if isinstance(stack, str) else stack
                stack.resolve()  # Get full path

                # Return database entry if it exists
                try:
                    stack_db_entry = db.exec(
                        select(CLEMImageStack)
                        .where(CLEMImageStack.session_id == session_id)
                        .where(CLEMImageStack.file_path == str(stack))
                    ).one()
                # Create new entry if no result found
                except NoResultFound:
                    stack_db_entry = CLEMImageStack(
                        file_path=str(stack),
                        session_id=session_id,
                    )
                    db.add(stack_db_entry)
                    db.commit()
                    db.refresh(stack_db_entry)
                except Exception:
                    print(
                        "Something went wrong when registering the image stack\n"
                        f"{Exception}"
                    )
                # Append entry
                clem_image_series.child_stacks.append(stack_db_entry)
            else:
                print("The file failed the validation check")

    # Register
    db.add(clem_image_series)
    db.commit()
    db.close()

    return True


@router.post("/sessions/{session_id}/clem/image_stacks")
def register_image_stack():
    # Register image stack

    # Register associated metadata

    # Register parent series it belongs to

    # Register parent LIF file

    # Register parent TIFF files

    return True


"""
FILE PROCESSING ENDPOINTS
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
