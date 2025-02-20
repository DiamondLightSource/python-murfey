from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional, Type, Union

from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from murfey.util.config import get_machine_config
from murfey.util.db import (
    CLEMImageMetadata,
    CLEMImageSeries,
    CLEMImageStack,
    CLEMLIFFile,
    CLEMTIFFFile,
)
from murfey.util.db import Session as MurfeySession

logger = logging.getLogger("murfey.workflows.clem")


"""
HELPER FUNCTIONS FOR CLEM DATABASE
"""


def _validate_and_sanitise(
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

    valid_file_types = (
        ".lif",
        ".tif",
        ".tiff",
        ".xlif",
        ".xml",
    )

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
    rsync_basepath = machine_config.rsync_basepath.resolve()

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
    if not str(full_path).startswith(str(rsync_basepath)):
        raise ValueError(f"{file} points to a directory that is not permitted")

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
            file_path = _validate_and_sanitise(file_path, session_id, db)
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

    except Exception:
        raise Exception

    return db_entry
