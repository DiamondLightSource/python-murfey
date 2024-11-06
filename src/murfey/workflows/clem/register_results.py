"""
Functions to process the requests received by Murfey related to the CLEM workflow.

The CLEM-related file registration API endpoints can eventually be moved here, since
the file registration processes all take place on the server side only.
"""

from __future__ import annotations

import json
import logging
import re
import traceback
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
from murfey.util.models import LIFPreprocessingResult, TIFFPreprocessingResult

logger = logging.getLogger("murfey.workflows.clem.register_results")


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
    rsync_basepath = machine_config.rsync_basepath
    try:
        base_path = list(rsync_basepath.parents)[-2].as_posix()
    except IndexError:
        logger.warning(f"Base path {rsync_basepath!r} is too short")
        base_path = rsync_basepath.as_posix()
    except Exception as e:
        raise Exception(
            f"Unexpected exception encountered when loading the file base path: {e}"
        )

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
        db.refresh(db_entry)
    except Exception:
        raise Exception

    return db_entry


def register_lif_preprocessing_result(
    message: dict, db: Session, demo: bool = False
) -> bool:
    """
    session_id (recipe)
    register (wrapper)
    result (wrapper)
        key1
        key2
        ...
    """

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )

    # Validate message and try and load results
    if isinstance(message["result"], str):
        try:
            json_obj: dict = json.loads(message["result"])
            result = LIFPreprocessingResult(**json_obj)
        except Exception:
            logger.error(traceback.format_exc())
            logger.error("Exception encountered when parsing LIF preprocessing result")
            return False
    elif isinstance(message["result"], dict):
        try:
            result = LIFPreprocessingResult(**message["result"])
        except Exception:
            logger.error(traceback.format_exc())
            logger.error("Exception encountered when parsing LIF preprocessing result")
            return False
    else:
        logger.error(
            f"Invalid type for LIF preprocessing result: {type(message['result'])}"
        )
        return False

    # Register items in database if not already present
    try:
        clem_img_stk: CLEMImageStack = get_db_entry(
            db=db,
            table=CLEMImageStack,
            session_id=session_id,
            file_path=result.image_stack,
        )

        clem_img_series: CLEMImageSeries = get_db_entry(
            db=db,
            table=CLEMImageSeries,
            session_id=session_id,
            series_name=result.series_name,
        )

        clem_metadata: CLEMImageMetadata = get_db_entry(
            db=db,
            table=CLEMImageMetadata,
            session_id=session_id,
            file_path=result.metadata,
        )

        clem_lif_file: CLEMLIFFile = get_db_entry(
            db=db,
            table=CLEMLIFFile,
            session_id=session_id,
            file_path=result.parent_lif,
        )

        # Link tables to one another and populate fields
        clem_img_stk.associated_metadata = clem_metadata
        clem_img_stk.parent_lif = clem_lif_file
        clem_img_stk.parent_series = clem_img_series
        clem_img_stk.channel_name = result.channel
        clem_img_stk.stack_created = True
        db.add(clem_img_stk)
        db.commit()
        db.refresh(clem_img_stk)

        clem_img_series.associated_metadata = clem_metadata
        clem_img_series.parent_lif = clem_lif_file
        clem_img_series.number_of_members = result.number_of_members
        db.add(clem_img_series)
        db.commit()
        db.refresh(clem_img_series)

        clem_metadata.parent_lif = clem_lif_file
        db.add(clem_metadata)
        db.commit()
        db.refresh(clem_metadata)

        logger.info(
            f"LIF preprocessing results registered for {result.series_name!r} {result.channel!r} image stack"
        )
        return True

    except Exception:
        logger.error(traceback.format_exc())
        logger.error(
            f"Exception encountered when registering LIF preprocessing result for {result.series_name!r} {result.channel!r} image stack"
        )
        return False

    finally:
        db.close()


def register_tiff_preprocessing_result(
    message: dict, db: Session, demo: bool = False
) -> bool:

    session_id: int = (
        int(message["session_id"])
        if not isinstance(message["session_id"], int)
        else message["session_id"]
    )
    if isinstance(message["result"], str):
        try:
            json_obj: dict = json.loads(message["result"])
            result = TIFFPreprocessingResult(**json_obj)
        except Exception:
            logger.error(traceback.format_exc())
            logger.error("Exception encountered when parsing TIFF preprocessing result")
            return False
    elif isinstance(message["result"], dict):
        try:
            result = TIFFPreprocessingResult(**message["result"])
        except Exception:
            logger.error(traceback.format_exc())
            logger.error("Exception encountered when parsing TIFF preprocessing result")
            return False
    else:
        logger.error(
            f"Invalid type for TIFF preprocessing result: {type(message['result'])}"
        )
        return False

    # Register items in database if not already present
    try:
        clem_img_stk: CLEMImageStack = get_db_entry(
            db=db,
            table=CLEMImageStack,
            session_id=session_id,
            file_path=result.image_stack,
        )
        clem_img_series: CLEMImageSeries = get_db_entry(
            db=db,
            table=CLEMImageSeries,
            session_id=session_id,
            series_name=result.series_name,
        )
        clem_metadata: CLEMImageMetadata = get_db_entry(
            db=db,
            table=CLEMImageMetadata,
            session_id=session_id,
            file_path=result.metadata,
        )

        # Link tables to one another and populate fields
        # Register TIFF files and populate them iteratively first
        for file in result.parent_tiffs:
            clem_tiff_file: CLEMTIFFFile = get_db_entry(
                db=db,
                table=CLEMTIFFFile,
                session_id=session_id,
                file_path=file,
            )
            clem_tiff_file.associated_metadata = clem_metadata
            clem_tiff_file.child_series = clem_img_series
            clem_tiff_file.child_stack = clem_img_stk
            db.add(clem_tiff_file)
            db.commit()
            db.refresh(clem_tiff_file)

        clem_img_stk.associated_metadata = clem_metadata
        clem_img_stk.parent_series = clem_img_series
        clem_img_stk.channel_name = result.channel
        clem_img_stk.stack_created = True
        db.add(clem_img_stk)
        db.commit()
        db.refresh(clem_img_stk)

        clem_img_series.associated_metadata = clem_metadata
        clem_img_series.number_of_members = result.number_of_members
        db.add(clem_img_series)
        db.commit()
        db.refresh(clem_img_series)

        logger.info(
            f"TIFF preprocessing results registered for {result.series_name!r} {result.channel!r} image stack"
        )
        return True

    except Exception:
        logger.error(traceback.format_exc())
        logger.error(
            f"Exception encountered when registering TIFF preprocessing result for {result.series_name!r} {result.channel!r} image stack"
        )
        return False

    finally:
        db.close()
