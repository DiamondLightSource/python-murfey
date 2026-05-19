import os
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select
from werkzeug.utils import secure_filename

from murfey.server.api.auth import (
    MurfeySessionIDInstrument as MurfeySessionID,
    validate_instrument_token,
)
from murfey.server.api.file_io_shared import (
    GainReference,
    process_gain as _process_gain,
)
from murfey.server.murfey_db import murfey_db
from murfey.util import sanitise, secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import Session, SessionProcessingParameters
from murfey.util.eer import num_frames

logger = getLogger("murfey.server.api.file_io_instrument")


router = APIRouter(
    prefix="/file_io/instrument",
    dependencies=[Depends(validate_instrument_token)],
    tags=["File I/O: Instrument"],
)


class SuggestedPathParameters(BaseModel):
    base_path: (
        Path  # Partial Path starting from immediately after the rsync destination
    )
    touch: bool = False
    extra_directory: str = ""


@router.post("/visits/{visit_name}/sessions/{session_id}/suggested_path")
def suggest_path(
    visit_name: str, session_id: int, params: SuggestedPathParameters, db=murfey_db
):
    count: Optional[int] = None
    secure_path_parts = [secure_filename(p) for p in params.base_path.parts]
    base_path = "/".join(secure_path_parts)
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if not machine_config:
        raise ValueError(
            "No machine configuration set when suggesting destination path"
        )

    # Construct the full path to where the dataset is to be saved
    rsync_basepath = (machine_config.rsync_basepath or Path("")).resolve()
    check_path = rsync_basepath / base_path

    # Check previous year to account for the year rolling over during data collection
    if not check_path.parent.exists():
        base_path_parts = base_path.split("/")
        for part in base_path_parts:
            # Find the path part corresponding to the year
            if len(part) == 4 and part.isdigit():
                year_idx = base_path_parts.index(part)
                base_path_parts[year_idx] = str(int(part) - 1)
        base_path = "/".join(base_path_parts)
        check_path_prev = check_path
        check_path = rsync_basepath / base_path

        # If it's not in the previous year either, it's a genuine error
        if not check_path.parent.exists():
            log_message = (
                "Unable to find current visit folder under "
                f"{str(check_path_prev.parent)!r} or {str(check_path.parent)!r}"
            )
            logger.error(log_message)
            raise FileNotFoundError(log_message)

    check_path_name = check_path.name
    if not machine_config.single_data_directory:
        while check_path.exists():
            count = count + 1 if count else 2
            check_path = check_path.parent / f"{check_path_name}{count}"
    if params.touch:
        check_path.mkdir(exist_ok=True)
        os.chmod(check_path, mode=machine_config.mkdir_chmod)
        if params.extra_directory:
            extra_dir = check_path / secure_filename(params.extra_directory)
            extra_dir.mkdir(exist_ok=True)
            os.chmod(extra_dir, mode=machine_config.mkdir_chmod)
    return {"suggested_path": check_path.relative_to(rsync_basepath)}


class Dest(BaseModel):
    destination: Path


@router.post("/sessions/{session_id}/make_rsyncer_destination")
def make_rsyncer_destination(session_id: int, destination: Dest, db=murfey_db):
    secure_path_parts = [secure_filename(p) for p in destination.destination.parts]
    destination_path = "/".join(secure_path_parts)
    session_entry = db.exec(select(Session).where(Session.id == session_id)).one()
    instrument_name = session_entry.instrument_name
    visit = session_entry.visit
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if not machine_config:
        raise ValueError("No machine configuration set when making rsyncer destination")

    # Make the destination directory and all parents
    full_destination_path = (
        machine_config.rsync_basepath or Path("")
    ).resolve() / destination_path
    full_destination_path.mkdir(parents=True, exist_ok=True)

    # Change permissions for every folder after the visit directory
    try:
        visit_index = full_destination_path.parts.index(visit)
    except ValueError:
        logger.error(f"Could not find directory level {visit!r} in destination path")
        raise
    current_path = full_destination_path.parents[-(visit_index + 1)]
    for part in full_destination_path.parts[visit_index + 1 :]:
        current_path = current_path / part
        try:
            os.chmod(current_path, mode=machine_config.mkdir_chmod)
        except PermissionError:
            logger.warning(f"Unable to change permissions for {current_path}")
            continue
    return destination


@router.post("/sessions/{session_id}/process_gain")
async def process_gain(
    session_id: MurfeySessionID, gain_reference_params: GainReference, db=murfey_db
):
    result = await _process_gain(session_id, gain_reference_params, db)
    return result


class FractionationParameters(BaseModel):
    fractionation: int
    dose_per_frame: Optional[float] = None
    num_frames: int = 0
    eer_path: Optional[str] = None
    fractionation_file_name: str = "eer_fractionation.txt"


@router.post("/visits/{visit_name}/sessions/{session_id}/eer_fractionation_file")
async def write_eer_fractionation_file(
    visit_name: str,
    session_id: int,
    fractionation_params: FractionationParameters,
    db=murfey_db,
) -> dict:
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config.eer_fractionation_file_template:
        file_path = Path(
            machine_config.eer_fractionation_file_template.format(
                visit=secure_filename(visit_name),
                year=str(datetime.now().year),
            )
        ) / secure_filename(fractionation_params.fractionation_file_name)
    else:
        file_path = (
            (machine_config.rsync_basepath or Path("")).resolve()
            / str(datetime.now().year)
            / secure_filename(visit_name)
            / machine_config.gain_directory_name
            / secure_filename(fractionation_params.fractionation_file_name)
        )

    session_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_parameters:
        fractionation_params.dose_per_frame = session_parameters[0].dose_per_frame
        fractionation_params.fractionation = session_parameters[0].eer_fractionation
        session_parameters[0].eer_fractionation_file = str(file_path)
        db.add(session_parameters[0])
        db.commit()

    if file_path.is_file():
        return {"eer_fractionation_file": str(file_path)}

    if not fractionation_params.dose_per_frame:
        logger.error("EER fractionation dose not set")
        return {"eer_fractionation_file": None}

    if fractionation_params.num_frames:
        num_eer_frames = fractionation_params.num_frames
    elif (
        fractionation_params.eer_path
        and secure_path(Path(fractionation_params.eer_path)).is_file()
    ):
        num_eer_frames = num_frames(Path(fractionation_params.eer_path))
    else:
        logger.warning(
            f"EER fractionation unable to find {secure_path(Path(fractionation_params.eer_path)) if fractionation_params.eer_path else None} "
            f"or use {int(sanitise(str(fractionation_params.num_frames)))} frames"
        )
        return {"eer_fractionation_file": None}
    with open(file_path, "w") as frac_file:
        frac_file.write(
            f"{num_eer_frames} {fractionation_params.fractionation} {fractionation_params.dose_per_frame / fractionation_params.fractionation}"
        )
    return {"eer_fractionation_file": str(file_path)}
