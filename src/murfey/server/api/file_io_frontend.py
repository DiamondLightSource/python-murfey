from logging import getLogger
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import select

from murfey.server.api.auth import (
    MurfeySessionIDFrontend as MurfeySessionID,
    validate_token,
)
from murfey.server.api.file_io_shared import (
    GainReference,
    process_gain as _process_gain,
)
from murfey.server.murfey_db import murfey_db
from murfey.util import secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import Session

logger = getLogger("murfey.server.api.file_io_frontend")


router = APIRouter(
    prefix="/file_io/frontend",
    dependencies=[Depends(validate_token)],
    tags=["File I/O: Frontend"],
)


@router.post("/sessions/{session_id}/process_gain")
async def process_gain(
    session_id: MurfeySessionID, gain_reference_params: GainReference, db=murfey_db
):
    result = await _process_gain(session_id, gain_reference_params, db)
    return result


class SymlinkParameters(BaseModel):
    target: Path  # these are the paths without the rsync basepath as that is what the frontend has access to
    symlink: Path
    override: bool = False


@router.post("/sessions/{session_id}/symlink")
async def create_symlink(
    session_id: MurfeySessionID, symlink_params: SymlinkParameters, db=murfey_db
) -> str:
    murfey_session = db.exec(select(Session).where(Session.id == session_id)).one()
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    rsync_basepath = (machine_config.rsync_basepath or Path("")).resolve()
    symlink_full_path = secure_path(
        rsync_basepath / symlink_params.symlink, keep_spaces=True
    )
    # Verify that the symlink provided does not lead elsewhere
    if not symlink_full_path.resolve().is_relative_to(rsync_basepath):
        logger.warning(
            "Symlink rejected because it will be created in a forbidden location"
        )
        return ""
    # Remove and replace symlink if it exists are 'override' is set
    if symlink_full_path.is_symlink() and symlink_params.override:
        symlink_full_path.unlink()
    # If a file/folder already exists using the desired symlink name, return empty string
    if symlink_full_path.exists():
        return ""
    symlink_full_path.symlink_to(
        secure_path(rsync_basepath / symlink_params.target, keep_spaces=True)
    )
    return str(symlink_params.symlink)
