from logging import getLogger

from fastapi import APIRouter, Depends

from murfey.server.api.auth import MurfeySessionIDFrontend as MurfeySessionID
from murfey.server.api.auth import validate_token
from murfey.server.api.file_io_shared import GainReference
from murfey.server.api.file_io_shared import process_gain as _process_gain
from murfey.server.murfey_db import murfey_db

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
