import logging
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from murfey.server import _transport_object
from murfey.server.api.auth import validate_instrument_token
from murfey.server.murfey_db import murfey_db
from murfey.util.models import FIBGIFParameters, LamellaSiteInfo

logger = logging.getLogger("murfey.server.api.workflow_fib")

router = APIRouter(
    prefix="/workflow/fib",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: FIB milling"],
)


class FIBAtlasFile(BaseModel):
    file: Path


@router.post("/sessions/{session_id}/register_atlas")
def register_fib_atlas(
    session_id: int,
    fib_atlas: FIBAtlasFile,
):
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return None
    _transport_object.send(
        _transport_object.feedback_queue,
        {
            "register": "fib.register_atlas",
            "session_id": session_id,
            "atlas_file": str(fib_atlas.file),
        },
    )


@router.post("/sessions/{session_id}/register_milling_progress")
def register_fib_milling_progress(
    session_id: int,
    site_info: LamellaSiteInfo,
):
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return None
    _transport_object.send(
        _transport_object.feedback_queue,
        {
            "register": "fib.register_milling_progress",
            "session_id": session_id,
            "site_info": site_info.model_dump(exclude_none=True),
        },
    )


@router.post("/sessions/{session_id}/make_gif")
async def make_gif(
    session_id: int,
    gif_params: FIBGIFParameters,
    db=murfey_db,
):
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return None
    _transport_object.send(
        _transport_object.feedback_queue,
        {
            "register": "fib.make_milling_gif",
            "session_id": session_id,
            "gif_params": gif_params.model_dump(mode="json"),
        },
    )
