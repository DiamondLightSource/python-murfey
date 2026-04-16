import logging
from importlib.metadata import entry_points
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session

from murfey.server.api.auth import validate_instrument_token
from murfey.server.murfey_db import murfey_db

logger = logging.getLogger("murfey.server.api.workflow_fib")

router = APIRouter(
    prefix="/workflow/fib",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: FIB milling"],
)


class FIBAtlasInfo(BaseModel):
    file: Path | None = None


@router.post("/sessions/{session_id}/register_atlas")
def register_fib_atlas(
    session_id: int,
    fib_atlas_info: FIBAtlasInfo,
    db: Session = murfey_db,
):
    # See if the relevant workflow is available
    if not (
        workflow_search := list(
            entry_points(group="murfey.workflows", name="fib.register_atlas")
        )
    ):
        raise RuntimeError("Unable to find Murfey workflow to register FIB atlas")
    workflow = workflow_search[0]

    # Run the workflow
    workflow.load()(
        session_id=session_id,
        file=fib_atlas_info.file,
        murfey_db=db,
    )
