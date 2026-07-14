import logging
from pathlib import Path

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from murfey.server import _transport_object
from murfey.server.api.auth import validate_instrument_token
from murfey.util import sanitise_path

logger = logging.getLogger("murfey.server.api.workflow_sim")

router = APIRouter(
    prefix="/workflow/sim",
    dependencies=[Depends(validate_instrument_token)],
    tags=["Workflows: CryoSIM"],
)


class SIMDataFile(BaseModel):
    file: Path


@router.post("/sessions/{session_id}/process_data")
def request_sim_processing(session_id: int, sim_data: SIMDataFile):
    if _transport_object is None:
        logger.error("No TransportManager object was set up")
        return None

    # Construct message and submit it to 'processing_recipe'
    logger.info(
        f"Submitting request to process the cryoSIM file {sanitise_path(sim_data.file)}"
    )
    recipe = {
        # Placeholder; fields will be populated once service is set up
        "recipes": ["sim-process-data"],
        "parameters": {
            # Job parameters
            "file": f"{str(sim_data.file)}",
            "feedback_queue": _transport_object.feedback_queue,
        },
    }
    _transport_object.send(
        queue="processing_recipe", message=recipe, new_connection=True
    )
