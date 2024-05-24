from __future__ import annotations

import importlib.metadata

from fastapi import APIRouter

from murfey.server import _transport_object
from murfey.util.lif import convert_lif_to_tiff
from murfey.util.models import LifFileInfo

# Create APIRouter class object
router = APIRouter()


# Allow function to be seen as an endpoint by the router
@router.post("/sessions/{session_id}/lif_to_tiff")
def lif_to_tiff(
    session_id: int,  # Used by the decorator
    lif_info: LifFileInfo,
):
    murfey_workflows = importlib.metadata.entry_points().select(
        group="murfey.workflows", name="lif_to_tiff"
    )
    if murfey_workflows:
        murfey_workflows[0].load()(
            file=lif_info.name, root_folder="images", messenger=_transport_object
        )
    else:
        convert_lif_to_tiff(
            file=lif_info.name,
            root_folder="images",
        )
