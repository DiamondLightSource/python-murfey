from __future__ import annotations

from fastapi import APIRouter

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
    convert_lif_to_tiff(file=lif_info.name)
