from __future__ import annotations

import importlib.metadata

from fastapi import APIRouter

from murfey.server import _transport_object
from murfey.util.clem.lif import convert_lif_to_tiff
from murfey.util.clem.tiff import convert_tiff_to_stack
from murfey.util.models import LifFileInfo, TiffSeriesInfo

# Create APIRouter class object
router = APIRouter()


@router.post("/sessions/{session_id}/lif_to_tiff")  # API posts to this URL
def lif_to_tiff(
    session_id: int,  # Used by the decorator
    lif_info: LifFileInfo,
):
    murfey_workflows = importlib.metadata.entry_points().select(
        group="murfey.workflows", name="lif_to_tiff"
    )
    if murfey_workflows:
        murfey_workflows[0].load()(
            # Match the arguments found in murfey.workflows.lif_to_tiff
            file=lif_info.name,
            root_folder="images",
            messenger=_transport_object,
        )
    else:
        convert_lif_to_tiff(
            file=lif_info.name,
            root_folder="images",
        )


# WORK IN PROGRESS
@router.post("/sessions/{session_id}/tiff_to_stack")
def tiff_to_stack(
    session_id: int,  # Used by the decorator
    tiff_info: TiffSeriesInfo,
):
    murfey_workflows = importlib.metadata.entry_points().select(
        group="murfey.workflows", name="tiff_to_stack"
    )
    if murfey_workflows:
        murfey_workflows[0].load()(
            # Match the arguments found in murfey.workflows.tiff_to_stack
            file=tiff_info.tiff_files,
            root_folder="images",
            metadata=tiff_info.series_metadata,
            messenger=_transport_object,
        )
    else:
        convert_tiff_to_stack(
            tiff_list=tiff_info.tiff_files,
            root_folder="images",
            metadata_file=tiff_info.series_metadata,
        )
