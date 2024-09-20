from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse
from sqlmodel import select

from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, GridSquare

# Create APIRouter class object
router = APIRouter(prefix="/display", tags=["display"])
machine_config = get_machine_config()


@router.get("/microscope_image/")
def get_mic_image():
    if machine_config.image_path:
        return FileResponse(machine_config.image_path)
    return None


@router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{grid_square_name}/image"
)
def get_grid_square_img(
    session_id: int, dcgid: int, grid_square_name: int, db=murfey_db
):
    grid_square = db.exec(
        select(GridSquare, DataCollectionGroup)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
        .where(GridSquare.name == grid_square_name)
    ).one()
    return FileResponse(grid_square[0].image)
