from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse
from sqlmodel import select

from murfey.server.murfey_db import murfey_db
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, FoilHole, GridSquare

# Create APIRouter class object
router = APIRouter(prefix="/display", tags=["Display"])
machine_config = get_machine_config()


@router.get("/instruments/{instrument_name}/instrument_name")
def get_instrument_display_name(instrument_name: str) -> str:
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config:
        return machine_config.display_name
    return ""


@router.get("/instruments/{instrument_name}/image/")
def get_mic_image(instrument_name: str):
    if machine_config[instrument_name].image_path:
        return FileResponse(machine_config[instrument_name].image_path)
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


@router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{grid_square_name}/foil_holes/{foil_hole_name}/image"
)
def get_foil_hole_img(
    session_id: int,
    dcgid: int,
    grid_square_name: int,
    foil_hole_name: int,
    db=murfey_db,
):
    foil_hole = db.exec(
        select(FoilHole, GridSquare, DataCollectionGroup)
        .where(FoilHole.name == foil_hole_name)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
        .where(GridSquare.name == grid_square_name)
    ).one()
    return FileResponse(foil_hole[0].image)
