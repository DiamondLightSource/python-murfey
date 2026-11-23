import logging

from sqlmodel import Session as SQLModelSession

from murfey.util.models import GridSquareParameters
from murfey.workflows.spa.register_grid_square import register_grid_square

logger = logging.getLogger("murfey.workflows.spa.register_tile")


def register_tiles(
    session_id: int,
    tile_group: dict[int, GridSquareParameters],
    murfey_db: SQLModelSession,
):
    for tile_id, tile_params in tile_group.items():
        # Tiles get registered in murfey and ispyb as grid squares
        # Attached to Atlas with type "TileScan"
        register_grid_square(session_id, tile_id, tile_params, murfey_db, is_tile=True)
    murfey_db.close()
    return {"success": True}


def run(message: dict, murfey_db: SQLModelSession) -> dict[str, bool]:
    return register_tiles(
        int(message["session_id"]),
        {
            tile_id: GridSquareParameters(**tile_params)
            for tile_id, tile_params in message["tile_group"]
        },
        murfey_db,
    )
