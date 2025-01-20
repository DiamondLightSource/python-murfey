import logging
from pathlib import Path

from PIL import Image
from sqlalchemy.exc import NoResultFound
from sqlmodel import select
from werkzeug.utils import secure_filename

from murfey.server import _transport_object, sanitise
from murfey.server.api.auth import MurfeySessionID
from murfey.server.murfey_db import murfey_db
from murfey.util.db import DataCollectionGroup, FoilHole, GridSquare
from murfey.util.models import FoilHoleParameters, GridSquareParameters

log = logging.getLogger("murfey.server.spa.spa_metadata")


def register_grid_square(
    session_id: MurfeySessionID,
    gsid: int,
    grid_square_params: GridSquareParameters,
    db=murfey_db,
):
    try:
        grid_square = db.exec(
            select(GridSquare)
            .where(GridSquare.name == gsid)
            .where(GridSquare.tag == grid_square_params.tag)
            .where(GridSquare.session_id == session_id)
        ).one()
        grid_square.x_location = grid_square_params.x_location
        grid_square.y_location = grid_square_params.y_location
        grid_square.x_stage_position = grid_square_params.x_stage_position
        grid_square.y_stage_position = grid_square_params.y_stage_position
        if _transport_object:
            _transport_object.do_update_grid_square(grid_square.id, grid_square_params)
    except Exception:
        if _transport_object:
            dcg = db.exec(
                select(DataCollectionGroup)
                .where(DataCollectionGroup.session_id == session_id)
                .where(DataCollectionGroup.tag == grid_square_params.tag)
            ).one()
            gs_ispyb_response = _transport_object.do_insert_grid_square(
                dcg.atlas_id, gsid, grid_square_params
            )
        else:
            # mock up response so that below still works
            gs_ispyb_response = {"success": False, "return_value": None}
        secured_grid_square_image_path = secure_filename(grid_square_params.image)
        if (
            secured_grid_square_image_path
            and Path(secured_grid_square_image_path).is_file()
        ):
            jpeg_size = Image.open(secured_grid_square_image_path).size
        else:
            jpeg_size = (0, 0)
        grid_square = GridSquare(
            id=(
                gs_ispyb_response["return_value"]
                if gs_ispyb_response["success"]
                else None
            ),
            name=gsid,
            session_id=session_id,
            tag=grid_square_params.tag,
            x_location=grid_square_params.x_location,
            y_location=grid_square_params.y_location,
            x_stage_position=grid_square_params.x_stage_position,
            y_stage_position=grid_square_params.y_stage_position,
            readout_area_x=grid_square_params.readout_area_x,
            readout_area_y=grid_square_params.readout_area_y,
            thumbnail_size_x=grid_square_params.thumbnail_size_x or jpeg_size[0],
            thumbnail_size_y=grid_square_params.thumbnail_size_y or jpeg_size[1],
            pixel_size=grid_square_params.pixel_size,
            image=secured_grid_square_image_path,
        )
    db.add(grid_square)
    db.commit()
    db.close()


def register_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    db=murfey_db,
):
    try:
        gs = db.exec(
            select(GridSquare)
            .where(GridSquare.tag == foil_hole_params.tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        ).one()
        gsid = gs.id
    except NoResultFound:
        log.debug(
            f"Foil hole {sanitise(str(foil_hole_params.name))} could not be registered as grid square {sanitise(str(gs_name))} was not found"
        )
        return
    secured_foil_hole_image_path = secure_filename(foil_hole_params.image)
    if foil_hole_params.image and Path(secured_foil_hole_image_path).is_file():
        jpeg_size = Image.open(secured_foil_hole_image_path).size
    else:
        jpeg_size = (0, 0)
    try:
        foil_hole = db.exec(
            select(FoilHole)
            .where(FoilHole.name == foil_hole_params.name)
            .where(FoilHole.grid_square_id == gsid)
            .where(FoilHole.session_id == session_id)
        ).one()
        foil_hole.x_location = foil_hole_params.x_location
        foil_hole.y_location = foil_hole_params.y_location
        foil_hole.x_stage_position = foil_hole_params.x_stage_position
        foil_hole.y_stage_position = foil_hole_params.y_stage_position
        foil_hole.readout_area_x = foil_hole_params.readout_area_x
        foil_hole.readout_area_y = foil_hole_params.readout_area_y
        foil_hole.thumbnail_size_x = foil_hole_params.thumbnail_size_x or jpeg_size[0]
        foil_hole.thumbnail_size_y = foil_hole_params.thumbnail_size_y or jpeg_size[1]
        foil_hole.pixel_size = foil_hole_params.pixel_size
        if _transport_object:
            _transport_object.do_update_foil_hole(
                foil_hole.id, gs.thumbnail_size_x / gs.readout_area_x, foil_hole_params
            )
    except Exception:
        if _transport_object:
            fh_ispyb_response = _transport_object.do_insert_foil_hole(
                gs.id, gs.thumbnail_size_x / gs.readout_area_x, foil_hole_params
            )
        else:
            fh_ispyb_response = {"success": False, "return_value": None}
        foil_hole = FoilHole(
            id=(
                fh_ispyb_response["return_value"]
                if fh_ispyb_response["success"]
                else None
            ),
            name=foil_hole_params.name,
            session_id=session_id,
            grid_square_id=gsid,
            x_location=foil_hole_params.x_location,
            y_location=foil_hole_params.y_location,
            x_stage_position=foil_hole_params.x_stage_position,
            y_stage_position=foil_hole_params.y_stage_position,
            readout_area_x=foil_hole_params.readout_area_x,
            readout_area_y=foil_hole_params.readout_area_y,
            thumbnail_size_x=foil_hole_params.thumbnail_size_x or jpeg_size[0],
            thumbnail_size_y=foil_hole_params.thumbnail_size_y or jpeg_size[1],
            pixel_size=foil_hole_params.pixel_size,
            image=secured_foil_hole_image_path,
        )
    db.add(foil_hole)
    db.commit()
    db.close()
