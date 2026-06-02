import logging
from pathlib import Path

from PIL import Image
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_common.schemas import FoilHoleData as SmartEMFoilHoleData

    SMARTEM_ACTIVE = True
except ImportError:
    SMARTEM_ACTIVE = False

from murfey.server import _transport_object
from murfey.util import sanitise, secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import (
    FoilHole,
    GridSquare,
    Session as MurfeySession,
)
from murfey.util.models import FoilHoleParameters

logger = logging.getLogger("murfey.workflows.spa.register_foil_holes")


def register_holes_on_grid(
    session_id: int,
    gs_name: int,
    foil_hole_group: dict[str, FoilHoleParameters],
    murfey_db: Session,
) -> list[int | None] | None:
    try:
        gs = murfey_db.exec(
            select(GridSquare)
            .where(GridSquare.tag == next(iter(foil_hole_group.values())).tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        ).one()
        gsid = gs.id
    except NoResultFound:
        logger.warning(
            f"Foil holes could not be registered as grid square {sanitise(str(gs_name))} was not found"
        )
        return None

    fh_ids: list[int | None] = []
    for fh_name, foil_hole_params in foil_hole_group.items():
        secured_foil_hole_image_path = secure_path(Path(foil_hole_params.image))
        if foil_hole_params.image and secured_foil_hole_image_path.is_file():
            jpeg_size = Image.open(secured_foil_hole_image_path).size
        else:
            jpeg_size = (0, 0)
        foil_hole_query = murfey_db.exec(
            select(FoilHole)
            .where(FoilHole.name == foil_hole_params.name)
            .where(FoilHole.grid_square_id == gsid)
            .where(FoilHole.session_id == session_id)
        ).all()
        if foil_hole_query:
            # Foil hole already exists in the murfey database
            foil_hole = foil_hole_query[0]
            foil_hole.x_location = foil_hole_params.x_location or foil_hole.x_location
            foil_hole.y_location = foil_hole_params.y_location or foil_hole.y_location
            foil_hole.x_stage_position = (
                foil_hole_params.x_stage_position or foil_hole.x_stage_position
            )
            foil_hole.y_stage_position = (
                foil_hole_params.y_stage_position or foil_hole.y_stage_position
            )
            foil_hole.readout_area_x = (
                foil_hole_params.readout_area_x or foil_hole.readout_area_x
            )
            foil_hole.readout_area_y = (
                foil_hole_params.readout_area_y or foil_hole.readout_area_y
            )
            foil_hole.thumbnail_size_x = (
                foil_hole_params.thumbnail_size_x or foil_hole.thumbnail_size_x
            ) or jpeg_size[0]
            foil_hole.thumbnail_size_y = (
                foil_hole_params.thumbnail_size_y or foil_hole.thumbnail_size_y
            ) or jpeg_size[1]
            foil_hole.pixel_size = foil_hole_params.pixel_size or foil_hole.pixel_size
            if _transport_object and gs.readout_area_x:
                _transport_object.do_update_foil_hole(
                    foil_hole.id,
                    gs.thumbnail_size_x / gs.readout_area_x,
                    foil_hole_params,
                )
        else:
            # No existing foil hole in the murfey database
            if _transport_object:
                fh_ispyb_response = _transport_object.do_insert_foil_hole(
                    gs.id,
                    gs.thumbnail_size_x / gs.readout_area_x
                    if gs.readout_area_x
                    else None,
                    foil_hole_params,
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
                image=str(secured_foil_hole_image_path),
            )
        fh_ids.append(foil_hole.id)
        murfey_db.add(foil_hole)

        if SMARTEM_ACTIVE and gs.smartem_uuid:
            try:
                murfey_session = murfey_db.exec(
                    select(MurfeySession).where(MurfeySession.id == session_id)
                ).one()
                machine_config = get_machine_config(
                    instrument_name=murfey_session.instrument_name
                )[murfey_session.instrument_name]
                if machine_config.smartem_api_url:
                    smartem_client = SmartEMAPIClient(
                        base_url=machine_config.smartem_api_url, logger=logger
                    )
                    fh_data = SmartEMFoilHoleData(
                        id=str(foil_hole_params.name),
                        gridsquare_id=str(gs.name),
                        gridsquare_uuid=gs.smartem_uuid,
                        x_location=(
                            int(foil_hole_params.x_location)
                            if foil_hole_params.x_location is not None
                            else None
                        ),
                        y_location=(
                            int(foil_hole_params.y_location)
                            if foil_hole_params.y_location is not None
                            else None
                        ),
                        x_stage_position=foil_hole_params.x_stage_position,
                        y_stage_position=foil_hole_params.y_stage_position,
                        diameter=(
                            int(foil_hole_params.diameter)
                            if foil_hole_params.diameter is not None
                            else None
                        ),
                        **(
                            {"uuid": foil_hole.smartem_uuid}
                            if foil_hole.smartem_uuid
                            else {}
                        ),
                    )
                    if foil_hole.smartem_uuid:
                        smartem_client.update_foilhole(fh_data)
                    else:
                        responses = smartem_client.create_gridsquare_foilholes(
                            gs.smartem_uuid, [fh_data]
                        )
                        if responses:
                            foil_hole.smartem_uuid = responses[0].uuid
                            murfey_db.add(foil_hole)
            except Exception:
                logger.warning(
                    f"Failed to register foil hole {foil_hole.id} with smartem",
                    exc_info=True,
                )

    murfey_db.commit()
    murfey_db.close()
    return fh_ids


def register_foil_holes(message: dict, murfey_db: Session) -> dict[str, bool]:
    session_id = message["session_id"]
    gs_name = message["gs_name"]
    foil_hole_group = message["foil_hole_group"]
    fh_ids = register_holes_on_grid(session_id, gs_name, foil_hole_group, murfey_db)
    if fh_ids is None:
        logger.warning(f"Failed to register foil holes on grid square {gs_name}")
        return {"success": False, "requeue": False}
    return {"success": True}
