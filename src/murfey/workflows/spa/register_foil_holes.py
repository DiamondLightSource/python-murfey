import logging
from pathlib import Path

from PIL import Image
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session as SQLModelSession, select

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_common.schemas import FoilHoleData as SmartEMFoilHoleData

    from murfey.util.config import get_smartem_keycloak_client

    if keycloak_client := get_smartem_keycloak_client():
        SMARTEM_ACTIVE = True
    else:
        SMARTEM_ACTIVE = False
except ImportError:
    keycloak_client = None
    SMARTEM_ACTIVE = False

import murfey.server
from murfey.util import sanitise, secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import FoilHole, GridSquare, Session as MurfeySession
from murfey.util.models import FoilHoleParameters

logger = logging.getLogger(__name__)


def handle_smartem_inserts(
    session_id: int,
    gs: GridSquare,
    foil_hole_uuids: dict[int, str | None] | None,
    foil_hole_parameters: list[FoilHoleParameters],
    murfey_db: SQLModelSession,
) -> dict[int, str | None]:
    """
    Insert or update foil holes in smartem database
    Do this before ispyb as the data gets mutated by the ispyb insert function
    """
    if SMARTEM_ACTIVE and gs.smartem_uuid:
        murfey_session = murfey_db.exec(
            select(MurfeySession).where(MurfeySession.id == session_id)
        ).one()
        machine_config = get_machine_config(
            instrument_name=murfey_session.instrument_name
        )[murfey_session.instrument_name]
        if machine_config.smartem_api_url:
            try:
                smartem_client = SmartEMAPIClient(
                    base_url=machine_config.smartem_api_url,
                    logger=logger,
                    keycloak_client=keycloak_client,
                )
            except Exception:
                return {}

            fh_data = []
            for fhp in foil_hole_parameters:
                fh_data.append(
                    SmartEMFoilHoleData(
                        id=str(fhp.name),
                        gridsquare_id=str(gs.name),
                        gridsquare_uuid=gs.smartem_uuid,
                        x_location=(
                            int(fhp.x_location) if fhp.x_location is not None else None
                        ),
                        y_location=(
                            int(fhp.y_location) if fhp.y_location is not None else None
                        ),
                        x_stage_position=fhp.x_stage_position,
                        y_stage_position=fhp.y_stage_position,
                        diameter=(
                            int(fhp.diameter) if fhp.diameter is not None else None
                        ),
                        **(
                            {"uuid": foil_hole_uuids[fhp.name]}
                            if foil_hole_uuids and foil_hole_uuids.get(fhp.name)
                            else {}
                        ),
                    )
                )
            try:
                if foil_hole_uuids:
                    for fhd in fh_data:
                        # Currently no update all for smartem so do one at a time
                        smartem_client.update_foilholes(fhd)
                    return foil_hole_uuids
                else:
                    # Post all new foil holes to smartem
                    responses = smartem_client.create_gridsquare_foilholes(
                        gs.smartem_uuid, fh_data
                    )
                    if responses:
                        return {int(resp.id): resp.uuid for resp in responses}
            except Exception:
                logger.warning(
                    "Failed to register foil hole with smartem", exc_info=True
                )
    return {}


def handle_existing_holes(
    session_id: int,
    gs: GridSquare,
    foil_hole_rows: list[FoilHole],
    foil_hole_params: list[FoilHoleParameters],
    murfey_db: SQLModelSession,
) -> list[int | None]:
    """Update holes which exist in the murfey database"""
    # Do smartem inserts
    existing_smartem_uuids = {fh.name: fh.smartem_uuid for fh in foil_hole_rows}
    fh_smartem_response = handle_smartem_inserts(
        session_id=session_id,
        gs=gs,
        foil_hole_uuids=existing_smartem_uuids,
        foil_hole_parameters=foil_hole_params,
        murfey_db=murfey_db,
    )

    # Find size of images
    secured_foil_hole_image_path = secure_path(Path(foil_hole_params[0].image))
    if foil_hole_params[0].image and secured_foil_hole_image_path.is_file():
        jpeg_size = Image.open(secured_foil_hole_image_path).size
    else:
        jpeg_size = (0, 0)

    foil_holes: list[FoilHole] = []
    foil_hole_ids: list[int | None] = []
    for fhp in foil_hole_params:
        # Foil hole already exists in the murfey database
        foil_hole = [fh for fh in foil_hole_rows if fh.name == fhp.name][0]
        foil_hole_ids.append(foil_hole.id)
        foil_hole.x_location = fhp.x_location or foil_hole.x_location
        foil_hole.y_location = fhp.y_location or foil_hole.y_location
        foil_hole.x_stage_position = fhp.x_stage_position or foil_hole.x_stage_position
        foil_hole.y_stage_position = fhp.y_stage_position or foil_hole.y_stage_position
        foil_hole.readout_area_x = fhp.readout_area_x or foil_hole.readout_area_x
        foil_hole.readout_area_y = fhp.readout_area_y or foil_hole.readout_area_y
        foil_hole.thumbnail_size_x = (
            fhp.thumbnail_size_x or foil_hole.thumbnail_size_x
        ) or jpeg_size[0]
        foil_hole.thumbnail_size_y = (
            fhp.thumbnail_size_y or foil_hole.thumbnail_size_y
        ) or jpeg_size[1]
        foil_hole.pixel_size = fhp.pixel_size or foil_hole.pixel_size
        foil_hole.smartem_uuid = fh_smartem_response.get(fhp.name)
        foil_holes.append(foil_hole)

    if (
        murfey.server._transport_object
        and gs.id
        and gs.thumbnail_size_x
        and gs.readout_area_x
        and None not in foil_hole_ids
    ):
        int_foil_holes_ids: list[int] = [
            fhid for fhid in foil_hole_ids if fhid is not None
        ]
        murfey.server._transport_object.do_update_foil_holes(
            gs.id,
            gs.thumbnail_size_x / gs.readout_area_x,
            int_foil_holes_ids,
            foil_hole_params,
        )

    # Complete murfey database registration
    for fh in foil_holes:
        murfey_db.add(fh)
    murfey_db.commit()
    return [fh.id for fh in foil_holes]


def handle_new_holes(
    session_id: int,
    gs: GridSquare,
    foil_hole_params: list[FoilHoleParameters],
    murfey_db: SQLModelSession,
) -> list[int | None]:
    """Insert holes which do not exist in the murfey database"""
    # Do smartem inserts
    fh_smartem_response = handle_smartem_inserts(
        session_id=session_id,
        gs=gs,
        foil_hole_uuids=None,
        foil_hole_parameters=foil_hole_params,
        murfey_db=murfey_db,
    )

    # Find size of images
    secured_foil_hole_image_path = secure_path(Path(foil_hole_params[0].image))
    if foil_hole_params[0].image and secured_foil_hole_image_path.is_file():
        jpeg_size = Image.open(secured_foil_hole_image_path).size
    else:
        jpeg_size = (0, 0)

    foil_holes: list[FoilHole] = []
    if murfey.server._transport_object and gs.id is not None:
        fh_ispyb_response = murfey.server._transport_object.do_insert_foil_holes(
            gs.id,
            gs.thumbnail_size_x / gs.readout_area_x
            if gs.thumbnail_size_x and gs.readout_area_x
            else None,
            foil_hole_params,
        )
    else:
        fh_ispyb_response = {"success": False, "return_value": None}
    for fhp in foil_hole_params:
        foil_holes.append(
            FoilHole(
                id=(
                    fh_ispyb_response["return_value"][fhp.name]
                    if fh_ispyb_response["success"]
                    and type(fh_ispyb_response["return_value"]) is dict
                    else None
                ),
                name=fhp.name,
                session_id=session_id,
                grid_square_id=gs.id,
                x_location=fhp.x_location,
                y_location=fhp.y_location,
                x_stage_position=fhp.x_stage_position,
                y_stage_position=fhp.y_stage_position,
                readout_area_x=fhp.readout_area_x,
                readout_area_y=fhp.readout_area_y,
                thumbnail_size_x=fhp.thumbnail_size_x or jpeg_size[0],
                thumbnail_size_y=fhp.thumbnail_size_y or jpeg_size[1],
                pixel_size=fhp.pixel_size,
                image=str(secure_path(Path(fhp.image))),
                smartem_uuid=fh_smartem_response.get(fhp.name),
            )
        )

    # Complete murfey database registration
    for fh in foil_holes:
        murfey_db.add(fh)
    murfey_db.commit()
    return [fh.id for fh in foil_holes]


def register_foil_holes(
    session_id: int,
    gs_name: int,
    foil_hole_group: dict[int, FoilHoleParameters],
    murfey_db: SQLModelSession,
) -> list[int | None] | None:
    """Register a list of foil holes on a grid square"""
    if not foil_hole_group:
        return None
    try:
        gs = murfey_db.exec(
            select(GridSquare)
            .where(GridSquare.tag == list(foil_hole_group.values())[0].tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        ).one()
    except NoResultFound:
        logger.warning(
            f"Foil holes could not be registered as grid square {sanitise(str(gs_name))} was not found"
        )
        return None

    # Check which holes already exist
    foil_hole_query = murfey_db.exec(
        select(FoilHole)
        .where(FoilHole.grid_square_id == gs.id)
        .where(FoilHole.session_id == session_id)
    ).all()
    existing_hole_names = [fh.name for fh in foil_hole_query]

    hole_ids = []
    # Existing holes can be updated in all databases
    existing_holes = [
        fhp for fhid, fhp in foil_hole_group.items() if fhid in existing_hole_names
    ]
    if existing_holes:
        hole_ids.extend(
            handle_existing_holes(
                session_id, gs, foil_hole_query, existing_holes, murfey_db
            )
        )
    # New holes need to be inserted
    new_holes = [
        fhp for fhid, fhp in foil_hole_group.items() if fhid not in existing_hole_names
    ]
    if new_holes:
        hole_ids.extend(handle_new_holes(session_id, gs, new_holes, murfey_db))
    murfey_db.close()
    return hole_ids


def run(message: dict, murfey_db: SQLModelSession) -> list[int | None] | None:
    return register_foil_holes(
        int(message["session_id"]),
        message["gs_name"],
        {fhid: FoilHoleParameters(**fhp) for fhid, fhp in message["foil_hole_group"]},
        murfey_db,
    )
