import logging
from pathlib import Path

from PIL import Image
from sqlalchemy import desc
from sqlmodel import Session as SQLModelSession, select

try:
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_common.schemas import (
        GridSquareData as SmartEMGridSquareData,
        GridSquareMetadata as SmartEMGridSquareMetadata,
    )

    from murfey.util.config import get_smartem_keycloak_client

    if keycloak_client := get_smartem_keycloak_client():
        SMARTEM_ACTIVE = True
    else:
        SMARTEM_ACTIVE = False
except ImportError:
    keycloak_client = None
    SMARTEM_ACTIVE = False

import murfey.server
from murfey.util import secure_path
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, GridSquare, Session as MurfeySession
from murfey.util.models import GridSquareParameters

logger = logging.getLogger("murfey.workflows.spa.register_grid_square")


def register_grid_square(
    session_id: int,
    gsid: int,
    grid_square_params: GridSquareParameters,
    murfey_db: SQLModelSession,
):
    # Calculate scaled down version of the image for registration to ISPyB first
    if grid_square_params.x_location is not None:
        grid_square_params.x_location_scaled = int(grid_square_params.x_location / 7.8)
    if grid_square_params.y_location is not None:
        grid_square_params.y_location_scaled = int(grid_square_params.y_location / 7.8)
    if grid_square_params.height is not None:
        grid_square_params.height_scaled = int(grid_square_params.height / 7.8)
    if grid_square_params.width is not None:
        grid_square_params.width_scaled = int(grid_square_params.width / 7.8)

    if grid_square_params.sample is not None:
        dcg = murfey_db.exec(
            select(DataCollectionGroup)
            .where(DataCollectionGroup.session_id == session_id)
            .where(DataCollectionGroup.sample == grid_square_params.sample)
            .order_by(desc(DataCollectionGroup.id))
        ).first()
    else:
        dcg = murfey_db.exec(
            select(DataCollectionGroup)
            .where(DataCollectionGroup.session_id == session_id)
            .where(DataCollectionGroup.tag == grid_square_params.tag)
            .order_by(desc(DataCollectionGroup.id))
        ).first()
    grid_square_query = murfey_db.exec(
        select(GridSquare)
        .where(GridSquare.name == gsid)
        .where(GridSquare.tag == dcg.tag)
        .where(GridSquare.session_id == session_id)
    ).all()
    if grid_square_query:
        # Grid square already exists in the murfey database
        grid_square = grid_square_query[0]
        grid_square.x_location = grid_square_params.x_location or grid_square.x_location
        grid_square.y_location = grid_square_params.y_location or grid_square.y_location
        grid_square.x_stage_position = (
            grid_square_params.x_stage_position or grid_square.x_stage_position
        )
        grid_square.y_stage_position = (
            grid_square_params.y_stage_position or grid_square.y_stage_position
        )
        grid_square.readout_area_x = (
            grid_square_params.readout_area_x or grid_square.readout_area_x
        )
        grid_square.readout_area_y = (
            grid_square_params.readout_area_y or grid_square.readout_area_y
        )
        grid_square.thumbnail_size_x = (
            grid_square_params.thumbnail_size_x or grid_square.thumbnail_size_x
        )
        grid_square.thumbnail_size_y = (
            grid_square_params.thumbnail_size_y or grid_square.thumbnail_size_y
        )
        grid_square.pixel_size = grid_square_params.pixel_size or grid_square.pixel_size
        grid_square.image = grid_square_params.image or grid_square.image
        if murfey.server._transport_object:
            murfey.server._transport_object.do_update_grid_square(
                grid_square.id, grid_square_params
            )
    else:
        # No existing grid square in the murfey database
        if murfey.server._transport_object:
            dcg = murfey_db.exec(
                select(DataCollectionGroup)
                .where(DataCollectionGroup.session_id == session_id)
                .where(DataCollectionGroup.tag == grid_square_params.tag)
            ).one()
            gs_ispyb_response = murfey.server._transport_object.do_insert_grid_square(
                dcg.atlas_id, gsid, grid_square_params
            )
        else:
            # mock up response so that below still works
            gs_ispyb_response = {"success": False, "return_value": None}
        secured_grid_square_image_path = secure_path(Path(grid_square_params.image))
        if secured_grid_square_image_path and secured_grid_square_image_path.is_file():
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
            image=str(secured_grid_square_image_path),
        )
    murfey_db.add(grid_square)
    murfey_db.commit()

    if SMARTEM_ACTIVE:
        try:
            murfey_session = murfey_db.exec(
                select(MurfeySession).where(MurfeySession.id == session_id)
            ).one()
            machine_config = get_machine_config(
                instrument_name=murfey_session.instrument_name
            )[murfey_session.instrument_name]
            if machine_config.smartem_api_url:
                if dcg.smartem_grid_uuid:
                    secured_grid_square_image_path_full_res: Path | None = None
                    if grid_square_params.image:
                        secured_grid_square_image_path_full_res = secure_path(
                            Path(grid_square_params.image)
                        )
                        if secured_grid_square_image_path_full_res.with_suffix(
                            ".tiff"
                        ).is_file():
                            secured_grid_square_image_path_full_res = (
                                secured_grid_square_image_path_full_res.with_suffix(
                                    ".tiff"
                                )
                            )
                        else:
                            secured_grid_square_image_path_full_res = (
                                secured_grid_square_image_path_full_res.with_suffix(
                                    ".mrc"
                                )
                            )
                    smartem_client = SmartEMAPIClient(
                        base_url=machine_config.smartem_api_url,
                        logger=logger,
                        keycloak_client=keycloak_client,
                    )
                    gs_data = SmartEMGridSquareData(
                        gridsquare_id=str(gsid),
                        grid_uuid=dcg.smartem_grid_uuid,
                        center_x=(
                            int(grid_square_params.x_location)
                            if grid_square_params.x_location is not None
                            else None
                        ),
                        center_y=(
                            int(grid_square_params.y_location)
                            if grid_square_params.y_location is not None
                            else None
                        ),
                        size_width=grid_square_params.width,
                        size_height=grid_square_params.height,
                        **(
                            {"uuid": grid_square.smartem_uuid}
                            if grid_square.smartem_uuid
                            else {}
                        ),
                        metadata=SmartEMGridSquareMetadata(
                            atlas_node_id=0,
                            stage_position=None,
                            state=None,
                            rotation=None,
                            image_path=secured_grid_square_image_path_full_res,
                            selected=False,
                            unusable=False,
                        ),
                    )
                    if grid_square.smartem_uuid:
                        smartem_client.update_gridsquare(gs_data)
                    else:
                        response = smartem_client.create_grid_gridsquare(gs_data)
                        grid_square.smartem_uuid = response.uuid
                        murfey_db.add(grid_square)
                        murfey_db.commit()
        except Exception:
            logger.warning("Failed to register grid square with smartem", exc_info=True)

    murfey_db.close()
    return {"success": True}


def run(message: dict, murfey_db: SQLModelSession) -> dict[str, bool]:
    return register_grid_square(
        message["session_id"],
        message["gsid"],
        GridSquareParameters(**message["grid_square_params"]),
        murfey_db,
    )
