from datetime import datetime
from logging import getLogger
from pathlib import Path

import requests
from sqlalchemy import desc
from sqlmodel import Session as SQLModelSession, select

try:
    from smartem_agent.fs_parser import EpuParser
    from smartem_backend.api_client import SmartEMAPIClient
    from smartem_common.schemas import AtlasData, AtlasTileGridSquarePositionData

    from murfey.util.config import get_smartem_keycloak_client

    if keycloak_client := get_smartem_keycloak_client():
        SMARTEM_ACTIVE = True
    else:
        SMARTEM_ACTIVE = False
except ImportError:
    keycloak_client = None
    SMARTEM_ACTIVE = False

from murfey.util import sanitise
from murfey.util.config import get_machine_config
from murfey.util.db import DataCollectionGroup, Session as MurfeySession
from murfey.util.models import AtlasRegistration

logger = getLogger("murfey.workflows.spa.smartem_atlas")


def smartem_atlas(message: dict, murfey_db: SQLModelSession):
    session_id = message.get("session_id")
    atlas_registration_data = AtlasRegistration(message["atlas_registration_data"])
    if SMARTEM_ACTIVE and atlas_registration_data.acquisition_uuid is not None:
        session = murfey_db.exec(
            select(MurfeySession).where(MurfeySession.id == session_id)
        ).one()
        machine_config = get_machine_config(session.instrument_name)[
            session.instrument_name
        ]
        if machine_config.smartem_api_url:
            smartem_client = SmartEMAPIClient(
                base_url=machine_config.smartem_api_url,
                logger=logger,
                keycloak_client=keycloak_client,
            )
            grid_uuid = None
            atlas_path = None
            if atlas_registration_data.tag:
                dcg = murfey_db.exec(
                    select(DataCollectionGroup)
                    .where(DataCollectionGroup.session_id == session_id)
                    .where(DataCollectionGroup.tag == atlas_registration_data.tag)
                ).one_or_none()
                if dcg is None and "Sample" in atlas_registration_data.tag:
                    sample = int(
                        atlas_registration_data.tag.split("Sample")[1].split("/")[0]
                    )
                    dcg = murfey_db.exec(
                        select(DataCollectionGroup)
                        .where(DataCollectionGroup.session_id == session_id)
                        .where(DataCollectionGroup.sample == sample)
                        .order_by(desc(DataCollectionGroup.id))
                    ).first()
                grid_uuid = dcg.smartem_grid_uuid if dcg is not None else None
                atlas_path = Path(dcg.atlas).parent
            else:
                possible_grids = smartem_client.get_acquisition_grids(
                    atlas_registration_data.acquisition_uuid
                )
                for grid in possible_grids:
                    if grid.name == atlas_registration_data.name.replace("_atlas", ""):
                        grid_uuid = grid.uuid
                        atlas_path = Path(grid.atlas_dir).parent
                        break
            logger.info(f"New atlas {grid_uuid} with path {atlas_path}")
            if grid_uuid is not None and atlas_path is not None:
                try:
                    existing_atlas = smartem_client.get_grid_atlas(grid_uuid)
                    if (
                        existing_atlas.name == atlas_registration_data.name
                        and existing_atlas.storage_folder
                        == atlas_registration_data.storage_folder
                    ):
                        # there is a question here of whether the grid should be registered if specified
                        return {"success": True}
                except requests.exceptions.HTTPError:
                    pass
                logger.info(f"Registering new atlas {atlas_registration_data.name}")
                if (Path(atlas_path) / "Atlas.dm").is_file():
                    parser = EpuParser()
                    atlas_data = parser.parse_atlas_manifest(
                        str(atlas_path / "Atlas.dm"), grid_uuid
                    )
                    atlas_data.acquisition_date = atlas_data.acquisition_date.replace(
                        tzinfo=None
                    )  # timezone information is not consistently provided so drop it
                else:
                    atlas_data = AtlasData(
                        id=atlas_registration_data.tag,
                        acquisition_date=datetime.now(),
                        storage_folder=str(atlas_path),
                        name=atlas_registration_data.name,
                        tiles=[],
                        gridsquare_positions=None,
                        grid_uuid=grid_uuid,
                    )
                smartem_client.create_grid_atlas(atlas_data)
                registered_squares = smartem_client.get_grid_gridsquares(grid_uuid)
                gs_uuid_map = {gs.gridsquare_id: gs.uuid for gs in registered_squares}
                for atlastile in atlas_data.tiles:
                    pos_data_for_tile = []
                    for gsid, gs_tile_pos in atlastile.gridsquare_positions.items():
                        for pos in gs_tile_pos:
                            pos_data_for_tile.append(
                                AtlasTileGridSquarePositionData(
                                    gridsquare_uuid=gs_uuid_map[gsid],
                                    tile_uuid=atlastile.uuid,
                                    position=pos.position,
                                    size=pos.size,
                                )
                            )
                    smartem_client.link_atlas_tile_and_gridsquares(pos_data_for_tile)
                if atlas_registration_data.register_grid:
                    smartem_client.grid_registered(grid_uuid)
    else:
        logger.info(
            f"smartem deactivated so did not register atlas for {sanitise(str(atlas_registration_data.acquisition_uuid))}"
        )
    return {"success": True}
