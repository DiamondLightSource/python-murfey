import logging

from sqlmodel.orm.session import Session as SQLModelSession, select

from murfey.server import _transport_object
from murfey.util import sanitise
from murfey.util.db import DataCollectionGroup, ImagingSite, SearchMap
from murfey.util.models import SearchMapParameters

logger = logging.getLogger("murfey.workflows.sxt.sxt_metadata")


def register_sxt_roi(
    session_id: int,
    roi_name: str,
    roi_parameters: SearchMapParameters,
    murfey_db: SQLModelSession,
) -> dict[str, bool]:
    dcg = murfey_db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == roi_parameters.tag)
    ).one()
    roi_query = murfey_db.exec(
        select(SearchMap)
        .where(SearchMap.name == roi_name)
        .where(SearchMap.tag == roi_parameters.tag)
        .where(SearchMap.session_id == SearchMap)
    ).all()
    if roi_query:
        # See if there is already a search map with this name and update if so
        roi = roi_query[0]
        roi.x_stage_position = roi_parameters.x_stage_position or roi.x_stage_position
        roi.y_stage_position = roi_parameters.y_stage_position or roi.y_stage_position
        roi.height = roi_parameters.height or roi.height
        roi.width = roi_parameters.width or roi.width
        roi.pixel_size = roi_parameters.pixel_size or roi.pixel_size
        roi.image = roi_parameters.image or roi.image
        if _transport_object:
            _transport_object.do_update_sxt_roi(roi.id, roi_parameters)
    else:
        logger.info(f"Registering new sxt roi {sanitise(roi_name)}")
        if _transport_object:
            roi_ispyb_response = _transport_object.do_insert_sxt_roi(
                dcg.atlas_id, roi_parameters
            )
        else:
            # mock up response so that below still works
            roi_ispyb_response = {"success": False, "return_value": None}
        # Register new search map
        roi = SearchMap(
            id=(
                roi_ispyb_response["return_value"]
                if roi_ispyb_response["success"]
                else None
            ),
            name=roi_name,
            session_id=session_id,
            tag=roi_parameters.tag,
            x_stage_position=roi_parameters.x_stage_position,
            y_stage_position=roi_parameters.y_stage_position,
            pixel_size=roi_parameters.pixel_size,
            width=roi_parameters.width,
            height=roi_parameters.height,
            image=roi_parameters.image or "",
        )

    atlas_sites = murfey_db.exec(
        select(ImagingSite).where(ImagingSite.dcg_id == dcg.id)
    ).all()

    if all(
        [
            roi.x_stage_position,
            roi.y_stage_position,
            roi.pixel_size,
            atlas_sites,
        ]
    ):
        atlas = atlas_sites[0]
        # Convert from stage position to pixel locations
        roi.x_location = (roi.x_stage_position - atlas.pos_x) / atlas.image_pixel_size
        roi.y_location = (roi.y_stage_position - atlas.pox_y) / atlas.image_pixel_size

        # Scaling from different pixel size of atlas and roi, and atlas thumbnail size
        roi_parameters.x_location = roi.x_location
        roi_parameters.y_location = roi.y_location
        roi_parameters.width_on_atlas = (
            roi.width
            * (roi.pixel_size / atlas.image_pixel_size)
            * (1024 / atlas.image_pixels_x)
        )
        roi_parameters.height_on_atlas = (
            roi.height
            * (roi.pixel_size / atlas.image_pixel_size)
            * (1024 / atlas.image_pixels_y)
        )
        if _transport_object:
            _transport_object.do_update_sxt_roi(roi.id, roi_parameters)
    else:
        logger.info(
            f"Unable to register roi {sanitise(roi.name)} position yet: "
            f"roi pixel size {sanitise(str(roi.pixel_size))}, "
            f"atlas pixel size {sanitise(str(dcg.atlas_pixel_size))}"
        )
    murfey_db.add(roi)
    murfey_db.commit()
    murfey_db.close()
    return {"success": True}


def run(message: dict, murfey_db: SQLModelSession) -> dict[str, bool]:
    return register_sxt_roi(
        message["session_id"],
        message["roi_name"],
        SearchMapParameters(**message["roi_info"]),
        murfey_db,
    )
