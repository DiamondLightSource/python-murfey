import logging

from sqlmodel.orm.session import Session as SQLModelSession, select

from murfey.server import _transport_object
from murfey.util import sanitise
from murfey.util.db import (
    DataCollectionGroup,
    SxtRoi,
)
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
        select(SxtRoi)
        .where(SxtRoi.name == roi_name)
        .where(SxtRoi.tag == roi_parameters.tag)
        .where(SxtRoi.session_id == SxtRoi)
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
        roi = SxtRoi(
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
            thumbnail_size_x=1024,
            thumbnail_size_y=1024,
            image=roi_parameters.image or "",
        )

    if all(
        [
            roi.x_stage_position,
            roi.y_stage_position,
            roi.pixel_size,
            dcg.atlas_pixel_size,
            dcg.atlas_stage_x,
            dcg.atlas_height,
        ]
    ):
        # Convert from stage position to pixel locations
        roi.x_location = (
            roi.x_stage_position - dcg.atlas_stage_x
        ) / dcg.atlas_pixel_size
        roi.y_location = (
            roi.y_stage_position - dcg.atlas_stage_y
        ) / dcg.atlas_pixel_size

        # Scaling from different pixel size of atlas and roi, and atlas thumbnailing
        roi_parameters.x_location = roi.x_location
        roi_parameters.y_location = roi.y_location
        roi_parameters.height_on_atlas = (
            roi.height
            * (roi.pixel_size / dcg.atlas_pixel_size)
            * (1024 / dcg.atlas_height)
        )
        roi_parameters.width_on_atlas = (
            roi.width
            * (roi.pixel_size / dcg.atlas_pixel_size)
            * (1024 / dcg.atlas_width)
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
