import logging
from pathlib import Path

import numpy as np
from pydantic import BaseModel
from sqlmodel import select
from sqlmodel.orm.session import Session as SQLModelSession
from werkzeug.utils import secure_filename

from murfey.server import _transport_object
from murfey.server.api.auth import MurfeySessionIDInstrument as MurfeySessionID
from murfey.util import sanitise
from murfey.util.config import get_machine_config
from murfey.util.db import (
    AutoProcProgram,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    SearchMap,
    Session,
    TiltSeries,
)

logger = logging.getLogger("murfey.workflows.sxt.process_sxt_tilt_series")


class SXTTiltSeriesInfo(BaseModel):
    tag: str
    source: str
    txrm: str
    tilt_series_length: int
    pixel_size: float
    tilt_offset: int
    xrm_reference: str | None
    x_stage_position: float | None = None
    y_stage_position: float | None = None


def process_sxt_tilt_series(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_info: SXTTiltSeriesInfo,
    murfey_db: SQLModelSession,
) -> dict[str, bool]:
    tilt_series_query = murfey_db.exec(
        select(TiltSeries)
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.tag == tilt_series_info.tag)
        .where(TiltSeries.rsync_source == tilt_series_info.source)
    ).all()
    if tilt_series_query:
        tilt_series = tilt_series_query[0]
        if tilt_series.processing_requested:
            logger.info(f"Tilt series {tilt_series.tag} has already been processed")
    else:
        tilt_series = TiltSeries(
            session_id=session_id,
            tag=tilt_series_info.tag,
            rsync_source=tilt_series_info.source,
            tilt_series_length=tilt_series_info.tilt_series_length,
            processing_requested=False,
        )
        murfey_db.add(tilt_series)
        murfey_db.commit()

    # Find all processing jobs registered for this tilt series
    collected_ids = murfey_db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == tilt_series.rsync_source)
        .where(DataCollection.tag == tilt_series.tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
    ).all()
    if len(collected_ids) == 0:
        logger.warning(f"No processing recipes found for {tilt_series.tag}")
        return {"success": False, "requeue": False}

    instrument_name = (
        murfey_db.exec(select(Session).where(Session.id == session_id))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]

    # Determine pixel location on a roi for display
    min_distance: float | None = None
    matching_roi: SearchMap | None = None
    x_pixel_location: int | None = None
    y_pixel_location: int | None = None
    if tilt_series_info.x_stage_position and tilt_series_info.y_stage_position:
        # Find all rois for this grid
        dcg_rois = murfey_db.exec(
            select(SearchMap)
            .where(SearchMap.session_id == session_id)
            .where(SearchMap.tag == tilt_series.rsync_source)
        ).all()
        for roi in dcg_rois:
            if roi.x_stage_position is not None and roi.y_stage_position is not None:
                # Determine the roi which is closest to this tomogram
                roi_distance = np.sqrt(
                    (tilt_series_info.x_stage_position - roi.x_stage_position) ** 2
                    + (tilt_series_info.y_stage_position - roi.y_stage_position) ** 2
                )
                if min_distance is None or roi_distance < min_distance:
                    min_distance = roi_distance
                    matching_roi = roi

        # Calculate the position on the roi
        if (
            matching_roi is not None
            and matching_roi.x_stage_position is not None
            and matching_roi.y_stage_position is not None
            and matching_roi.height
            and matching_roi.width
            and matching_roi.pixel_size
        ):
            # Convert from stage position to pixel locations
            x_location_centered = (
                (tilt_series_info.x_stage_position - matching_roi.x_stage_position)
                / matching_roi.pixel_size
                / 1e6
            )
            y_location_centered = (
                (tilt_series_info.y_stage_position - matching_roi.y_stage_position)
                / matching_roi.pixel_size
                / 1e6
            )

            # Scaling from different pixel size of atlas and roi, and atlas thumbnail size
            x_pixel_location = int(
                x_location_centered * 1024 / matching_roi.width + 512
            )
            y_pixel_location = int(
                512 - y_location_centered * 1024 / matching_roi.height
            )
        else:
            logger.warning(
                f"Cannot match ROI {matching_roi.id if matching_roi else None} for {tilt_series_info.tag}"
            )

    # Find the visit folder and any subfolders needed
    parts = [secure_filename(p) for p in Path(tilt_series_info.txrm).parts]
    visit_idx = parts.index(visit_name)
    core = Path(*Path(tilt_series_info.txrm).parts[: visit_idx + 1])
    ppath = Path(
        "/".join(secure_filename(p) for p in Path(tilt_series_info.txrm).parts)
    )
    sub_dataset = "/".join(ppath.relative_to(core).parts[1:-1])

    # Loop over all processing jobs, and send the alignment recipe for it
    for recipe_ids in collected_ids:
        # Stack file path needs to contain both recipe name and tilt series name
        stack_file = (
            core
            / machine_config.processed_directory_name
            / machine_config.processed_extra_directory
            / sub_dataset
            / tilt_series.tag
            / recipe_ids[2].recipe
            / "Tomograms"
            / f"{tilt_series.tag}_stack.mrc"
        )
        stack_file.parent.mkdir(parents=True, exist_ok=True)

        # Send message to rabbitmq
        zocalo_message = {
            "recipes": [recipe_ids[2].recipe],
            "parameters": {
                "txrm_file": tilt_series_info.txrm,
                "xrm_reference": tilt_series_info.xrm_reference or "",
                "dcid": recipe_ids[1].id,
                "appid": recipe_ids[3].id,
                "stack_file": str(stack_file),
                "tilt_axis": 0,
                "pixel_size": tilt_series_info.pixel_size,
                "manual_tilt_offset": -tilt_series_info.tilt_offset,
                "node_creator_queue": machine_config.node_creator_queue,
                "search_map_id": matching_roi.id if matching_roi else None,
                "x_location": x_pixel_location,
                "y_location": y_pixel_location,
            },
        }
        if _transport_object:
            logger.info(
                f"Sending Zocalo message for processing: {sanitise(str(zocalo_message))}"
            )
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        else:
            logger.info(
                f"No transport object found. Zocalo message would be {sanitise(str(zocalo_message))}"
            )
    tilt_series.processing_requested = True
    murfey_db.add(tilt_series)
    murfey_db.commit()
    return {"success": True}


def run(message: dict, murfey_db: SQLModelSession) -> dict[str, bool]:
    return process_sxt_tilt_series(
        message["visit_name"],
        message["session_id"],
        SXTTiltSeriesInfo(**message["tilt_series_info"]),
        murfey_db,
    )
