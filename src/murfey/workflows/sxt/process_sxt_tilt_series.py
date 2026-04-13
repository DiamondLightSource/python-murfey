import logging
from pathlib import Path

from pydantic import BaseModel
from sqlmodel import select
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


def process_sxt_tilt_series_workflow(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_info: SXTTiltSeriesInfo,
    murfey_db: Session,
):
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
            return
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
    instrument_name = (
        murfey_db.exec(select(Session).where(Session.id == session_id))
        .one()
        .instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]

    parts = [secure_filename(p) for p in Path(tilt_series_info.txrm).parts]
    visit_idx = parts.index(visit_name)
    core = Path(*Path(tilt_series_info.txrm).parts[: visit_idx + 1])
    ppath = Path(
        "/".join(secure_filename(p) for p in Path(tilt_series_info.txrm).parts)
    )
    sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
    extra_path = machine_config.processed_extra_directory
    stack_file = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / extra_path
        / "Tomograms"
        / f"{tilt_series.tag}_stack.mrc"
    )
    stack_file.parent.mkdir(parents=True, exist_ok=True)

    for recipe_ids in collected_ids:
        # Loop over all processing jobs, and send the alignment recipe for it
        zocalo_message = {
            "recipes": recipe_ids[2].recipe,
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
