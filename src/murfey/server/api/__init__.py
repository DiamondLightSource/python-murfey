from __future__ import annotations

import asyncio
import datetime
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import sqlalchemy
from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse
from ispyb.sqlalchemy import AutoProcProgram as ISPyBAutoProcProgram
from ispyb.sqlalchemy import (
    BLSample,
    BLSampleGroup,
    BLSampleImage,
    BLSession,
    BLSubSample,
    Proposal,
)
from PIL import Image
from prometheus_client import Counter, Gauge
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from sqlmodel import col, select
from werkzeug.utils import secure_filename

import murfey.server.ispyb
import murfey.server.prometheus as prom
import murfey.server.websocket as ws
from murfey.server import (
    _murfey_id,
    _transport_object,
    check_tilt_series_mc,
    get_all_tilts,
    get_angle,
    get_hostname,
    get_job_ids,
    get_machine_config,
    get_microscope,
    get_tomo_proc_params,
    sanitise,
    templates,
)
from murfey.server.api.auth import MurfeySessionID, validate_token
from murfey.server.api.spa import _cryolo_model_path
from murfey.server.murfey_db import murfey_db
from murfey.util import safe_run
from murfey.util.config import MachineConfig, from_file, settings
from murfey.util.db import (
    AutoProcProgram,
    ClientEnvironment,
    DataCollection,
    DataCollectionGroup,
    FoilHole,
    GridSquare,
    MagnificationLookup,
    Movie,
    PreprocessStash,
    ProcessingJob,
    RsyncInstance,
    Session,
    SessionProcessingParameters,
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)
from murfey.util.models import (
    BLSampleImageParameters,
    BLSampleParameters,
    BLSubSampleParameters,
    ClientInfo,
    CurrentGainRef,
    FoilHoleParameters,
    GridSquareParameters,
    MillingParameters,
    PostInfo,
    ProcessingParametersSPA,
    ProcessingParametersTomo,
    RegistrationMessage,
    RsyncerInfo,
    RsyncerSource,
    Sample,
    SessionInfo,
    SPAProcessFile,
    TiltInfo,
    TiltSeriesGroupInfo,
    TiltSeriesInfo,
    TomoProcessFile,
)
from murfey.util.processing_params import default_spa_parameters
from murfey.util.tomo import midpoint
from murfey.workflows.spa.flush_spa_preprocess import (
    register_foil_hole,
    register_grid_square,
)

log = logging.getLogger("murfey.server.api")

router = APIRouter(dependencies=[Depends(validate_token)])


# This will be the homepage for a given microscope.
@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
            "hostname": get_hostname(),
            "microscope": get_microscope(),
            "version": murfey.__version__,
        },
    )


@router.get("/health/")
def health_check(db=murfey.server.ispyb.DB):
    conn = db.connection()
    conn.close()
    return {
        "ispyb_connection": True,
        "rabbitmq_connection": _transport_object.transport.is_connected(),
    }


@router.get("/connections/")
def connections_check():
    return {"connections": list(ws.manager.active_connections.keys())}


@router.get("/machine")
def machine_info() -> Optional[MachineConfig]:
    instrument_name = os.getenv("BEAMLINE")
    if settings.murfey_machine_configuration and instrument_name:
        return from_file(Path(settings.murfey_machine_configuration), instrument_name)[
            instrument_name
        ]
    return None


@lru_cache(maxsize=5)
@router.get("/instruments/{instrument_name}/machine")
def machine_info_by_name(instrument_name: str) -> Optional[MachineConfig]:
    if settings.murfey_machine_configuration:
        return from_file(Path(settings.murfey_machine_configuration), instrument_name)[
            instrument_name
        ]
    return None


@router.get("/mag_table/")
def get_mag_table(db=murfey_db) -> List[MagnificationLookup]:
    return db.exec(select(MagnificationLookup)).all()


@router.post("/mag_table/")
def add_to_mag_table(rows: List[MagnificationLookup], db=murfey_db):
    for r in rows:
        db.add(r)
    db.commit()


@router.delete("/mag_table/{mag}")
def remove_mag_table_row(mag: int, db=murfey_db):
    row = db.exec(
        select(MagnificationLookup).where(MagnificationLookup.magnification == mag)
    ).one()
    db.delete(row)
    db.commit()


@router.get("/instruments/{instrument_name}/instrument_name")
def get_instrument_display_name(instrument_name: str) -> str:
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    if machine_config:
        return machine_config.display_name
    return ""


@router.get("/instruments/{instrument_name}/visits/")
def all_visit_info(instrument_name: str, request: Request, db=murfey.server.ispyb.DB):
    visits = murfey.server.ispyb.get_all_ongoing_visits(instrument_name, db)

    if visits:
        return_query = [
            {
                "Start date": visit.start,
                "End date": visit.end,
                "Visit name": visit.name,
                "Time remaining": str(visit.end - datetime.datetime.now()),
            }
            for visit in visits
        ]  # "Proposal title": visit.proposal_title
        log.debug(
            f"{len(visits)} visits active for {sanitise(instrument_name)=}: {', '.join(v.name for v in visits)}"
        )
        return templates.TemplateResponse(
            request=request,
            name="activevisits.html",
            context={"info": return_query, "microscope": instrument_name},
        )
    else:
        log.debug(f"No visits identified for {sanitise(instrument_name)=}")
        return templates.TemplateResponse(
            request=request,
            name="activevisits.html",
            context={"info": [], "microscope": instrument_name},
        )


@router.post("/visits/{visit_name}")
def register_client_to_visit(visit_name: str, client_info: ClientInfo, db=murfey_db):
    client_env = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_info.id)
    ).one()
    session = db.exec(select(Session).where(Session.id == client_env.session_id)).one()
    if client_env:
        client_env.visit = visit_name
        db.add(client_env)
        db.commit()
    if session:
        session.visit = visit_name
        db.add(session)
        db.commit()
    db.close()
    return client_info


@router.get("/num_movies")
def count_number_of_movies(db=murfey_db) -> Dict[str, int]:
    res = db.exec(
        select(Movie.tag, func.count(Movie.murfey_id)).group_by(Movie.tag)
    ).all()
    return {r[0]: r[1] for r in res}


@router.post("/sessions/{session_id}/rsyncer")
def register_rsyncer(session_id: int, rsyncer_info: RsyncerInfo, db=murfey_db):
    visit_name = db.exec(select(Session).where(Session.id == session_id)).one().visit
    rsync_instance = RsyncInstance(
        source=rsyncer_info.source,
        session_id=rsyncer_info.session_id,
        transferring=rsyncer_info.transferring,
        destination=rsyncer_info.destination,
        tag=rsyncer_info.tag,
    )
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files.labels(rsync_source=rsyncer_info.source, visit=visit_name)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    )
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(0)
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).set(
        0
    )
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).set(0)
    return rsyncer_info


@router.delete("/sessions/{session_id}/rsyncer")
def delete_rsyncer(session_id: int, source: Path, db=murfey_db):
    try:
        rsync_instance = db.exec(
            select(RsyncInstance)
            .where(RsyncInstance.session_id == session_id)
            .where(RsyncInstance.source == str(source))
        ).one()
        db.delete(rsync_instance)
        db.commit()
    except Exception:
        log.error(
            f"Failed to delete rsyncer for source directory {sanitise(str(source))!r} "
            f"in session {session_id}.",
            exc_info=True,
        )


@router.post("/sessions/{session_id}/rsyncer_stopped")
def register_stopped_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = False
    db.add(rsyncer)
    db.commit()


@router.post("/sessions/{session_id}/rsyncer_started")
def register_restarted_rsyncer(
    session_id: int, rsyncer_source: RsyncerSource, db=murfey_db
):
    rsyncer = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == rsyncer_source.source)
    ).one()
    rsyncer.transferring = True
    db.add(rsyncer)
    db.commit()


@router.post("/visits/{visit_name}/increment_rsync_file_count")
def increment_rsync_file_count(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.session_id == rsyncer_info.session_id,
        )
    ).one()
    rsync_instance.files_counted += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()
    prom.seen_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_count
    )
    prom.seen_data_files.labels(rsync_source=rsyncer_info.source, visit=visit_name).inc(
        rsyncer_info.increment_data_count
    )


@router.post("/visits/{visit_name}/increment_rsync_transferred_files")
def increment_rsync_transferred_files(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    rsync_instance = db.exec(
        select(RsyncInstance).where(
            RsyncInstance.source == rsyncer_info.source,
            RsyncInstance.destination == rsyncer_info.destination,
            RsyncInstance.session_id == rsyncer_info.session_id,
        )
    ).one()
    rsync_instance.files_transferred += rsyncer_info.increment_count
    db.add(rsync_instance)
    db.commit()
    db.close()


@router.post("/visits/{visit_name}/increment_rsync_transferred_files_prometheus")
def increment_rsync_transferred_files_prometheus(
    visit_name: str, rsyncer_info: RsyncerInfo, db=murfey_db
):
    prom.transferred_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_count)
    prom.transferred_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.bytes)
    prom.transferred_data_files.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.increment_data_count)
    prom.transferred_data_files_bytes.labels(
        rsync_source=rsyncer_info.source, visit=visit_name
    ).inc(rsyncer_info.data_bytes)


class ProcessingDetails(BaseModel):
    data_collection_group: DataCollectionGroup
    data_collections: List[DataCollection]
    processing_jobs: List[ProcessingJob]
    relion_params: SPARelionParameters
    feedback_params: SPAFeedbackParameters


@router.post("/sessions/{session_id}/spa_processing_parameters")
def register_spa_proc_params(
    session_id: MurfeySessionID, proc_params: ProcessingParametersSPA, db=murfey_db
):
    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_processing_parameters:
        proc_params.gain_ref = session_processing_parameters[0].gain_ref
        proc_params.dose_per_frame = session_processing_parameters[0].dose_per_frame
        proc_params.eer_fractionation_file = session_processing_parameters[
            0
        ].eer_fractionation_file
        proc_params.symmetry = session_processing_parameters[0].symmetry

    zocalo_message = {
        "register": "spa_processing_parameters",
        **dict(proc_params),
        "session_id": session_id,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


@router.get("/sessions/{session_id}/grid_squares")
def get_grid_squares(session_id: MurfeySessionID, db=murfey_db):
    grid_squares = db.exec(
        select(GridSquare).where(GridSquare.session_id == session_id)
    ).all()
    tags = {gs.tag for gs in grid_squares}
    res = {}
    for t in tags:
        res[t] = [gs for gs in grid_squares if gs.tag == t]
    return res


@router.get("/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares")
def get_grid_squares_from_dcg(
    session_id: int, dcgid: int, db=murfey_db
) -> List[GridSquare]:
    grid_squares = db.exec(
        select(GridSquare, DataCollectionGroup)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return [gs[0] for gs in grid_squares]


@router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{gsid}/num_movies"
)
def get_number_of_movies_from_grid_square(
    session_id: int, dcgid: int, gsid: int, db=murfey_db
) -> int:
    movies = db.exec(
        select(Movie, FoilHole, GridSquare, DataCollectionGroup)
        .where(Movie.foil_hole_id == FoilHole.id)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.name == gsid)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return len(movies)


@router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{gsid}/foil_holes"
)
def get_foil_holes_from_grid_square(
    session_id: int, dcgid: int, gsid: int, db=murfey_db
) -> List[FoilHole]:
    foil_holes = db.exec(
        select(FoilHole, GridSquare, DataCollectionGroup)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.name == gsid)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return [fh[0] for fh in foil_holes]


@router.get(
    "/sessions/{session_id}/data_collection_groups/{dcgid}/grid_squares/{gsid}/foil_holes/{fhid}/num_movies"
)
def get_number_of_movies_from_foil_hole(
    session_id: int, dcgid: int, gsid: int, fhid: int, db=murfey_db
) -> int:
    movies = db.exec(
        select(Movie, FoilHole, GridSquare, DataCollectionGroup)
        .where(Movie.foil_hole_id == FoilHole.id)
        .where(FoilHole.name == fhid)
        .where(FoilHole.grid_square_id == GridSquare.id)
        .where(GridSquare.name == gsid)
        .where(GridSquare.session_id == session_id)
        .where(GridSquare.tag == DataCollectionGroup.tag)
        .where(DataCollectionGroup.id == dcgid)
    ).all()
    return len(movies)


@router.post("/sessions/{session_id}/grid_square/{gsid}")
def posted_grid_square(
    session_id: MurfeySessionID,
    gsid: int,
    grid_square_params: GridSquareParameters,
    db=murfey_db,
):
    return register_grid_square(session_id, gsid, grid_square_params, db)


@router.get("/sessions/{session_id}/foil_hole/{fh_name}")
def get_foil_hole(
    session_id: MurfeySessionID, fh_name: int, db=murfey_db
) -> Dict[str, int]:
    foil_holes = db.exec(
        select(FoilHole, GridSquare)
        .where(FoilHole.name == fh_name)
        .where(FoilHole.session_id == session_id)
        .where(GridSquare.id == FoilHole.grid_square_id)
    ).all()
    return {f[1].tag: f[0].id for f in foil_holes}


@router.post("/sessions/{session_id}/grid_square/{gs_name}/foil_hole")
def post_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    db=murfey_db,
):
    log.info(
        f"Registering foil hole {foil_hole_params.name} with position {(foil_hole_params.x_location, foil_hole_params.y_location)}"
    )
    return register_foil_hole(session_id, gs_name, foil_hole_params, db)


@router.post("/sessions/{session_id}/tomography_processing_parameters")
def register_tomo_proc_params(
    session_id: MurfeySessionID, proc_params: ProcessingParametersTomo, db=murfey_db
):
    session_processing_parameters = db.exec(
        select(SessionProcessingParameters).where(
            SessionProcessingParameters.session_id == session_id
        )
    ).all()
    if session_processing_parameters:
        proc_params.gain_ref = session_processing_parameters[0].gain_ref
        proc_params.dose_per_frame = session_processing_parameters[0].dose_per_frame
        proc_params.eer_fractionation_file = session_processing_parameters[
            0
        ].eer_fractionation_file

    zocalo_message = {
        "register": "tomography_processing_parameters",
        **dict(proc_params),
        "session_id": session_id,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


class Tag(BaseModel):
    tag: str


@router.post("/visits/{visit_name}/{session_id}/flush_spa_processing")
def flush_spa_processing(
    visit_name: str, session_id: MurfeySessionID, tag: Tag, db=murfey_db
):
    zocalo_message = {
        "register": "spa.flush_spa_preprocess",
        "session_id": session_id,
        "tag": tag.tag,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)
    return


class Source(BaseModel):
    rsync_source: str


@router.post("/visits/{visit_name}/{session_id}/flush_tomography_processing")
def flush_tomography_processing(
    visit_name: str, session_id: MurfeySessionID, rsync_source: Source, db=murfey_db
):
    zocalo_message = {
        "register": "flush_tomography_preprocess",
        "session_id": session_id,
        "visit_name": visit_name,
        "data_collection_group_tag": rsync_source.rsync_source,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)
    return


@router.post("/visits/{visit_name}/tilt_series")
def register_tilt_series(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    session_id = tilt_series_info.session_id
    if db.exec(
        select(TiltSeries)
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.tag == tilt_series_info.tag)
        .where(TiltSeries.rsync_source == tilt_series_info.source)
    ).all():
        return
    tilt_series = TiltSeries(
        session_id=session_id,
        tag=tilt_series_info.tag,
        rsync_source=tilt_series_info.source,
    )
    db.add(tilt_series)
    db.commit()


@router.post("/sessions/{session_id}/tilt_series_length")
def register_tilt_series_length(
    session_id: int,
    tilt_series_group: TiltSeriesGroupInfo,
    db=murfey_db,
):
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(col(TiltSeries.tag).in_(tilt_series_group.tags))
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.rsync_source == tilt_series_group.source)
    ).all()
    for ts in tilt_series_db:
        ts_index = tilt_series_group.tags.index(ts.tag)
        ts.tilt_series_length = tilt_series_group.tilt_series_lengths[ts_index]
        db.add(ts)
    db.commit()


@router.post("/visits/{visit_name}/{session_id}/completed_tilt_series")
def register_completed_tilt_series(
    visit_name: str,
    session_id: MurfeySessionID,
    tilt_series_group: TiltSeriesGroupInfo,
    db=murfey_db,
):
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(col(TiltSeries.tag).in_(tilt_series_group.tags))
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.rsync_source == tilt_series_group.source)
    ).all()
    for ts in tilt_series_db:
        ts_index = tilt_series_group.tags.index(ts.tag)
        ts.tilt_series_length = tilt_series_group.tilt_series_lengths[ts_index]
        db.add(ts)
    db.commit()
    for ts in tilt_series_db:
        if (
            check_tilt_series_mc(ts.id)
            and not ts.processing_requested
            and ts.tilt_series_length > 2
        ):
            ts.processing_requested = True
            db.add(ts)

            collected_ids = db.exec(
                select(
                    DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram
                )
                .where(DataCollectionGroup.session_id == session_id)
                .where(DataCollectionGroup.tag == tilt_series_group.source)
                .where(DataCollection.tag == ts.tag)
                .where(DataCollection.dcg_id == DataCollectionGroup.id)
                .where(ProcessingJob.dc_id == DataCollection.id)
                .where(AutoProcProgram.pj_id == ProcessingJob.id)
                .where(ProcessingJob.recipe == "em-tomo-align")
            ).one()
            instrument_name = (
                db.exec(select(Session).where(Session.id == session_id))
                .one()
                .instrument_name
            )
            machine_config = get_machine_config(instrument_name=instrument_name)[
                instrument_name
            ]
            tilts = get_all_tilts(ts.id)
            ids = get_job_ids(ts.id, collected_ids[3].id)
            preproc_params = get_tomo_proc_params(ids.dcgid)

            first_tilt = db.exec(
                select(Tilt).where(Tilt.tilt_series_id == ts.id)
            ).first()
            parts = [secure_filename(p) for p in Path(first_tilt.movie_path).parts]
            visit_idx = parts.index(visit_name)
            core = Path(*Path(first_tilt.movie_path).parts[: visit_idx + 1])
            ppath = Path(
                "/".join(secure_filename(p) for p in Path(first_tilt.movie_path).parts)
            )
            sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
            extra_path = machine_config.processed_extra_directory
            stack_file = (
                core
                / machine_config.processed_directory_name
                / sub_dataset
                / extra_path
                / "Tomograms"
                / "job006"
                / "tomograms"
                / f"{ts.tag}_stack.mrc"
            )
            if not stack_file.parent.exists():
                stack_file.parent.mkdir(parents=True)
            tilt_offset = midpoint([float(get_angle(t)) for t in tilts])
            zocalo_message = {
                "recipes": ["em-tomo-align"],
                "parameters": {
                    "input_file_list": str([[t, str(get_angle(t))] for t in tilts]),
                    "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
                    "dcid": ids.dcid,
                    "appid": ids.appid,
                    "stack_file": str(stack_file),
                    "dose_per_frame": preproc_params.dose_per_frame,
                    "frame_count": preproc_params.frame_count,
                    "kv": preproc_params.voltage,
                    "tilt_axis": preproc_params.tilt_axis,
                    "pixel_size": preproc_params.pixel_size,
                    "manual_tilt_offset": -tilt_offset,
                    "node_creator_queue": machine_config.node_creator_queue,
                },
            }
            if _transport_object:
                log.info(f"Sending Zocalo message for processing: {zocalo_message}")
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
            else:
                log.info(
                    f"No transport object found. Zocalo message would be {zocalo_message}"
                )
    db.commit()


@router.post("/visits/{visit_name}/rerun_tilt_series")
def register_tilt_series_for_rerun(
    visit_name: str, tilt_series_info: TiltSeriesInfo, db=murfey_db
):
    """Set processing to false for cases where an extra tilt is found for a series"""
    session_id = tilt_series_info.session_id
    tilt_series_db = db.exec(
        select(TiltSeries)
        .where(TiltSeries.session_id == session_id)
        .where(TiltSeries.tag == tilt_series_info.tag)
        .where(TiltSeries.rsync_source == tilt_series_info.source)
    ).all()
    for ts in tilt_series_db:
        ts.processing_requested = False
        db.add(ts)
    db.commit()


@router.post("/visits/{visit_name}/{session_id}/tilt")
async def register_tilt(
    visit_name: str, session_id: MurfeySessionID, tilt_info: TiltInfo, db=murfey_db
):
    def _add_tilt():
        tilt_series_id = (
            db.exec(
                select(TiltSeries)
                .where(TiltSeries.tag == tilt_info.tilt_series_tag)
                .where(TiltSeries.session_id == session_id)
                .where(TiltSeries.rsync_source == tilt_info.source)
            )
            .one()
            .id
        )
        if db.exec(
            select(Tilt)
            .where(Tilt.movie_path == tilt_info.movie_path)
            .where(Tilt.tilt_series_id == tilt_series_id)
        ).all():
            return
        tilt = Tilt(movie_path=tilt_info.movie_path, tilt_series_id=tilt_series_id)
        db.add(tilt)
        db.commit()

    try:
        _add_tilt()
    except OperationalError:
        await asyncio.sleep(30)
        _add_tilt()


@router.get("/visit/{visit_name}/samples")
def get_samples(visit_name: str, db=murfey.server.ispyb.DB) -> List[Sample]:
    return murfey.server.ispyb.get_sub_samples_from_visit(visit_name, db=db)


@router.post("/visit/{visit_name}/sample_group")
def register_sample_group(visit_name: str, db=murfey.server.ispyb.DB) -> dict:
    proposal_id = murfey.server.ispyb.get_proposal_id(
        visit_name[:2], visit_name.split("-")[0][2:], db=db
    )
    record = BLSampleGroup(proposalId=proposal_id)
    if _transport_object:
        return _transport_object.do_insert_sample_group(record)
    return {"success": False}


@router.post("/visit/{visit_name}/sample")
def register_sample(visit_name: str, sample_params: BLSampleParameters) -> dict:
    record = BLSample()
    if _transport_object:
        return _transport_object.do_insert_sample(record, sample_params.sample_group_id)
    return {"success": False}


@router.post("/visit/{visit_name}/subsample")
def register_subsample(
    visit_name: str, subsample_params: BLSubSampleParameters
) -> dict:
    record = BLSubSample(
        blSampleId=subsample_params.sample_id, imgFilePath=subsample_params.image_path
    )
    if _transport_object:
        return _transport_object.do_insert_subsample(record)
    return {"success": False}


@router.post("/visit/{visit_name}/sample_image")
def register_sample_image(
    visit_name: str, sample_image_params: BLSampleImageParameters
) -> dict:
    record = BLSampleImage(
        blSampleId=sample_image_params.sample_id,
        imageFullPath=sample_image_params.image_path,
    )
    if _transport_object:
        return _transport_object.do_insert_sample_image(record)
    return {"success": False}


@router.get("/instruments/{instrument_name}/visits/{visit_name}")
def visit_info(
    request: Request, instrument_name: str, visit_name: str, db=murfey.server.ispyb.DB
):
    query = (
        db.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == instrument_name,
            BLSession.endDate > datetime.datetime.now(),
            BLSession.startDate < datetime.datetime.now(),
        )
        .add_columns(
            BLSession.startDate,
            BLSession.endDate,
            BLSession.beamLineName,
            Proposal.proposalCode,
            Proposal.proposalNumber,
            BLSession.visit_number,
            Proposal.title,
        )
        .all()
    )
    if query:
        return_query = [
            {
                "Start date": id.startDate,
                "End date": id.endDate,
                "Beamline name": id.beamLineName,
                "Visit name": visit_name,
                "Time remaining": str(id.endDate - datetime.datetime.now()),
            }
            for id in query
            if id.proposalCode + str(id.proposalNumber) + "-" + str(id.visit_number)
            == visit_name
        ]  # "Proposal title": id.title
        return templates.TemplateResponse(
            request=request,
            name="visit.html",
            context={"visit": return_query},
        )
    else:
        return None


@router.post("/instruments/{instrument_name}/feedback")
async def send_murfey_message(instrument_name: str, msg: RegistrationMessage):
    if _transport_object:
        _transport_object.send(
            _transport_object.feedback_queue, {"register": msg.registration}
        )


@router.post("/visits/{visit_name}/{session_id}/spa_preprocess")
async def request_spa_preprocessing(
    visit_name: str,
    session_id: MurfeySessionID,
    proc_file: SPAProcessFile,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    parts = [secure_filename(p) for p in Path(proc_file.path).parts]
    visit_idx = parts.index(visit_name)
    core = Path("/") / Path(*parts[: visit_idx + 1])
    ppath = Path("/") / Path(*parts)
    sub_dataset = ppath.relative_to(core).parts[0]
    extra_path = machine_config.processed_extra_directory
    for i, p in enumerate(ppath.parts):
        if p.startswith("raw"):
            movies_path_index = i
            break
    else:
        raise ValueError(f"{proc_file.path} does not contain a raw directory")
    mrc_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / extra_path
        / "MotionCorr"
        / "job002"
        / "Movies"
        / "/".join(ppath.parts[movies_path_index + 1 : -1])
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    try:
        collected_ids = db.exec(
            select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
            .where(DataCollectionGroup.session_id == session_id)
            .where(DataCollectionGroup.tag == proc_file.tag)
            .where(DataCollection.dcg_id == DataCollectionGroup.id)
            .where(ProcessingJob.dc_id == DataCollection.id)
            .where(AutoProcProgram.pj_id == ProcessingJob.id)
            .where(ProcessingJob.recipe == "em-spa-preprocess")
        ).one()
        params = db.exec(
            select(SPARelionParameters, SPAFeedbackParameters)
            .where(SPARelionParameters.pj_id == collected_ids[2].id)
            .where(SPAFeedbackParameters.pj_id == SPARelionParameters.pj_id)
        ).one()
        proc_params: dict | None = dict(params[0])
        feedback_params = params[1]
    except sqlalchemy.exc.NoResultFound:
        proc_params = None
    try:
        foil_hole_id = (
            db.exec(
                select(FoilHole, GridSquare)
                .where(FoilHole.name == proc_file.foil_hole_id)
                .where(FoilHole.session_id == session_id)
                .where(GridSquare.id == FoilHole.grid_square_id)
                .where(GridSquare.tag == proc_file.tag)
            )
            .one()[0]
            .id
        )
    except Exception as e:
        log.warning(
            f"Foil hole ID not found for foil hole {sanitise(str(proc_file.foil_hole_id))}: {e}",
            exc_info=True,
        )
        foil_hole_id = None
    if proc_params:

        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], db, number=2, close=False)

        if feedback_params.picker_murfey_id is None:
            feedback_params.picker_murfey_id = murfey_ids[1]
            db.add(feedback_params)
        movie = Movie(
            murfey_id=murfey_ids[0],
            path=proc_file.path,
            image_number=proc_file.image_number,
            tag=proc_file.tag,
            foil_hole_id=foil_hole_id,
        )
        db.add(movie)
        db.commit()
        db.close()

        if not mrc_out.parent.exists():
            Path(secure_filename(str(mrc_out))).parent.mkdir(
                parents=True, exist_ok=True
            )
        recipe_name = machine_config.recipes.get(
            "em-spa-preprocess", "em-spa-preprocess"
        )
        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": detached_ids[1],
                "kv": proc_params["voltage"],
                "autoproc_program_id": detached_ids[3],
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pixel_size": proc_params["angpix"],
                "image_number": proc_file.image_number,
                "microscope": instrument_name,
                "mc_uuid": murfey_ids[0],
                "foil_hole_id": foil_hole_id,
                "ft_bin": proc_params["motion_corr_binning"],
                "fm_dose": proc_params["dose_per_frame"],
                "gain_ref": proc_params["gain_ref"],
                "picker_uuid": murfey_ids[1],
                "session_id": session_id,
                "particle_diameter": proc_params["particle_diameter"] or 0,
                "fm_int_file": (
                    proc_params["eer_fractionation_file"]
                    if proc_params["eer_fractionation_file"]
                    else proc_file.eer_fractionation_file
                ),
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "cryolo_model_weights": str(
                    _cryolo_model_path(visit_name, instrument_name)
                ),
            },
        }
        # log.info(f"Sending Zocalo message {zocalo_message}")
        if _transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = _transport_object.feedback_queue
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            log.error(
                f"Pe-processing was requested for {sanitise(ppath.name)} but no Zocalo transport object was found"
            )
            return proc_file

    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            tag=proc_file.tag,
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            eer_fractionation_file=str(proc_file.eer_fractionation_file),
            foil_hole_id=foil_hole_id,
        )
        db.add(for_stash)
        db.commit()
        db.close()

    return proc_file


@router.post("/visits/{visit_name}/{session_id}/tomography_preprocess")
async def request_tomography_preprocessing(
    visit_name: str,
    session_id: MurfeySessionID,
    proc_file: TomoProcessFile,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path("/".join(secure_filename(p) for p in Path(proc_file.path).parts))
    sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
    extra_path = machine_config.processed_extra_directory
    mrc_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / extra_path
        / "MotionCorr"
        / "job002"
        / "Movies"
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    mrc_out = Path("/".join(secure_filename(p) for p in mrc_out.parts))

    recipe_name = machine_config.recipes.get("em-tomo-preprocess", "em-tomo-preprocess")

    data_collection = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == proc_file.group_tag)
        .where(DataCollection.tag == proc_file.tag)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
        .where(ProcessingJob.recipe == recipe_name)
    ).all()
    if data_collection:
        if registered_tilts := db.exec(
            select(Tilt).where(Tilt.movie_path == proc_file.path)
        ).all():
            if len(registered_tilts) == 1:
                if registered_tilts[0].motion_corrected:
                    return proc_file
        dcid = data_collection[0][1].id
        appid = data_collection[0][3].id
        murfey_ids = _murfey_id(appid, db, number=1, close=False)
        if not mrc_out.parent.exists():
            mrc_out.parent.mkdir(parents=True, exist_ok=True)

        session_processing_parameters = db.exec(
            select(SessionProcessingParameters).where(
                SessionProcessingParameters.session_id == session_id
            )
        ).all()
        if session_processing_parameters:
            proc_file.gain_ref = session_processing_parameters[0].gain_ref
            proc_file.dose_per_frame = session_processing_parameters[0].dose_per_frame
            proc_file.eer_fractionation_file = session_processing_parameters[
                0
            ].eer_fractionation_file

        zocalo_message: dict = {
            "recipes": [recipe_name],
            "parameters": {
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": dcid,
                # "timestamp": datetime.datetime.now(),
                "autoproc_program_id": appid,
                "movie": proc_file.path,
                "mrc_out": str(mrc_out),
                "pixel_size": (proc_file.pixel_size) * 10**10,
                "image_number": proc_file.image_number,
                "kv": int(proc_file.voltage),
                "microscope": instrument_name,
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_file.mc_binning,
                "fm_dose": proc_file.dose_per_frame,
                "frame_count": proc_file.frame_count,
                "gain_ref": (
                    str(machine_config.rsync_basepath / proc_file.gain_ref)
                    if proc_file.gain_ref and machine_config.data_transfer_enabled
                    else proc_file.gain_ref
                ),
                "fm_int_file": proc_file.eer_fractionation_file,
            },
        }
        if _transport_object:
            zocalo_message["parameters"][
                "feedback_queue"
            ] = _transport_object.feedback_queue
            _transport_object.send("processing_recipe", zocalo_message)
        else:
            log.error(
                f"Pe-processing was requested for {sanitise(ppath.name)} but no Zocalo transport object was found"
            )
            return proc_file
    else:
        for_stash = PreprocessStash(
            file_path=str(proc_file.path),
            session_id=session_id,
            image_number=proc_file.image_number,
            mrc_out=str(mrc_out),
            tag=proc_file.tag,
            group_tag=proc_file.group_tag,
        )
        db.add(for_stash)
        db.commit()
        db.close()
    return proc_file


@router.delete("/sessions/{session_id}")
def remove_session_by_id(session_id: MurfeySessionID, db=murfey_db):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    sessions_for_visit = db.exec(
        select(Session).where(Session.visit == session.visit)
    ).all()
    # Don't remove prometheus metrics if there are other sessions using them
    if len(sessions_for_visit) == 1:
        safe_run(
            prom.monitoring_switch.remove,
            args=(session.visit,),
            label="monitoring_switch",
        )
        rsync_instances = db.exec(
            select(RsyncInstance).where(RsyncInstance.session_id == session_id)
        ).all()
        for ri in rsync_instances:
            safe_run(
                prom.seen_files.remove,
                args=(ri.source, session.visit),
                label="seen_files",
            )
            safe_run(
                prom.transferred_files.remove,
                args=(ri.source, session.visit),
                label="transferred_files",
            )
            safe_run(
                prom.transferred_files_bytes.remove,
                args=(ri.source, session.visit),
                label="transferred_files_bytes",
            )
            safe_run(
                prom.seen_data_files.remove,
                args=(ri.source, session.visit),
                label="seen_data_files",
            )
            safe_run(
                prom.transferred_data_files.remove,
                args=(ri.source, session.visit),
                label="transferred_data_files",
            )
            safe_run(
                prom.transferred_data_files_bytes.remove,
                args=(ri.source, session.visit),
                label="transferred_data_file_bytes",
            )
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
    ).all()
    for c in collected_ids:
        safe_run(
            prom.preprocessed_movies.remove,
            args=(c[2].id,),
            label="preprocessed_movies",
        )
    db.delete(session)
    db.commit()
    return


@router.post("/visits/{year}/{visit_name}/{session_id}/make_milling_gif")
async def make_gif(
    year: int,
    visit_name: str,
    session_id: int,
    gif_params: MillingParameters,
    db=murfey_db,
):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    output_dir = (
        Path(machine_config.rsync_basepath)
        / secure_filename(year)
        / secure_filename(visit_name)
        / "processed"
    )
    output_dir.mkdir(exist_ok=True)
    output_dir = output_dir / secure_filename(gif_params.raw_directory)
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"lamella_{gif_params.lamella_number}_milling.gif"
    image_full_paths = [
        output_dir.parent / gif_params.raw_directory / i for i in gif_params.images
    ]
    images = [Image.open(f) for f in image_full_paths]
    for im in images:
        im.thumbnail((512, 512))
    images[0].save(
        output_path,
        format="GIF",
        append_images=images[1:],
        save_all=True,
        duration=30,
        loop=0,
    )
    return {"output_gif": str(output_path)}


@router.get("/new_client_id/")
async def new_client_id(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    if not clients:
        return {"new_id": 0}
    sorted_ids = sorted([c.client_id for c in clients])
    return {"new_id": sorted_ids[-1] + 1}


@router.post("/instruments/{instrument_name}/clients/{client_id}/session")
def link_client_to_session(
    instrument_name: str, client_id: int, sess: SessionInfo, db=murfey_db
):
    sid = sess.session_id
    if sid is None:
        s = Session(name=sess.session_name, instrument_name=instrument_name)
        db.add(s)
        db.commit()
        sid = s.id
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    client.session_id = sid
    db.add(client)
    db.commit()
    db.close()
    return sid


@router.post("/sessions/{session_id}/successful_processing")
def register_processing_success_in_ispyb(
    session_id: MurfeySessionID, db=murfey.server.ispyb.DB, murfey_db=murfey_db
):
    collected_ids = murfey_db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
        .where(AutoProcProgram.pj_id == ProcessingJob.id)
    ).all()
    appids = [c[3].id for c in collected_ids]
    if _transport_object:
        apps = db.query(ISPyBAutoProcProgram).filter(
            ISPyBAutoProcProgram.autoProcProgramId.in_(appids)
        )
        for updated in apps:
            updated.processingStatus = True
            _transport_object.do_update_processing_status(updated)


@router.post("/visits/{visit_name}/monitoring/{on}")
def change_monitoring_status(visit_name: str, on: int):
    prom.monitoring_switch.labels(visit=visit_name)
    prom.monitoring_switch.labels(visit=visit_name).set(on)


@router.post("/instruments/{instrument_name}/failed_client_post")
def failed_client_post(instrument_name: str, post_info: PostInfo):
    zocalo_message = {
        "register": "failed_client_post",
        "url": post_info.url,
        "json": post_info.data,
    }
    if _transport_object:
        _transport_object.send(_transport_object.feedback_queue, zocalo_message)


@router.get("/sessions/{session_id}/upstream_visits")
async def find_upstream_visits(session_id: MurfeySessionID, db=murfey_db):
    murfey_session = db.exec(select(Session).where(Session.id == session_id)).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    upstream_visits = {}
    # Iterates through provided upstream directories
    for p in machine_config.upstream_data_directories:
        # Looks for visit name in file path
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = v / machine_config.processed_directory_name
    return upstream_visits


def _get_upstream_tiff_dirs(visit_name: str, instrument_name: str) -> List[Path]:
    tiff_dirs = []
    machine_config = get_machine_config(instrument_name=instrument_name)[
        instrument_name
    ]
    for directory_name in machine_config.upstream_data_tiff_locations:
        for p in machine_config.upstream_data_directories:
            if (Path(p) / secure_filename(visit_name)).is_dir():
                processed_dir = Path(p) / secure_filename(visit_name) / directory_name
                tiff_dirs.append(processed_dir)
                break
    if not tiff_dirs:
        log.warning(
            f"No candidate directory found for upstream download from visit {sanitise(visit_name)}"
        )
    return tiff_dirs


@router.get("/visits/{visit_name}/{session_id}/upstream_tiff_paths")
async def gather_upstream_tiffs(visit_name: str, session_id: int, db=murfey_db):
    """
    Looks for TIFF files associated with the current session in the permitted storage
    servers, and returns their relative file paths as a list.
    """
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    upstream_tiff_paths = []
    tiff_dirs = _get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
        for f in tiff_dir.glob("**/*.tif"):
            upstream_tiff_paths.append(str(f.relative_to(tiff_dir)))
    return upstream_tiff_paths


@router.get("/visits/{visit_name}/{session_id}/upstream_tiff/{tiff_path:path}")
async def get_tiff(visit_name: str, session_id: int, tiff_path: str, db=murfey_db):
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    tiff_dirs = _get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None

    tiff_path = "/".join(secure_filename(p) for p in tiff_path.split("/"))
    for tiff_dir in tiff_dirs:
        test_path = tiff_dir / tiff_path
        if test_path.is_file():
            break
    else:
        log.warning(f"TIFF {tiff_path} not found")
        return None

    return FileResponse(path=test_path)


@router.post("/instruments/{instrument_name}/visits/{visit}/session/{name}")
def create_session(instrument_name: str, visit: str, name: str, db=murfey_db) -> int:
    s = Session(name=name, visit=visit, instrument_name=instrument_name)
    db.add(s)
    db.commit()
    sid = s.id
    return sid


@router.post("/sessions/{session_id}")
def update_session(
    session_id: MurfeySessionID, process: bool = True, db=murfey_db
) -> None:
    session = db.exec(select(Session).where(session_id == session_id)).one()
    session.process = process
    db.add(session)
    db.commit()
    return None


@router.put("/sessions/{session_id}/current_gain_ref")
def update_current_gain_ref(
    session_id: MurfeySessionID, new_gain_ref: CurrentGainRef, db=murfey_db
):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    session.current_gain_ref = new_gain_ref.path
    db.add(session)
    db.commit()


@router.get("/prometheus/{metric_name}")
def inspect_prometheus_metrics(
    metric_name: str,
):
    """
    A debugging endpoint that returns the current contents of any Prometheus
    gauges and counters that have been set up thus far.
    """

    # Extract the Prometheus metric defined in the Prometheus module
    metric: Optional[Counter | Gauge] = getattr(prom, metric_name, None)
    if metric is None or not isinstance(metric, (Counter, Gauge)):
        raise LookupError("No matching metric was found")

    # Package contents into dict and return
    results = {}
    if hasattr(metric, "_metrics"):
        for i, (label_tuple, sub_metric) in enumerate(metric._metrics.items()):
            labels = dict(zip(metric._labelnames, label_tuple))
            labels["value"] = sub_metric._value.get()
            results[i] = labels
        return results
    else:
        value = metric._value.get()
        return {"value": value}
