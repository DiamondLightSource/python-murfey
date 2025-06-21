from __future__ import annotations

import datetime
import logging
import os
from functools import lru_cache
from itertools import count
from pathlib import Path
from typing import Dict, List, Optional

import packaging.version
from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse
from ispyb.sqlalchemy import BLSession
from PIL import Image
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlalchemy import func
from sqlmodel import select
from werkzeug.utils import secure_filename

import murfey.server.api.bootstrap
import murfey.server.prometheus as prom
from murfey.server.api import templates
from murfey.server.api.auth import MurfeySessionIDFrontend as MurfeySessionID
from murfey.server.api.auth import validate_token
from murfey.server.api.session_info import Visit
from murfey.server.api.workflow import (
    DCGroupParameters,
    DCParameters,
    ProcessingJobParameters,
)
from murfey.server.feedback import (
    _flush_grid_square_records,
    _murfey_id,
    get_microscope,
    sanitise,
)
from murfey.server.murfey_db import murfey_db
from murfey.server.run import shutdown as _shutdown
from murfey.util import sanitise_path
from murfey.util.config import (
    MachineConfig,
    from_file,
    get_hostname,
    security_from_file,
)
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
    SPAFeedbackParameters,
    SPARelionParameters,
    Tilt,
    TiltSeries,
)
from murfey.util.models import (
    ClientInfo,
    FoilHoleParameters,
    GridSquareParameters,
    RegistrationMessage,
)
from murfey.util.processing_params import default_spa_parameters

log = logging.getLogger("murfey.server.demo_api")

tags_metadata = [murfey.server.api.bootstrap.tag]

router = APIRouter(dependencies=[Depends(validate_token)])
router.raw_count = 2


global_counter = count()


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()

machine_config: dict[str, MachineConfig] = {}
if settings.murfey_machine_configuration:
    microscope = get_microscope()
    machine_config = from_file(Path(settings.murfey_machine_configuration), microscope)


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


@router.get("/microscope_image/")
def get_mic_image():
    if machine_config.get("image_path"):
        return FileResponse(machine_config["image_path"])
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
    if machine_config.get(instrument_name):
        return machine_config[instrument_name].display_name
    return ""


@router.get("/instruments/{instrument_name}/visits/")
def all_visit_info(instrument_name: str, request: Request):
    return_query = [
        {
            "Start date": datetime.datetime.now(),
            "End date": datetime.datetime.now(),
            "Visit name": "dummy",
            "Time remaining": 0,
        }
    ]  # "Proposal title": visit.proposal_title

    return templates.TemplateResponse(
        request=request,
        name="activevisits.html",
        context={"info": return_query, "microscope": instrument_name},
    )


@router.post("/visits/{visit_name}")
def register_client_to_visit(visit_name: str, client_info: ClientInfo, db=murfey_db):
    client_env = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_info.id)
    ).one()
    if client_env:
        client_env.visit = visit_name
        db.add(client_env)
        db.commit()
    return client_info


@router.get("/num_movies")
def count_number_of_movies(db=murfey_db) -> Dict[str, int]:
    res = db.exec(
        select(Movie.tag, func.count(Movie.murfey_id)).group_by(Movie.tag)
    ).all()
    return {r[0]: r[1] for r in res}


@router.delete("/sessions/{session_id}/rsyncer/{source:path}")
def delete_rsyncer(session_id: int, source: str, db=murfey_db):
    rsync_instance = db.exec(
        select(RsyncInstance)
        .where(RsyncInstance.session_id == session_id)
        .where(RsyncInstance.source == source)
    ).one()
    db.delete(rsync_instance)
    db.commit()


@router.get("/clients/{client_id}/rsyncers")
def get_rsyncers_for_client(client_id: int, db=murfey_db):
    log.info("rsyncers requested")
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    )
    res = rsync_instances.all()
    log.info(res)
    return res


class SessionClients(BaseModel):
    session: Session
    clients: List[ClientEnvironment]


@router.get("/session/{session_id}")
async def get_session(session_id: MurfeySessionID, db=murfey_db) -> SessionClients:
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    clients = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.session_id == session_id)
    ).all()
    return SessionClients(session=session, clients=clients)


class ProcessingDetails(BaseModel):
    data_collection_group: DataCollectionGroup
    data_collections: List[DataCollection]
    processing_jobs: List[ProcessingJob]
    relion_params: SPARelionParameters
    feedback_params: SPAFeedbackParameters


@router.get("/sessions/{session_id}/spa_processing_parameters")
def get_spa_proc_param_details(
    session_id: MurfeySessionID, db=murfey_db
) -> Optional[List[ProcessingDetails]]:
    params = db.exec(
        select(
            DataCollectionGroup,
            DataCollection,
            ProcessingJob,
            SPARelionParameters,
            SPAFeedbackParameters,
        )
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.id == DataCollection.dcg_id)
        .where(DataCollection.id == ProcessingJob.dc_id)
        .where(SPARelionParameters.pj_id == ProcessingJob.id)
        .where(SPAFeedbackParameters.pj_id == ProcessingJob.id)
    ).all()
    if not params:
        return None
    unique_dcg_indices = []
    dcg_ids = []
    for i, p in enumerate(params):
        if p[0].id not in dcg_ids:
            dcg_ids.append(p[0].id)
            unique_dcg_indices.append(i)

    def _parse(ps, i, dcg_id):
        res = []
        for p in ps:
            if p[0].id == dcg_id:
                if p[i] not in res:
                    res.append(p[i])
        return res

    return [
        ProcessingDetails(
            data_collection_group=params[i][0],
            data_collections=_parse(params, 1, d),
            processing_jobs=_parse(params, 2, d),
            relion_params=_parse(params, 3, d)[0],
            feedback_params=_parse(params, 4, d)[0],
        )
        for i, d in zip(unique_dcg_indices, dcg_ids)
    ]


@router.get("/clients/{client_id}/spa_processing_parameters")
def get_spa_proc_params(client_id: int, db=murfey_db) -> List[dict]:
    params = db.exec(
        select(SPARelionParameters).where(SPARelionParameters.client_id == client_id)
    ).all()
    return [p.json() for p in params]


@router.get("/sessions/{session_id}/grid_squares")
def get_grid_squares(session_id: int, db=murfey_db):
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
def register_grid_square(
    session_id: int,
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
    except Exception:
        if sanitise_path(Path(grid_square_params.image)).is_file():
            jpeg_size = Image.open(grid_square_params.image).size
        else:
            jpeg_size = (0, 0)
        grid_square = GridSquare(
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
            image=grid_square_params.image,
        )
    db.add(grid_square)
    db.commit()
    db.close()


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
def register_foil_hole(
    session_id: MurfeySessionID,
    gs_name: int,
    foil_hole_params: FoilHoleParameters,
    db=murfey_db,
):
    gsid = (
        db.exec(
            select(GridSquare)
            .where(GridSquare.tag == foil_hole_params.tag)
            .where(GridSquare.session_id == session_id)
            .where(GridSquare.name == gs_name)
        )
        .one()
        .id
    )
    if foil_hole_params.image and sanitise_path(Path(foil_hole_params.image)).is_file():
        jpeg_size = Image.open(foil_hole_params.image).size
    else:
        jpeg_size = (0, 0)
    foil_hole = FoilHole(
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
        image=foil_hole_params.image,
    )
    db.add(foil_hole)
    db.commit()
    db.close()


@router.get("/clients/{client_id}/tilt_series/{tilt_series_tag}/tilts")
def get_tilts(client_id: int, tilt_series_tag: str, db=murfey_db):
    res = db.exec(
        select(ClientEnvironment, TiltSeries, Tilt)
        .where(ClientEnvironment.client_id == client_id)
        .where(TiltSeries.tag == tilt_series_tag)
        .where(TiltSeries.session_id == ClientEnvironment.session_id)
        .where(Tilt.tilt_series_id == TiltSeries.id)
    ).all()
    tilts: Dict[str, List[str]] = {}
    for el in res:
        if tilts.get(el[1].rsync_source):
            tilts[el[1].rsync_source].append(el[2].movie_path)
        else:
            tilts[el[1].rsync_source] = [el[2].movie_path]
    return tilts


@router.get("/instruments/{instrument_name}/visits_raw", response_model=List[Visit])
def get_current_visits(instrument_name: str, db=murfey.server.ispyb.DB):
    return murfey.server.ispyb.get_all_ongoing_visits(instrument_name, db)


@router.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str):
    microscope = get_microscope()
    query = [
        BLSession(
            proposalId=1,
            beamLineName=microscope,
            endDate=datetime.datetime.now() + datetime.timedelta(days=1),
            startDate=datetime.datetime.now(),
            visitNumber=1,
        )
    ]
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


@router.post("/feedback")
async def send_murfey_message(msg: RegistrationMessage):
    pass


class Tag(BaseModel):
    tag: str


@router.post("/visits/{visit_name}/{session_id}/flush_spa_processing")
def flush_spa_processing(
    visit_name: str, session_id: MurfeySessionID, tag: Tag, db=murfey_db
):
    stashed_files = db.exec(
        select(PreprocessStash).where(PreprocessStash.session_id == session_id)
    ).all()
    if not stashed_files:
        return
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob, AutoProcProgram)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == tag.tag)
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
    proc_params = dict(params[0])
    feedback_params = params[1]
    if not proc_params:
        visit_name = visit_name.replace("\r\n", "").replace("\n", "")
        log.warning(
            f"No SPA processing parameters found for Murfey session {sanitise(str(session_id))} on visit {sanitise(visit_name)}"
        )
        return

    detached_ids = [c.id for c in collected_ids]
    try:
        instrument_name = (
            db.exec(select(Session).where(Session.id == session_id))
            .one()
            .instrument_name
        )
    except Exception:
        log.error(
            f"Unable to find a Murfey session associated with session ID {sanitise(str(session_id))}"
        )
        return

    # Load the security config
    security_config_file = machine_config[instrument_name].security_configuration_path
    if not security_config_file:
        log.error(
            f"No security configuration file set for instrument {instrument_name!r}"
        )
        return
    security_config = security_from_file(security_config_file)

    murfey_ids = _murfey_id(
        detached_ids[3], db, number=2 * len(stashed_files), close=False
    )
    feedback_params.picker_murfey_id = murfey_ids[1]
    db.add(feedback_params)
    for i, f in enumerate(stashed_files):
        p = Path(f.mrc_out)
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        movie = Movie(
            murfey_id=murfey_ids[2 * i],
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
            foil_hole_id=f.foil_hole_id,
        )
        db.add(movie)
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": security_config.feedback_queue,
                "node_creator_queue": machine_config[
                    instrument_name
                ].node_creator_queue,
                "dcid": detached_ids[1],
                "autoproc_program_id": detached_ids[3],
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params["angpix"],
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[2 * i],
                "ft_bin": proc_params["motion_corr_binning"],
                "fm_dose": proc_params["dose_per_frame"],
                "gain_ref": (
                    str(
                        machine_config[instrument_name].rsync_basepath
                        / proc_params["gain_ref"]
                    )
                    if proc_params["gain_ref"]
                    else proc_params["gain_ref"]
                ),
                "picker_uuid": murfey_ids[2 * i + 1],
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            },
        }
        log.info(f"Launching SPA preprocessing with Zoaclo message: {zocalo_message}")
        db.delete(f)
    db.commit()

    return


class Source(BaseModel):
    rsync_source: str


@router.get("/version")
def get_version(client_version: str = ""):
    result = {
        "server": murfey.__version__,
        "oldest-supported-client": murfey.__supported_client_version__,
    }

    if client_version:
        client = packaging.version.parse(client_version)
        server = packaging.version.parse(murfey.__version__)
        minimum_version = packaging.version.parse(murfey.__supported_client_version__)
        result["client-needs-update"] = minimum_version > client
        result["client-needs-downgrade"] = client > server

    return result


@router.get("/shutdown", include_in_schema=False)
def shutdown():
    """A method to stop the server. This should be removed before Murfey is
    deployed in production. To remove it we need to figure out how to control
    to process (eg. systemd) and who to run it as."""
    log.info("Server shutdown request received")
    _shutdown()
    return {"success": True}


@router.post("/visits/{visit_name}/{session_id}/register_data_collection_group")
def register_dc_group(
    visit_name: str,
    session_id: MurfeySessionID,
    dcg_params: DCGroupParameters,
    db=murfey_db,
):
    log.info(f"Registering data collection group on microscope {get_microscope()}")
    if dcg_murfey := db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollectionGroup.tag == dcg_params.tag)
    ).all():
        dcg_murfey[0].atlas = dcg_params.atlas
        dcg_murfey[0].sample = dcg_params.sample
        db.add(dcg_murfey[0])
        db.commit()
    else:
        dcgid = next(global_counter)
        murfey_dcg = DataCollectionGroup(
            id=dcgid,
            session_id=session_id,
            tag=dcg_params.tag,
            atlas=dcg_params.atlas,
            sample=dcg_params.sample,
        )
        db.add(murfey_dcg)
        db.commit()

        if dcg_params.experiment_type == "single particle":
            dcid = next(global_counter)
            murfey_dc = DataCollection(
                id=dcid,
                tag=dcg_params.tag,
                dcg_id=dcgid,
            )
            db.add(murfey_dc)
            db.commit()

            pjids = [next(global_counter) for _ in range(4)]

            murfey_pj_pre = ProcessingJob(
                id=pjids[0], recipe="em-spa-preprocess", dc_id=dcid
            )
            murfey_pj_ext = ProcessingJob(
                id=pjids[1], recipe="em-spa-extract", dc_id=dcid
            )
            murfey_pj_2d = ProcessingJob(
                id=pjids[2], recipe="em-spa-class2d", dc_id=dcid
            )
            murfey_pj_3d = ProcessingJob(
                id=pjids[3], recipe="em-spa-class3d", dc_id=dcid
            )
            db.add(murfey_pj_pre)
            db.add(murfey_pj_ext)
            db.add(murfey_pj_2d)
            db.add(murfey_pj_3d)
            db.commit()

            murfey_app_pre = AutoProcProgram(id=next(global_counter), pj_id=pjids[0])
            murfey_app_ext = AutoProcProgram(id=next(global_counter), pj_id=pjids[1])
            murfey_app_2d = AutoProcProgram(id=next(global_counter), pj_id=pjids[2])
            murfey_app_3d = AutoProcProgram(id=next(global_counter), pj_id=pjids[3])
            db.add(murfey_app_pre)
            db.add(murfey_app_ext)
            db.add(murfey_app_2d)
            db.add(murfey_app_3d)
            db.commit()

    if dcg_params.atlas:
        _flush_grid_square_records(
            {"session_id": session_id, "tag": dcg_params.tag}, demo=True
        )
    return dcg_params


@router.post("/visits/{visit_name}/{session_id}/start_data_collection")
def start_dc(
    visit_name: str, session_id: MurfeySessionID, dc_params: DCParameters, db=murfey_db
) -> Optional[DCParameters]:
    dcg_tag = dc_params.source.replace("\r\n", "").replace("\n", "")
    log.info(
        f"Starting data collection, data collection group tag {dcg_tag} and data collection tag {sanitise(dc_params.tag)}"
    )
    dcg = db.exec(
        select(DataCollectionGroup)
        .where(DataCollectionGroup.tag == dcg_tag)
        .where(DataCollectionGroup.session_id == session_id)
    ).one()
    dc_tag = dc_params.tag
    if db.exec(
        select(DataCollection)
        .where(DataCollection.tag == dc_tag)
        .where(DataCollection.dcg_id == dcg.id)
    ).all():
        return None
    dc_id = next(global_counter)
    murfey_dc = DataCollection(
        id=dc_id,
        tag=dc_tag,
        dcg_id=dcg.id,
    )
    db.add(murfey_dc)
    db.commit()
    pj_id_preproc = next(global_counter)
    pj_id_align = next(global_counter)
    murfey_pj = ProcessingJob(
        id=pj_id_preproc,
        recipe="em-tomo-preprocess",
        dc_id=dc_id,
    )
    db.add(murfey_pj)
    murfey_pj = ProcessingJob(
        id=pj_id_align,
        recipe="em-tomo-align",
        dc_id=dc_id,
    )
    db.add(murfey_pj)
    murfey_app = AutoProcProgram(id=pj_id_preproc, pj_id=pj_id_preproc)
    db.add(murfey_app)
    murfey_app = AutoProcProgram(id=pj_id_align, pj_id=pj_id_align)
    db.add(murfey_app)
    db.commit()
    db.close()
    if dc_params.exposure_time:
        prom.exposure_time.set(dc_params.exposure_time)
    return dc_params


@router.post("/visits/{visit_name}/{session_id}/register_processing_job")
def register_proc(
    visit_name, session_id: MurfeySessionID, proc_params: ProcessingJobParameters
):
    # This should probably do something
    log.info("Registering processing job")
    log.info("Processing job registered")
    return proc_params


@router.get("/new_client_id/")
async def new_client_id(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    if not clients:
        return {"new_id": 0}
    sorted_ids = sorted([c.client_id for c in clients])
    return {"new_id": sorted_ids[-1] + 1}


@router.get("/clients")
async def get_clients(db=murfey_db):
    clients = db.exec(select(ClientEnvironment)).all()
    return clients


@router.get("/sessions")
async def get_sessions(db=murfey_db):
    sessions = db.exec(select(Session)).all()
    clients = db.exec(select(ClientEnvironment)).all()
    res = []
    for sess in sessions:
        r = {"session": sess, "clients": []}
        for cl in clients:
            if cl.session_id == sess.id:
                r["clients"].append(cl)
        res.append(r)
    return res


@router.get("/instruments/{instrument_name}/visits/{visit_name}/sessions")
def get_sessions_with_visit(
    instrument_name: str, visit_name: str, db=murfey_db
) -> List[Session]:
    sessions = db.exec(
        select(Session)
        .where(Session.instrument_name == instrument_name)
        .where(Session.visit == visit_name)
    ).all()
    return sessions


@router.get("/instruments/{instrument_name}/sessions")
async def get_sessions_by_instrument_name(
    instrument_name: str, db=murfey_db
) -> List[Session]:
    sessions = db.exec(
        select(Session).where(Session.instrument_name == instrument_name)
    ).all()
    return sessions


@router.delete("/clients/{client_id}/session")
def remove_session(client_id: int, db=murfey_db):
    client = db.exec(
        select(ClientEnvironment).where(ClientEnvironment.client_id == client_id)
    ).one()
    session_id = client.session_id
    client.session_id = None
    db.add(client)
    db.commit()
    if session_id is None:
        return
    prom.monitoring_switch.remove(client.visit)
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.client_id == client_id)
    ).all()
    for ri in rsync_instances:
        prom.seen_files.remove(ri.source, client.visit)
        prom.transferred_files.remove(ri.source, client.visit)
        prom.transferred_files_bytes.remove(ri.source, client.visit)
    collected_ids = db.exec(
        select(DataCollectionGroup, DataCollection, ProcessingJob)
        .where(DataCollectionGroup.session_id == session_id)
        .where(DataCollection.dcg_id == DataCollectionGroup.id)
        .where(ProcessingJob.dc_id == DataCollection.id)
    ).all()
    for c in collected_ids:
        try:
            prom.preprocessed_movies.remove(c[2].id)
        except KeyError:
            continue
    if db.exec(
        select(ClientEnvironment).where(ClientEnvironment.session_id == session_id)
    ).all():
        return
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    return


@router.get("/sessions/{session_id}/rsyncers", response_model=List[RsyncInstance])
def get_rsyncers_for_session(
    session_id: MurfeySessionID, db=murfey_db
) -> List[RsyncInstance]:
    rsync_instances = db.exec(
        select(RsyncInstance).where(RsyncInstance.session_id == session_id)
    )
    return rsync_instances.all()


@router.delete("/sessions/{session_id}")
def remove_session_by_id(session_id: MurfeySessionID, db=murfey_db):
    session = db.exec(select(Session).where(Session.id == session_id)).one()
    db.delete(session)
    db.commit()
    return


@router.post("/visits/{visit_name}/monitoring/{on}")
def change_monitoring_status(visit_name: str, on: int):
    prom.monitoring_switch.labels(visit=visit_name)
    prom.monitoring_switch.labels(visit=visit_name).set(on)


@router.get("/sessions/{session_id}/upstream_visits")
def find_upstream_visits(session_id: MurfeySessionID, db=murfey_db):
    murfey_session = db.exec(select(Session).where(Session.id == session_id)).one()
    visit_name = murfey_session.visit
    instrument_name = murfey_session.instrument_name
    upstream_visits = {}
    for p in machine_config[instrument_name].upstream_data_directories:
        for v in Path(p).glob(f"{visit_name.split('-')[0]}-*"):
            upstream_visits[v.name] = (
                v / machine_config[instrument_name].processed_directory_name
            )
    return upstream_visits


def _get_upstream_tiff_dirs(visit_name: str, instrument_name: str) -> List[Path]:
    tiff_dirs = []
    for directory_name in machine_config[instrument_name].upstream_data_tiff_locations:
        for p in machine_config[instrument_name].upstream_data_directories:
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
    upstream_tiff_paths = []
    instrument_name = (
        db.exec(select(Session).where(Session.id == session_id)).one().instrument_name
    )
    tiff_dirs = _get_upstream_tiff_dirs(visit_name, instrument_name)
    if not tiff_dirs:
        return None
    for tiff_dir in tiff_dirs:
        for f in tiff_dir.glob("**/*.tiff"):
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
