from __future__ import annotations

import datetime
import logging
from functools import lru_cache
from pathlib import Path
from typing import List

import packaging.version
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from ispyb.sqlalchemy import BLSession, Proposal
from pydantic import BaseSettings
from werkzeug.utils import secure_filename

import murfey.server.bootstrap
import murfey.server.ispyb
import murfey.server.websocket as ws
from murfey.server import _transport_object, get_hostname, get_microscope
from murfey.server import shutdown as _shutdown
from murfey.server import templates
from murfey.server.config import MachineConfig, from_file
from murfey.server.gain import Camera, prepare_gain
from murfey.util.models import (
    ConnectionFileParameters,
    ContextInfo,
    DCGroupParameters,
    DCParameters,
    File,
    GainReference,
    ProcessFile,
    ProcessingJobParameters,
    RegistrationMessage,
    SPAProcessingParameters,
    SuggestedPathParameters,
    TiltSeries,
    Visit,
)

log = logging.getLogger("murfey.server.api")


class Settings(BaseSettings):
    murfey_machine_configuration: str = ""


settings = Settings()

machine_config: MachineConfig = MachineConfig(
    acquisition_software=[],
    calibrations={},
    data_directories={},
    rsync_basepath=Path("dls/tmp"),
)
if settings.murfey_machine_configuration:
    microscope = get_microscope()
    machine_config = from_file(Path(settings.murfey_machine_configuration), microscope)

router = APIRouter()

# This will be the homepage for a given microscope.
@router.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
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


@lru_cache(maxsize=1)
@router.get("/machine/")
def machine_info():
    if settings.murfey_machine_configuration:
        microscope = get_microscope()
        print(from_file(settings.murfey_machine_configuration, microscope))
        return from_file(settings.murfey_machine_configuration, microscope)
    return {}


@router.get("/microscope/")
def get_mic():
    microscope = get_microscope()
    return {"microscope": microscope}


@router.get("/visits/")
def all_visit_info(request: Request, db=murfey.server.ispyb.DB):
    microscope = get_microscope()
    visits = murfey.server.ispyb.get_all_ongoing_visits(microscope, db)

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
            f"{len(visits)} visits active for {microscope=}: {', '.join(v.name for v in visits)}"
        )
        return templates.TemplateResponse(
            "activevisits.html",
            {"request": request, "info": return_query, "microscope": microscope},
        )
    else:
        log.debug(f"No visits identified for {microscope=}")
        return templates.TemplateResponse(
            "activevisits.html",
            {"request": request, "info": [], "microscope": microscope},
        )


@router.get("/demo/visits_raw", response_model=List[Visit])
def get_current_visits_demo(db=murfey.server.ispyb.DB):
    microscope = "m12"
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@router.get("/visits_raw", response_model=List[Visit])
def get_current_visits(db=murfey.server.ispyb.DB):
    microscope = get_microscope()
    return murfey.server.ispyb.get_all_ongoing_visits(microscope, db)


@router.get("/visits/{visit_name}")
def visit_info(request: Request, visit_name: str, db=murfey.server.ispyb.DB):
    microscope = get_microscope()
    query = (
        db.query(BLSession)
        .join(Proposal)
        .filter(
            BLSession.proposalId == Proposal.proposalId,
            BLSession.beamLineName == microscope,
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
            "visit.html",
            {"request": request, "visit": return_query},
        )
    else:
        return None


@router.post("/visits/{visit_name}/context")
async def register_context(context_info: ContextInfo):
    log.info(
        f"Context {context_info.experiment_type}:{context_info.acquisition_software} registered"
    )
    await ws.manager.broadcast(f"Context registered: {context_info}")
    await ws.manager.set_state("experiment_type", context_info.experiment_type)
    await ws.manager.set_state(
        "acquisition_software", context_info.acquisition_software
    )


@router.post("/visits/{visit_name}/files")
async def add_file(file: File):
    message = f"File {file} transferred"
    log.info(message)
    await ws.manager.broadcast(f"File {file} transferred")
    return file


@router.post("/feedback")
async def send_murfey_message(msg: RegistrationMessage):
    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue, {"register": msg.registration}
        )


@router.post("/visits/{visit_name}/spa_processing")
async def request_spa_processing(visit_name: str, proc_params: SPAProcessingParameters):
    zocalo_message = {
        "parameters": {"ispyb_process": proc_params.job_id},
        "recipes": ["relion"],
    }
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message)


@router.post("/visits/{visit_name}/tomography_preprocess")
async def request_tomography_preprocessing(visit_name: str, proc_file: ProcessFile):
    visit_idx = Path(proc_file.path).parts.index(visit_name)
    core = Path(*Path(proc_file.path).parts[: visit_idx + 1])
    ppath = Path(proc_file.path)
    sub_dataset = "/".join(ppath.relative_to(core).parts[:-1])
    mrc_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / "MotionCorr"
        / str(ppath.stem + "_motion_corrected.mrc")
    )
    ctf_out = (
        core
        / machine_config.processed_directory_name
        / sub_dataset
        / "CTF"
        / str(ppath.stem + "_ctf.mrc")
    )
    if not mrc_out.parent.exists():
        mrc_out.parent.mkdir(parents=True)
    if not ctf_out.parent.exists():
        ctf_out.parent.mkdir(parents=True)
    zocalo_message = {
        "recipes": ["em-tomo-preprocess"],
        "parameters": {
            "feedback_queue": machine_config.feedback_queue,
            "dcid": proc_file.data_collection_id,
            # "timestamp": datetime.datetime.now(),
            "autoproc_program_id": proc_file.autoproc_program_id,
            "movie": proc_file.path,
            "mrc_out": str(mrc_out),
            "pix_size": (proc_file.pixel_size) * 10**10,
            "output_image": str(ctf_out),
            "image_number": proc_file.image_number,
            "microscope": get_microscope(),
            "mc_uuid": proc_file.mc_uuid,
            "ft_bin": proc_file.mc_binning,
            "fm_dose": proc_file.dose_per_frame,
            "gain_ref": str(machine_config.rsync_basepath / proc_file.gain_ref)
            if proc_file.gain_ref
            else proc_file.gain_ref,
        },
    }
    # log.info(f"Sending Zocalo message {zocalo_message}")
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message)
    else:
        log.error(
            f"Pe-processing was requested for {ppath.name} but no Zocalo transport object was found"
        )
        return proc_file
    # await ws.manager.broadcast(f"Pre-processing requested for {ppath.name}")
    return proc_file


@router.post("/visits/{visit_name}/align")
async def request_tilt_series_alignment(tilt_series: TiltSeries):
    stack_file = (
        Path(tilt_series.motion_corrected_path).parents[1]
        / "align_output"
        / f"{tilt_series.name}_stack.mrc"
    )
    if not stack_file.parent.exists():
        stack_file.parent.mkdir(parents=True)
    zocalo_message = {
        "recipes": ["em-tomo-align"],
        "parameters": {
            "input_file_list": tilt_series.file_tilt_list,
            "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
            "dcid": tilt_series.dcid,
            "appid": tilt_series.autoproc_program_id,
            "stack_file": str(stack_file),
            "pix_size": tilt_series.pixel_size,
            "manual_tilt_offset": tilt_series.manual_tilt_offset,
        },
    }
    if _transport_object:
        log.info(f"Sending Zocalo message {zocalo_message}")
        _transport_object.send("processing_recipe", zocalo_message)
    else:
        log.error(
            f"Processing was requested for tilt series {tilt_series.name} but no Zocalo transport object was found"
        )
        return tilt_series
    await ws.manager.broadcast(
        f"Processing requested for tilt series {tilt_series.name}"
    )

    return tilt_series


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


@router.post("/visits/{visit_name}/suggested_path")
def suggest_path(visit_name, params: SuggestedPathParameters):
    count: int | None = None
    secure_path_parts = [secure_filename(p) for p in params.base_path.parts]
    base_path = "/".join(secure_path_parts)
    check_path = (
        machine_config.rsync_basepath / base_path
        if machine_config
        else Path(f"/dls/{get_microscope()}") / base_path
    )
    check_path_name = check_path.name
    while check_path.exists():
        count = count + 1 if count else 2
        check_path = check_path.parent / f"{check_path_name}{count}"
    if params.touch:
        check_path.mkdir()
    return {"suggested_path": check_path.relative_to(machine_config.rsync_basepath)}


@router.post("/visits/{visit_name}/register_data_collection_group")
def register_dc_group(visit_name, dcg_params: DCGroupParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    log.info(f"Registering data collection group on microscope {get_microscope()}")
    dcg_parameters = {
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=get_microscope(),
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
        "start_time": str(datetime.datetime.now()),
        "experiment_type": dcg_params.experiment_type,
        "experiment_type_id": dcg_params.experiment_type_id,
        "tag": dcg_params.tag,
    }

    if _transport_object:
        _transport_object.send(
            machine_config.feedback_queue, {"register": "data_collection_group", **dcg_parameters}  # type: ignore
        )
    return dcg_parameters


@router.post("/visits/{visit_name}/start_data_collection")
def start_dc(visit_name, dc_params: DCParameters):
    ispyb_proposal_code = visit_name[:2]
    ispyb_proposal_number = visit_name.split("-")[0][2:]
    ispyb_visit_number = visit_name.split("-")[-1]
    log.info(f"Starting data collection on microscope {get_microscope()}")
    dc_parameters = {
        "visit": visit_name,
        "session_id": murfey.server.ispyb.get_session_id(
            microscope=get_microscope(),
            proposal_code=ispyb_proposal_code,
            proposal_number=ispyb_proposal_number,
            visit_number=ispyb_visit_number,
            db=murfey.server.ispyb.Session(),
        ),
        "image_directory": dc_params.image_directory,
        "start_time": str(datetime.datetime.now()),
        "voltage": dc_params.voltage,
        "pixel_size": str(float(dc_params.pixel_size_on_image) * 1e9),
        "image_suffix": dc_params.file_extension,
        "experiment_type": dc_params.experiment_type,
        "image_size_x": dc_params.image_size_x,
        "image_size_y": dc_params.image_size_y,
        "acquisition_software": dc_params.acquisition_software,
        "tag": dc_params.tag,
        "source": dc_params.source,
    }

    if _transport_object:
        log.debug(
            f"Send registration message to {machine_config.feedback_queue}: {dc_parameters}"
        )
        _transport_object.send(
            machine_config.feedback_queue,
            {"register": "data_collection", **dc_parameters},
        )
    return dc_params


@router.post("/visits/{visit_name}/register_processing_job")
def register_proc(visit_name, proc_params: ProcessingJobParameters):
    proc_parameters = {
        "recipe": proc_params.recipe,
        "tag": proc_params.tag,
        "job_parameters": proc_params.parameters,
    }

    if _transport_object:
        log.info(
            f"Send processing registration message to {machine_config.feedback_queue}: {proc_parameters}"
        )
        _transport_object.send(
            machine_config.feedback_queue,
            {"register": "processing_job", **proc_parameters},
        )
    return proc_params


@router.post("/visits/{visit_name}/write_connections_file")
def write_conn_file(visit_name, params: ConnectionFileParameters):
    filepath = (
        Path(machine_config["rsync_basepath"])
        / (machine_config.get("rsync_module") or "data")
        / str(datetime.datetime.now().year)
        / secure_filename(visit_name)
    )
    with open(filepath / secure_filename(params.filename), "w") as f:
        for d in params.destinations:
            f.write(f"{d}\n")


@router.post("/visits/{visit_name}/process_gain")
async def process_gain(visit_name, gain_reference_params: GainReference):
    camera = getattr(Camera, machine_config.camera)
    executables = machine_config.external_executables
    new_gain_ref = await prepare_gain(
        camera, gain_reference_params.gain_ref, executables
    )
    return {"gain_ref": new_gain_ref}
