from __future__ import annotations

import argparse
import logging
import math
import os
import subprocess
import time
from datetime import datetime
from functools import partial, singledispatch, wraps
from pathlib import Path
from threading import Thread
from typing import Any, Callable, Dict, List, NamedTuple, Tuple

import mrcfile
import numpy as np
import uvicorn
import workflows
import zocalo.configuration
from fastapi import Request
from fastapi.templating import Jinja2Templates
from ispyb.sqlalchemy._auto_db_schema import (
    AutoProcProgram,
    Base,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
)
from rich.logging import RichHandler
from sqlalchemy import func
from sqlalchemy.exc import (
    InvalidRequestError,
    OperationalError,
    PendingRollbackError,
    SQLAlchemyError,
)
from sqlalchemy.orm.exc import ObjectDeletedError
from sqlmodel import Session, create_engine, select
from werkzeug.utils import secure_filename

import murfey
import murfey.server.prometheus as prom
import murfey.server.websocket
from murfey.client.contexts.tomo import _midpoint
from murfey.server.config import (
    MachineConfig,
    get_hostname,
    get_machine_config,
    get_microscope,
)
from murfey.server.murfey_db import url  # murfey_db

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass
import murfey.util.db as db
from murfey.util.spa_params import default_spa_parameters
from murfey.util.state import global_state

try:
    from importlib.resources import files  # type: ignore
except ImportError:
    # Fallback for Python 3.8
    from importlib_resources import files  # type: ignore

logger = logging.getLogger("murfey.server")

template_files = files("murfey") / "templates"
templates = Jinja2Templates(directory=template_files)

_running_server: uvicorn.Server | None = None
_transport_object: TransportManager | None = None

try:
    _url = url(get_machine_config())
    engine = create_engine(_url)
    murfey_db = Session(engine, expire_on_commit=False)
except Exception:
    murfey_db = None


class ExtendedRecord(NamedTuple):
    record: Base
    record_params: List[Base]


class JobIDs(NamedTuple):
    dcgid: int
    dcid: int
    pid: int
    appid: int
    client_id: int


def record_failure(
    f: Callable, record_queue: str = "", is_callback: bool = True
) -> Callable:
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            logger.warning(f"Call to {f} failed", exc_info=True)
            if _transport_object and is_callback:
                if not record_queue:
                    machine_config = get_machine_config()
                    record_queue = (
                        machine_config.failure_queue
                        or f"dlq.{machine_config.feedback_queue}"
                    )
                _transport_object.send(record_queue, args[0], new_connection=True)
            return None

    return wrapper


def sanitise(in_string: str) -> str:
    return in_string.replace("\r\n", "").replace("\n", "")


def sanitise_path(in_path: Path) -> Path:
    return Path("/".join(secure_filename(p) for p in in_path.parts))


def get_angle(tilt_file_name: str) -> float:
    for p in Path(tilt_file_name).name.split("_"):
        if "." in p:
            return float(p)
    raise ValueError(f"Tilt angle not found for file {tilt_file_name}")


def check_tilt_series_mc(tilt_series_id: int) -> bool:
    results = murfey_db.exec(
        select(db.Tilt, db.TiltSeries)
        .where(db.Tilt.tilt_series_id == db.TiltSeries.id)
        .where(db.TiltSeries.id == tilt_series_id)
    ).all()
    return (
        all(r[0].motion_corrected for r in results)
        and len(results) == results[0][1].tilt_series_length
    )


def get_all_tilts(tilt_series_id: int) -> List[str]:
    machine_config = get_machine_config()
    results = murfey_db.exec(
        select(db.Tilt).where(db.Tilt.tilt_series_id == tilt_series_id)
    )

    def _mc_path(mov_path: Path) -> str:
        for p in mov_path.parts:
            if "-" in p and p.startswith(("bi", "nr", "nt", "cm", "sw")):
                visit_name = p
                break
        else:
            raise ValueError(f"No visit found in {mov_path}")
        visit_idx = Path(mov_path).parts.index(visit_name)
        core = Path(*Path(mov_path).parts[: visit_idx + 1])
        ppath = Path(mov_path)
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
        return str(mrc_out)

    return [_mc_path(Path(r.movie_path)) for r in results]


def get_job_ids(tilt_series_id: int, appid: int) -> JobIDs:
    results = murfey_db.exec(
        select(
            db.TiltSeries,
            db.AutoProcProgram,
            db.ProcessingJob,
            db.DataCollection,
            db.DataCollectionGroup,
            db.ClientEnvironment,
        )
        .where(db.TiltSeries.id == tilt_series_id)
        .where(db.DataCollection.tag == db.TiltSeries.tag)
        .where(db.ProcessingJob.id == db.AutoProcProgram.pj_id)
        .where(db.AutoProcProgram.id == appid)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
        .where(db.DataCollectionGroup.id == db.DataCollection.dcg_id)
        .where(db.ClientEnvironment.session_id == db.TiltSeries.session_id)
    ).all()
    return JobIDs(
        dcgid=results[0][4].id,
        dcid=results[0][3].id,
        pid=results[0][2].id,
        appid=results[0][1].id,
        client_id=results[0][5].client_id,
    )


def get_tomo_proc_params(pj_id: int, *args) -> db.TomographyProcessingParameters:
    results = murfey_db.exec(
        select(db.TomographyProcessingParameters).where(
            db.TomographyProcessingParameters.pj_id == pj_id
        )
    ).one()
    return results


def get_tomo_preproc_params(dcg_id: int, *args) -> db.TomographyPreprocessingParameters:
    results = murfey_db.exec(
        select(db.TomographyPreprocessingParameters).where(
            db.TomographyPreprocessingParameters.dcg_id == dcg_id
        )
    ).one()
    return results


def respond_with_template(
    request: Request, filename: str, parameters: dict[str, Any] | None = None
):
    template_parameters = {
        "hostname": get_hostname(),
        "microscope": get_microscope(),
        "version": murfey.__version__,
    }
    if parameters:
        template_parameters.update(parameters)
    return templates.TemplateResponse(
        request=request, name=filename, context=template_parameters
    )


class LogFilter(logging.Filter):
    """A filter to limit messages going to Graylog"""

    def __repr__(self):
        return "<murfey.server.LogFilter>"

    def __init__(self):
        self._filter_levels = {
            "murfey": logging.DEBUG,
            "ispyb": logging.DEBUG,
            "zocalo": logging.DEBUG,
            "uvicorn": logging.INFO,
            "fastapi": logging.INFO,
            "starlette": logging.INFO,
            "sqlalchemy": logging.INFO,
        }

    @staticmethod
    def install() -> LogFilter:
        logfilter = LogFilter()
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            handler.addFilter(logfilter)
        return logfilter

    def filter(self, record: logging.LogRecord) -> bool:
        logger_name = record.name
        while True:
            if logger_name in self._filter_levels:
                return record.levelno >= self._filter_levels[logger_name]
            if "." not in logger_name:
                return False
            logger_name = logger_name.rsplit(".", maxsplit=1)[0]


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey server")
    parser.add_argument(
        "--host",
        help="Listen for incoming connections on a specific interface (IP address or hostname; default: all)",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        help="Listen for incoming TCP connections on this port (default: 8000)",
        type=int,
        default=8000,
    )
    parser.add_argument(
        "--workers", help="Number of workers for Uvicorn server", type=int, default=2
    )
    parser.add_argument(
        "--demo",
        action="store_true",
    )
    parser.add_argument(
        "--feedback",
        action="store_true",
    )
    parser.add_argument(
        "--temporary",
        action="store_true",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Decrease logging output verbosity",
    )
    verbosity.add_argument(
        "-v",
        "--verbose",
        action="count",
        help="Increase logging output verbosity",
        default=0,
    )

    # setup logging
    zc = zocalo.configuration.from_file()
    zc.activate()

    # Install a log filter to all existing handlers.
    # At this stage this will exclude console loggers, but will cover
    # any Graylog logging set up by the environment activation
    LogFilter.install()

    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    args = parser.parse_args()

    # Set up Zocalo connection
    if args.demo:
        os.environ["MURFEY_DEMO"] = "1"
    else:
        _set_up_transport(args.transport)

    # Set up logging now that the desired verbosity is known
    _set_up_logging(quiet=args.quiet, verbosity=args.verbose)

    machine_config = get_machine_config()
    if not args.temporary and _transport_object:
        _transport_object.feedback_queue = machine_config.feedback_queue
    rabbit_thread = Thread(
        target=feedback_listen,
        daemon=True,
    )
    logger.info("Starting Murfey RabbitMQ thread")
    if args.feedback:
        rabbit_thread.start()

    logger.info(
        f"Starting Murfey server version {murfey.__version__} for beamline {get_microscope()}, listening on {args.host}:{args.port}"
    )
    global _running_server
    config = uvicorn.Config(
        "murfey.server.main:app",
        host=args.host,
        port=args.port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
        workers=args.workers,
    )

    _running_server = uvicorn.Server(config=config)
    _running_server.run()
    logger.info("Server shutting down")


def shutdown():
    global _running_server
    if _running_server:
        _running_server.should_exit = True
        _running_server.force_exit = True


def _set_up_logging(quiet: bool, verbosity: int):
    rich_handler = RichHandler(enable_link_path=False)
    if quiet:
        rich_handler.setLevel(logging.INFO)
        log_levels = {
            "murfey": logging.INFO,
            "uvicorn": logging.WARNING,
            "fastapi": logging.INFO,
            "starlette": logging.INFO,
            "sqlalchemy": logging.WARNING,
        }
    elif verbosity <= 0:
        rich_handler.setLevel(logging.INFO)
        log_levels = {
            "murfey": logging.DEBUG,
            "uvicorn": logging.INFO,
            "uvicorn.access": logging.WARNING,
            "fastapi": logging.INFO,
            "starlette": logging.INFO,
            "sqlalchemy": logging.WARNING,
        }
    elif verbosity <= 1:
        rich_handler.setLevel(logging.DEBUG)
        log_levels = {
            "": logging.INFO,
            "murfey": logging.DEBUG,
            "uvicorn": logging.INFO,
            "fastapi": logging.INFO,
            "starlette": logging.INFO,
            "sqlalchemy": logging.WARNING,
        }
    elif verbosity <= 2:
        rich_handler.setLevel(logging.DEBUG)
        log_levels = {
            "": logging.INFO,
            "murfey": logging.DEBUG,
            "uvicorn": logging.DEBUG,
            "fastapi": logging.DEBUG,
            "starlette": logging.DEBUG,
            "sqlalchemy": logging.WARNING,
        }
    else:
        rich_handler.setLevel(logging.DEBUG)
        log_levels = {
            "": logging.DEBUG,
            "murfey": logging.DEBUG,
            "uvicorn": logging.DEBUG,
            "fastapi": logging.DEBUG,
            "starlette": logging.DEBUG,
            "sqlalchemy": logging.DEBUG,
        }

    logging.getLogger().addHandler(rich_handler)
    for logger_name, log_level in log_levels.items():
        logging.getLogger(logger_name).setLevel(log_level)


def _set_up_transport(transport_type):
    global _transport_object
    _transport_object = TransportManager(transport_type)


def _murfey_id(app_id: int, _db, number: int = 1, close: bool = True) -> List[int]:
    murfey_ledger = [db.MurfeyLedger(app_id=app_id) for _ in range(number)]
    for ml in murfey_ledger:
        _db.add(ml)
    _db.commit()
    # There is a race condition between the IDs being read back from the database
    # after the insert and the insert being synchronised so allow multiple attempts
    attempts = 0
    while attempts < 100:
        try:
            for m in murfey_ledger:
                _db.refresh(m)
            res = [m.id for m in murfey_ledger if m.id is not None]
            break
        except (ObjectDeletedError, InvalidRequestError):
            pass
        attempts += 1
        time.sleep(0.1)
    else:
        raise RuntimeError(
            "Maximum number of attempts exceeded when producing new Murfey IDs"
        )
    if close:
        _db.close()
    return res


def _murfey_class2ds(
    murfey_ids: List[int], particles_file: str, app_id: int, _db, close: bool = False
):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class2d")
    class2ds = [
        db.Class2D(
            class_number=i,
            particles_file=particles_file,
            pj_id=pj_id,
            murfey_id=mid,
        )
        for i, mid in enumerate(murfey_ids)
    ]
    for c in class2ds:
        _db.add(c)
    _db.commit()
    if close:
        _db.close()


def _murfey_class3ds(murfey_ids: List[int], particles_file: str, app_id: int, _db):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class3d")
    class3ds = [
        db.Class3D(
            class_number=i,
            particles_file=str(Path(particles_file).parent),
            pj_id=pj_id,
            murfey_id=mid,
        )
        for i, mid in enumerate(murfey_ids)
    ]
    for c in class3ds:
        _db.add(c)
    _db.commit()
    _db.close()


def _murfey_refine(murfey_id: int, refine_dir: str, app_id: int, _db):
    pj_id = _pj_id(app_id, _db, recipe="em-spa-refine")
    refine3d = db.Refine3D(
        refine_dir=refine_dir,
        pj_id=pj_id,
        murfey_id=murfey_id,
    )
    _db.add(refine3d)
    _db.commit()
    _db.close()


def _2d_class_murfey_ids(particles_file: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    classes = _db.exec(
        select(db.Class2D).where(
            db.Class2D.particles_file == particles_file and db.Class2D.pj_id == pj_id
        )
    ).all()
    return {str(cl.class_number): cl.murfey_id for cl in classes}


def _3d_class_murfey_ids(particles_file: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    classes = _db.exec(
        select(db.Class3D).where(
            db.Class3D.particles_file == str(Path(particles_file).parent)
            and db.Class3D.pj_id == pj_id
        )
    ).all()
    return {str(cl.class_number): cl.murfey_id for cl in classes}


def _refine_murfey_id(refine_dir: str, app_id: int, _db) -> Dict[str, int]:
    pj_id = (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
        .one()
        .pj_id
    )
    refined_class = _db.exec(
        select(db.Refine3D)
        .where(db.Refine3D.refine_dir == refine_dir)
        .where(db.Refine3D.pj_id == pj_id)
    ).one()
    return refined_class.murfey_id


def _app_id(pj_id: int, _db) -> int:
    return (
        _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.pj_id == pj_id))
        .one()
        .id
    )


def _pj_id(app_id: int, _db, recipe: str = "") -> int:
    if recipe:
        dc_id = (
            _db.exec(
                select(db.AutoProcProgram, db.ProcessingJob)
                .where(db.AutoProcProgram.id == app_id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
            )
            .one()[1]
            .dc_id
        )
        pj_id = (
            _db.exec(
                select(db.ProcessingJob)
                .where(db.ProcessingJob.dc_id == dc_id)
                .where(db.ProcessingJob.recipe == recipe)
            )
            .one()
            .id
        )
    else:
        pj_id = (
            _db.exec(select(db.AutoProcProgram).where(db.AutoProcProgram.id == app_id))
            .one()
            .pj_id
        )
    return pj_id


def _get_spa_params(
    app_id: int, _db
) -> Tuple[db.SPARelionParameters, db.SPAFeedbackParameters]:
    pj_id = _pj_id(app_id, _db, recipe="em-spa-preprocess")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(db.SPARelionParameters.pj_id == pj_id)
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(db.SPAFeedbackParameters.pj_id == pj_id)
    ).one()
    _db.expunge(relion_params)
    _db.expunge(feedback_params)
    return relion_params, feedback_params


def _register_picked_particles_use_diameter(
    message: dict, _db=murfey_db, demo: bool = False
):
    """Received picked particles from the autopick service"""
    # Add this message to the table of seen messages
    params_to_forward = message.get("extraction_parameters")
    assert isinstance(params_to_forward, dict)
    pj_id = _pj_id(message["program_id"], _db)
    ctf_params = db.CtfParameters(
        pj_id=pj_id,
        micrographs_file=params_to_forward["micrographs_file"],
        extract_file=params_to_forward["extract_file"],
        coord_list_file=params_to_forward["coord_list_file"],
        ctf_image=params_to_forward["ctf_values"]["CtfImage"],
        ctf_max_resolution=params_to_forward["ctf_values"]["CtfMaxResolution"],
        ctf_figure_of_merit=params_to_forward["ctf_values"]["CtfFigureOfMerit"],
        defocus_u=params_to_forward["ctf_values"]["DefocusU"],
        defocus_v=params_to_forward["ctf_values"]["DefocusV"],
        defocus_angle=params_to_forward["ctf_values"]["DefocusAngle"],
    )
    _db.add(ctf_params)
    _db.commit()
    _db.close()

    picking_db_len = _db.exec(
        select(func.count(db.ParticleSizes.id)).where(db.ParticleSizes.pj_id == pj_id)
    ).one()
    if picking_db_len > default_spa_parameters.nr_picks_before_diameter:
        # If there are enough particles to get a diameter
        machine_config = get_machine_config()
        relion_params = _db.exec(
            select(db.SPARelionParameters).where(db.SPARelionParameters.pj_id == pj_id)
        ).one()
        relion_options = dict(relion_params)
        feedback_params = _db.exec(
            select(db.SPAFeedbackParameters).where(
                db.SPAFeedbackParameters.pj_id == pj_id
            )
        ).one()

        particle_diameter = relion_params.particle_diameter

        if feedback_params.picker_ispyb_id is None:
            if demo or not _transport_object:
                feedback_params.picker_ispyb_id = 1000
            else:
                assert feedback_params.picker_murfey_id is not None
                feedback_params.picker_ispyb_id = _transport_object.do_buffer_lookup(
                    message["program_id"], feedback_params.picker_murfey_id
                )
                if feedback_params.picker_ispyb_id is not None:
                    _flush_class2d(message["session_id"], message["program_id"], _db)
            _db.add(feedback_params)
            _db.commit()
            selection_stash = _db.exec(
                select(db.SelectionStash).where(db.SelectionStash.pj_id == pj_id)
            ).all()
            for s in selection_stash:
                _register_class_selection(
                    {
                        "session_id": s.session_id,
                        "class_selection_score": s.class_selection_score or 0,
                    },
                    _db=_db,
                    demo=demo,
                )
                _db.delete(s)
                _db.commit()

        if not particle_diameter:
            # If the diameter has not been calculated then find it
            picking_db = _db.exec(
                select(db.ParticleSizes.particle_size).where(
                    db.ParticleSizes.pj_id == pj_id
                )
            ).all()
            particle_diameter = np.quantile(list(picking_db), 0.75)
            relion_params.particle_diameter = particle_diameter
            _db.add(relion_params)
            _db.commit()

            ctf_db = _db.exec(
                select(db.CtfParameters).where(db.CtfParameters.pj_id == pj_id)
            ).all()
            for saved_message in ctf_db:
                # Send on all saved messages to extraction
                _db.expunge(saved_message)
                zocalo_message = {
                    "parameters": {
                        "micrographs_file": saved_message.micrographs_file,
                        "coord_list_file": saved_message.coord_list_file,
                        "output_file": saved_message.extract_file,
                        "pixel_size": (
                            relion_options["angpix"]
                            * relion_options["motion_corr_binning"]
                        ),
                        "ctf_image": saved_message.ctf_image,
                        "ctf_max_resolution": saved_message.ctf_max_resolution,
                        "ctf_figure_of_merit": saved_message.ctf_figure_of_merit,
                        "defocus_u": saved_message.defocus_u,
                        "defocus_v": saved_message.defocus_v,
                        "defocus_angle": saved_message.defocus_angle,
                        "particle_diameter": particle_diameter,
                        "downscale": relion_options["downscale"],
                        "kv": relion_options["voltage"],
                        "feedback_queue": machine_config.feedback_queue,
                        "node_creator_queue": machine_config.node_creator_queue,
                        "session_id": message["session_id"],
                        "autoproc_program_id": _app_id(
                            _pj_id(message["program_id"], _db, recipe="em-spa-extract"),
                            _db,
                        ),
                        "batch_size": default_spa_parameters.batch_size_2d,
                    },
                    "recipes": ["em-spa-extract"],
                }
                if _transport_object:
                    _transport_object.send(
                        "processing_recipe", zocalo_message, new_connection=True
                    )
        else:
            # If the diameter is known then just send the new message
            particle_diameter = relion_params.particle_diameter
            zocalo_message = {
                "parameters": {
                    "micrographs_file": params_to_forward["micrographs_file"],
                    "coord_list_file": params_to_forward["coord_list_file"],
                    "output_file": params_to_forward["extract_file"],
                    "pixel_size": (
                        relion_options["angpix"] * relion_options["motion_corr_binning"]
                    ),
                    "ctf_image": params_to_forward["ctf_values"]["CtfImage"],
                    "ctf_max_resolution": params_to_forward["ctf_values"][
                        "CtfMaxResolution"
                    ],
                    "ctf_figure_of_merit": params_to_forward["ctf_values"][
                        "CtfFigureOfMerit"
                    ],
                    "defocus_u": params_to_forward["ctf_values"]["DefocusU"],
                    "defocus_v": params_to_forward["ctf_values"]["DefocusV"],
                    "defocus_angle": params_to_forward["ctf_values"]["DefocusAngle"],
                    "particle_diameter": particle_diameter,
                    "downscale": relion_options["downscale"],
                    "kv": relion_options["voltage"],
                    "feedback_queue": machine_config.feedback_queue,
                    "node_creator_queue": machine_config.node_creator_queue,
                    "session_id": message["session_id"],
                    "autoproc_program_id": _app_id(
                        _pj_id(message["program_id"], _db, recipe="em-spa-extract"), _db
                    ),
                    "batch_size": default_spa_parameters.batch_size_2d,
                },
                "recipes": ["em-spa-extract"],
            }
            if _transport_object:
                _transport_object.send(
                    "processing_recipe", zocalo_message, new_connection=True
                )
            if demo:
                _register_incomplete_2d_batch(
                    {
                        "session_id": message["session_id"],
                        "program_id": message["program_id"],
                        "class2d_message": {
                            "particles_file": "Select/job009/particles_split_1.star",
                            "class2d_dir": "Class2D",
                            "batch_size": 50000,
                        },
                    },
                    _db=_db,
                    demo=demo,
                )

    else:
        # If not enough particles then save the new sizes
        particle_list = message.get("particle_diameters")
        assert isinstance(particle_list, list)
        for particle in particle_list:
            new_particle = db.ParticleSizes(pj_id=pj_id, particle_size=particle)
            _db.add(new_particle)
            _db.commit()
    _db.close()


def _register_picked_particles_use_boxsize(message: dict, _db=murfey_db):
    """Received picked particles from the autopick service"""
    # Add this message to the table of seen messages
    params_to_forward = message.get("extraction_parameters")
    assert isinstance(params_to_forward, dict)
    machine_config = get_machine_config()
    pj_id = _pj_id(message["program_id"], _db)
    ctf_params = db.CtfParameters(
        pj_id=pj_id,
        micrographs_file=params_to_forward["micrographs_file"],
        coord_list_file=params_to_forward["coord_list_file"],
        extract_file=params_to_forward["extract_file"],
        ctf_image=params_to_forward["ctf_values"]["CtfImage"],
        ctf_max_resolution=params_to_forward["ctf_values"]["CtfMaxResolution"],
        ctf_figure_of_merit=params_to_forward["ctf_values"]["CtfFigureOfMerit"],
        defocus_u=params_to_forward["ctf_values"]["DefocusU"],
        defocus_v=params_to_forward["ctf_values"]["DefocusV"],
        defocus_angle=params_to_forward["ctf_values"]["DefocusAngle"],
    )
    murfey_db.add(ctf_params)
    murfey_db.commit()
    murfey_db.close()

    # Set particle diameter as zero and send box sizes
    relion_params = murfey_db.exec(
        select(db.SPARelionParameters).where(db.SPARelionParameters.pj_id == pj_id)
    ).one()
    feedback_params = murfey_db.exec(
        select(db.SPAFeedbackParameters).where(db.SPAFeedbackParameters.pj_id == pj_id)
    ).one()

    if feedback_params.picker_ispyb_id is None and _transport_object:
        assert feedback_params.picker_murfey_id is not None
        feedback_params.picker_ispyb_id = _transport_object.do_buffer_lookup(
            message["program_id"], feedback_params.picker_murfey_id
        )
        if feedback_params.picker_ispyb_id is not None:
            _flush_class2d(message["session_id"], message["program_id"], _db)
        _db.add(feedback_params)
        _db.commit()
        selection_stash = _db.exec(
            select(db.SelectionStash).where(db.SelectionStash.pj_id == pj_id)
        ).all()
        for s in selection_stash:
            _register_class_selection(
                {
                    "session_id": s.session_id,
                    "class_selection_score": s.class_selection_score or 0,
                },
                _db=_db,
            )
            _db.delete(s)
            _db.commit()

    # Send the message to extraction with the box sizes
    zocalo_message = {
        "parameters": {
            "micrographs_file": params_to_forward["micrographs_file"],
            "coord_list_file": params_to_forward["coord_list_file"],
            "output_file": params_to_forward["extract_file"],
            "pixel_size": relion_params.angpix * relion_params.motion_corr_binning,
            "ctf_image": params_to_forward["ctf_values"]["CtfImage"],
            "ctf_max_resolution": params_to_forward["ctf_values"]["CtfMaxResolution"],
            "ctf_figure_of_merit": params_to_forward["ctf_values"]["CtfFigureOfMerit"],
            "defocus_u": params_to_forward["ctf_values"]["DefocusU"],
            "defocus_v": params_to_forward["ctf_values"]["DefocusV"],
            "defocus_angle": params_to_forward["ctf_values"]["DefocusAngle"],
            "particle_diameter": relion_params.particle_diameter,
            "boxsize": relion_params.boxsize,
            "small_boxsize": relion_params.small_boxsize,
            "downscale": relion_params.downscale,
            "kv": relion_params.voltage,
            "feedback_queue": machine_config.feedback_queue,
            "node_creator_queue": machine_config.node_creator_queue,
            "session_id": message["session_id"],
            "autoproc_program_id": _app_id(
                _pj_id(message["program_id"], _db, recipe="em-spa-extract"), _db
            ),
            "batch_size": default_spa_parameters.batch_size_2d,
        },
        "recipes": ["em-spa-extract"],
    }
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message, new_connection=True)


def _release_2d_hold(message: dict, _db=murfey_db):
    relion_params, feedback_params = _get_spa_params(message["program_id"], _db)
    if not feedback_params.star_combination_job:
        feedback_params.star_combination_job = feedback_params.next_job + (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    if feedback_params.rerun_class2d:
        first_class2d = _db.exec(
            select(db.Class2DParameters).where(db.Class2DParameters.pj_id == pj_id)
        ).first()
        machine_config = get_machine_config()
        zocalo_message = {
            "parameters": {
                "particles_file": first_class2d.particles_file,
                "class2d_dir": message["job_dir"],
                "batch_is_complete": first_class2d.complete,
                "batch_size": first_class2d.batch_size,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "autoproc_program_id": message["program_id"],
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _2d_class_murfey_ids(
                    first_class2d.particles_file, message["program_id"], _db
                ),
                "class2d_grp_uuid": _db.exec(
                    select(db.Class2DParameters)
                    .where(
                        db.Class2DParameters.particles_file
                        == first_class2d.particles_file
                    )
                    .where(db.Class2DParameters.pj_id == pj_id)
                )
                .one()
                .murfey_id,
                "session_id": message["session_id"],
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if first_class2d.complete:
            feedback_params.next_job += (
                4 if default_spa_parameters.do_icebreaker_jobs else 3
            )
        feedback_params.rerun_class2d = False
        _db.add(feedback_params)
        if first_class2d.complete:
            _db.delete(first_class2d)
        _db.commit()
        _db.close()
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
    else:
        feedback_params.hold_class2d = False
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _release_3d_hold(message: dict, _db=murfey_db):
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class3d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    class3d_params = _db.exec(
        select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
    ).one()
    if class3d_params.run:
        machine_config = get_machine_config()
        zocalo_message = {
            "parameters": {
                "particles_file": class3d_params.particles_file,
                "class3d_dir": class3d_params.class3d_dir,
                "batch_size": class3d_params.batch_size,
                "symmetry": relion_params.symmetry,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "do_initial_model": False if feedback_params.initial_model else True,
                "initial_model_file": feedback_params.initial_model,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _3d_class_murfey_ids(
                    class3d_params.particles_file, _app_id(pj_id, _db), _db
                ),
                "class3d_grp_uuid": _db.exec(
                    select(db.Class3DParameters)
                    .where(
                        db.Class3DParameters.particles_file
                        == class3d_params.particles_file
                    )
                    .where(db.Class3DParameters.pj_id == pj_id)
                )
                .one()
                .murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        class3d_params.run = False
        _db.add(class3d_params)
    else:
        feedback_params.hold_class3d = False
    _db.add(feedback_params)
    _db.commit()
    _db.close()


def _release_refine_hold(message: dict, _db=murfey_db):
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    refine_params = _db.exec(
        select(db.RefineParameters).where(db.RefineParameters.pj_id == pj_id)
    ).one()
    if refine_params.run:
        machine_config = get_machine_config()
        zocalo_message = {
            "parameters": {
                "refine_job_dir": refine_params.refine_dir,
                "class3d_dir": refine_params.class3d_dir,
                "class_number": refine_params.class_number,
                "pixel_size": relion_params.angpix,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "node_creator_queue": machine_config.node_creator_queue,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "picker_id": feedback_params.picker_ispyb_id,
                "refined_class_uuid": _refine_murfey_id(
                    refine_params.refine_dir, _app_id(pj_id, _db), _db
                ),
                "refined_grp_uuid": refine_params.murfey_id,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
            },
            "recipes": ["em-spa-refine"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        refine_params.run = False
        _db.add(refine_params)
    else:
        feedback_params.hold_refine = False
    _db.add(feedback_params)
    _db.commit()
    _db.close()


def _register_incomplete_2d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received first batch from particle selection service"""
    # the general parameters are stored using the preprocessing auto proc program ID
    logger.info("Registering incomplete particle batch for 2D classification")
    machine_config = get_machine_config()
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    if feedback_params.hold_class2d:
        feedback_params.rerun_class2d = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()
        return
    feedback_params.next_job = 10 if default_spa_parameters.do_icebreaker_jobs else 7
    feedback_params.hold_class2d = True
    relion_options = dict(relion_params)
    other_options = dict(feedback_params)
    if other_options["picker_ispyb_id"] is None:
        logger.info("No ISPyB particle picker ID yet")
        feedback_params.hold_class2d = False
        _db.add(feedback_params)
        _db.commit()
        _db.expunge(feedback_params)
        return
    _db.add(feedback_params)
    _db.commit()
    _db.expunge(feedback_params)
    class2d_message = message.get("class2d_message")
    assert isinstance(class2d_message, dict)
    if not _db.exec(
        select(func.count(db.Class2DParameters.particles_file))
        .where(db.Class2DParameters.particles_file == class2d_message["particles_file"])
        .where(db.Class2DParameters.pj_id == pj_id)
    ).one():
        class2d_params = db.Class2DParameters(
            pj_id=pj_id,
            murfey_id=_murfey_id(message["program_id"], _db)[0],
            particles_file=class2d_message["particles_file"],
            class2d_dir=class2d_message["class2d_dir"],
            batch_size=class2d_message["batch_size"],
            complete=False,
        )
        _db.add(class2d_params)
        _db.commit()
        murfey_ids = _murfey_id(message["program_id"], _db, number=50)
        _murfey_class2ds(
            murfey_ids, class2d_message["particles_file"], message["program_id"], _db
        )
    zocalo_message = {
        "parameters": {
            "particles_file": class2d_message["particles_file"],
            "class2d_dir": f"{class2d_message['class2d_dir']}{other_options['next_job']:03}",
            "batch_is_complete": False,
            "particle_diameter": relion_options["particle_diameter"],
            "combine_star_job_number": -1,
            "picker_id": other_options["picker_ispyb_id"],
            "nr_iter": default_spa_parameters.nr_iter_2d,
            "batch_size": default_spa_parameters.batch_size_2d,
            "nr_classes": default_spa_parameters.nr_classes_2d,
            "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
            "mask_diameter": 0,
            "class_uuids": _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            ),
            "class2d_grp_uuid": _db.exec(
                select(db.Class2DParameters).where(
                    db.Class2DParameters.particles_file
                    == class2d_message["particles_file"]
                    and db.Class2DParameters.pj_id == pj_id
                )
            )
            .one()
            .murfey_id,
            "session_id": message["session_id"],
            "autoproc_program_id": _app_id(
                _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
            ),
            "feedback_queue": machine_config.feedback_queue,
            "node_creator_queue": machine_config.node_creator_queue,
        },
        "recipes": ["em-spa-class2d"],
    }
    if _transport_object:
        _transport_object.send("processing_recipe", zocalo_message, new_connection=True)
        logger.info("2D classification requested")
    if demo:
        logger.info("Incomplete 2D batch registered in demo mode")
        if not _db.exec(
            select(func.count(db.Class2DParameters.particles_file)).where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
                and db.Class2DParameters.pj_id == pj_id
                and db.Class2DParameters.complete
            )
        ).one():
            _register_complete_2d_batch(message, _db=_db, demo=demo)
            message["class2d_message"]["particles_file"] = (
                message["class2d_message"]["particles_file"] + "_new"
            )
            _register_complete_2d_batch(message, _db=_db, demo=demo)
    _db.close()


def _register_complete_2d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received full batch from particle selection service"""
    machine_config = get_machine_config()
    class2d_message = message.get("class2d_message")
    assert isinstance(class2d_message, dict)
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    _db.expunge(relion_params)
    _db.expunge(feedback_params)
    if feedback_params.hold_class2d or feedback_params.picker_ispyb_id is None:
        feedback_params.rerun_class2d = True
        _db.add(feedback_params)
        _db.commit()
        # If waiting then save the message
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class2d_params = _db.exec(
                select(db.Class2DParameters)
                .where(db.Class2DParameters.pj_id == pj_id)
                .where(
                    db.Class2DParameters.particles_file
                    == class2d_message["particles_file"]
                )
            ).one()
            class2d_params.complete = True
            _db.add(class2d_params)
            _db.commit()
            _db.close()
        else:
            class2d_params = db.Class2DParameters(
                pj_id=pj_id,
                murfey_id=_murfey_id(message["program_id"], _db)[0],
                particles_file=class2d_message["particles_file"],
                class2d_dir=class2d_message["class2d_dir"],
                batch_size=class2d_message["batch_size"],
            )
            _db.add(class2d_params)
            _db.commit()
            _db.close()
            murfey_ids = _murfey_id(_app_id(pj_id, _db), _db, number=50)
            _murfey_class2ds(
                murfey_ids, class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
        if demo:
            _register_class_selection(
                {"session_id": message["session_id"], "class_selection_score": 0.5},
                _db=_db,
                demo=demo,
            )
    elif not feedback_params.class_selection_score:
        # For the first batch, start a container and set the database to wait
        job_number_after_first_batch = (
            10 if default_spa_parameters.do_icebreaker_jobs else 7
        )
        if (
            feedback_params.next_job is not None
            and feedback_params.next_job < job_number_after_first_batch
        ):
            feedback_params.next_job = job_number_after_first_batch
        if not feedback_params.star_combination_job:
            feedback_params.star_combination_job = feedback_params.next_job + (
                3 if default_spa_parameters.do_icebreaker_jobs else 2
            )
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class_uuids = _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
            class2d_grp_uuid = (
                _db.exec(
                    select(db.Class2DParameters)
                    .where(db.Class2DParameters.pj_id == pj_id)
                    .where(
                        db.Class2DParameters.particles_file
                        == class2d_message["particles_file"]
                    )
                )
                .one()
                .murfey_id
            )
        else:
            class_uuids = {
                str(i + 1): m
                for i, m in enumerate(_murfey_id(_app_id(pj_id, _db), _db, number=50))
            }
            class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
        zocalo_message = {
            "parameters": {
                "particles_file": class2d_message["particles_file"],
                "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": class_uuids,
                "class2d_grp_uuid": class2d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "batch_size": default_spa_parameters.batch_size_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_class2d = True
        feedback_params.next_job += (
            4 if default_spa_parameters.do_icebreaker_jobs else 3
        )
        _db.add(feedback_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        if _db.exec(
            select(func.count(db.Class2DParameters.particles_file))
            .where(db.Class2DParameters.pj_id == pj_id)
            .where(
                db.Class2DParameters.particles_file == class2d_message["particles_file"]
            )
        ).one():
            class_uuids = _2d_class_murfey_ids(
                class2d_message["particles_file"], _app_id(pj_id, _db), _db
            )
            class2d_grp_uuid = (
                _db.exec(
                    select(db.Class2DParameters)
                    .where(db.Class2DParameters.pj_id == pj_id)
                    .where(
                        db.Class2DParameters.particles_file
                        == class2d_message["particles_file"]
                    )
                )
                .one()
                .murfey_id
            )
        else:
            class_uuids = {
                str(i + 1): m
                for i, m in enumerate(_murfey_id(_app_id(pj_id, _db), _db, number=50))
            }
            class2d_grp_uuid = _murfey_id(_app_id(pj_id, _db), _db)[0]
        zocalo_message = {
            "parameters": {
                "particles_file": class2d_message["particles_file"],
                "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": class_uuids,
                "class2d_grp_uuid": class2d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "batch_size": default_spa_parameters.batch_size_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class2d"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.next_job += (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _flush_class2d(
    session_id: int,
    app_id: int,
    _db,
    relion_params: db.SPARelionParameters | None = None,
    feedback_params: db.SPAFeedbackParameters | None = None,
):
    machine_config = get_machine_config()
    if not relion_params or feedback_params:
        pj_id_params = _pj_id(app_id, _db, recipe="em-spa-preprocess")
    if not relion_params:
        relion_params = _db.exec(
            select(db.SPARelionParameters).where(
                db.SPARelionParameters.pj_id == pj_id_params
            )
        ).one()
        _db.expunge(relion_params)
    if not feedback_params:
        feedback_params = _db.exec(
            select(db.SPAFeedbackParameters).where(
                db.SPAFeedbackParameters.pj_id == pj_id_params
            )
        ).one()
        _db.expunge(feedback_params)
    if not relion_params or not feedback_params:
        return
    pj_id = _pj_id(app_id, _db, recipe="em-spa-class2d")
    class2d_db = _db.exec(
        select(db.Class2DParameters)
        .where(db.Class2DParameters.pj_id == pj_id)
        .where(db.Class2DParameters.complete)
    ).all()
    if not feedback_params.next_job:
        feedback_params.next_job = (
            10 if default_spa_parameters.do_icebreaker_jobs else 7
        )
    if not feedback_params.star_combination_job:
        feedback_params.star_combination_job = feedback_params.next_job + (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
    for saved_message in class2d_db:
        # Send all held Class2D messages on with the selection score added
        _db.expunge(saved_message)
        zocalo_message = {
            "parameters": {
                "particles_file": saved_message.particles_file,
                "class2d_dir": f"{saved_message.class2d_dir}{feedback_params.next_job:03}",
                "batch_is_complete": True,
                "batch_size": saved_message.batch_size,
                "particle_diameter": relion_params.particle_diameter,
                "mask_diameter": relion_params.mask_diameter or 0,
                "combine_star_job_number": feedback_params.star_combination_job,
                "autoselect_min_score": feedback_params.class_selection_score or 0,
                "picker_id": feedback_params.picker_ispyb_id,
                "class_uuids": _2d_class_murfey_ids(
                    saved_message.particles_file, _app_id(pj_id, _db), _db
                ),
                "class2d_grp_uuid": saved_message.murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_2d,
                "nr_classes": default_spa_parameters.nr_classes_2d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": session_id,
                "autoproc_program_id": _app_id(pj_id, _db),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class2d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.next_job += (
            3 if default_spa_parameters.do_icebreaker_jobs else 2
        )
        _db.delete(saved_message)
    _db.add(feedback_params)
    _db.commit()


def _register_class_selection(message: dict, _db=murfey_db, demo: bool = False):
    """Received selection score from class selection service"""
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class2d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    class2d_db = _db.exec(
        select(db.Class2DParameters).where(db.Class2DParameters.pj_id == pj_id)
    ).all()
    # Add the class selection score to the database
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    _db.expunge(feedback_params)

    if feedback_params.picker_ispyb_id is None:
        selection_stash = db.SelectionStash(
            pj_id=pj_id,
            class_selection_score=message["class_selection_score"] or 0,
        )
        _db.add(selection_stash)
        _db.commit()
        _db.close()
        return

    feedback_params.class_selection_score = message.get("class_selection_score") or 0
    feedback_params.hold_class2d = False
    next_job = feedback_params.next_job
    if demo:
        for saved_message in class2d_db:
            # Send all held Class2D messages on with the selection score added
            _db.expunge(saved_message)
            particles_file = saved_message.particles_file
            logger.info("Complete 2D classification registered in demo mode")
            _register_3d_batch(
                {
                    "session_id": message["session_id"],
                    "class3d_message": {
                        "particles_file": particles_file,
                        "class3d_dir": "Class3D",
                        "batch_size": 50000,
                    },
                },
                _db=_db,
                demo=demo,
            )
            logger.info("3D classification registered in demo mode")
            _register_3d_batch(
                {
                    "session_id": message["session_id"],
                    "class3d_message": {
                        "particles_file": particles_file + "_new",
                        "class3d_dir": "Class3D",
                        "batch_size": 50000,
                    },
                },
                _db=_db,
                demo=demo,
            )
            _register_initial_model(
                {
                    "session_id": message["session_id"],
                    "initial_model": "InitialModel/job015/model.mrc",
                },
                _db=_db,
                demo=demo,
            )
            next_job += 3 if default_spa_parameters.do_icebreaker_jobs else 2
        feedback_params.next_job = next_job
        _db.close()
    else:
        _flush_class2d(
            message["session_id"],
            message["program_id"],
            _db,
            relion_params=relion_params,
            feedback_params=feedback_params,
        )
    _db.add(feedback_params)
    for sm in class2d_db:
        _db.delete(sm)
    _db.commit()
    _db.close()


def _find_initial_model(visit: str, machine_config: MachineConfig) -> Path | None:
    if machine_config.initial_model_search_directory:
        visit_directory = (
            machine_config.rsync_basepath
            / (machine_config.rsync_module or "data")
            / str(datetime.now().year)
            / visit
        )
        possible_models = [
            p
            for p in (
                visit_directory / machine_config.initial_model_search_directory
            ).glob("*.mrc")
            if "rescaled" not in p.name
        ]
        if possible_models:
            return sorted(possible_models, key=lambda x: x.stat().st_ctime)[-1]
    return None


def _downscaled_box_size(
    particle_diameter: int, pixel_size: float
) -> Tuple[int, float]:
    box_size = int(math.ceil(1.2 * particle_diameter))
    box_size = box_size + box_size % 2
    for small_box_pix in (
        48,
        64,
        96,
        128,
        160,
        192,
        256,
        288,
        300,
        320,
        360,
        384,
        400,
        420,
        450,
        480,
        512,
        640,
        768,
        896,
        1024,
    ):
        # Don't go larger than the original box
        if small_box_pix > box_size:
            return box_size, pixel_size
        # If Nyquist freq. is better than 8.5 A, use this downscaled box, else step size
        small_box_angpix = pixel_size * box_size / small_box_pix
        if small_box_angpix < 4.25:
            return small_box_pix, small_box_angpix
    raise ValueError(f"Box size is too large: {box_size}")


def _resize_intial_model(
    downscaled_box_size: int,
    downscaled_pixel_size: float,
    input_path: Path,
    output_path: Path,
    executables: Dict[str, str],
    env: Dict[str, str],
) -> None:
    if executables.get("relion_image_handler"):
        comp_proc = subprocess.run(
            [
                f"{executables['relion_image_handler']}",
                "--i",
                str(input_path),
                "--new_box",
                str(downscaled_box_size),
                "--rescale_angpix",
                str(downscaled_pixel_size),
                "--o",
                str(output_path),
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        with mrcfile.open(output_path) as rescaled_mrc:
            rescaled_mrc.header.cella = (
                downscaled_pixel_size,
                downscaled_pixel_size,
                downscaled_pixel_size,
            )
        if comp_proc.returncode:
            logger.error(
                f"Resizing initial model {input_path} failed \n {comp_proc.stdout}"
            )
    return None


def _register_3d_batch(message: dict, _db=murfey_db, demo: bool = False):
    """Received 3d batch from class selection service"""
    class3d_message = message.get("class3d_message")
    assert isinstance(class3d_message, dict)
    machine_config = get_machine_config()
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-class3d")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    other_options = dict(feedback_params)

    visit_name = (
        _db.exec(
            select(db.ClientEnvironment).where(
                db.ClientEnvironment.session_id == message["session_id"]
            )
        )
        .one()
        .visit
    )

    provided_initial_model = _find_initial_model(visit_name, machine_config)
    if provided_initial_model and not feedback_params.initial_model:
        rescaled_initial_model_path = (
            provided_initial_model.parent
            / f"{provided_initial_model.stem}_rescaled_{pj_id}{provided_initial_model.suffix}"
        )
        if not rescaled_initial_model_path.is_file():
            _resize_intial_model(
                *_downscaled_box_size(
                    message["particle_diameter"],
                    relion_options["angpix"],
                ),
                provided_initial_model,
                rescaled_initial_model_path,
                machine_config.external_executables,
                machine_config.external_environment,
            )
            feedback_params.initial_model = str(rescaled_initial_model_path)
            other_options["initial_model"] = str(rescaled_initial_model_path)
            next_job = feedback_params.next_job
            class3d_dir = (
                f"{class3d_message['class3d_dir']}{(feedback_params.next_job+1):03}"
            )
            feedback_params.next_job += 1
            _db.add(feedback_params)
            _db.commit()

            class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
            class_uuids = _murfey_id(message["program_id"], _db, number=4)
            class3d_params = db.Class3DParameters(
                pj_id=pj_id,
                murfey_id=class3d_grp_uuid,
                particles_file=class3d_message["particles_file"],
                class3d_dir=class3d_dir,
                batch_size=class3d_message["batch_size"],
            )
            _db.add(class3d_params)
            _db.commit()
            _murfey_class3ds(
                class_uuids,
                class3d_message["particles_file"],
                message["program_id"],
                _db,
            )

    if feedback_params.hold_class3d:
        # If waiting then save the message
        class3d_params = _db.exec(
            select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
        ).one()
        class3d_params.run = True
        class3d_params.particles_file = class3d_message["particles_file"]
        class3d_params.batch_size = class3d_message["batch_size"]
        _db.add(class3d_params)
        _db.commit()
        _db.close()
    elif not feedback_params.initial_model:
        # For the first batch, start a container and set the database to wait
        next_job = feedback_params.next_job
        class3d_dir = (
            f"{class3d_message['class3d_dir']}{(feedback_params.next_job+1):03}"
        )
        class3d_grp_uuid = _murfey_id(message["program_id"], _db)[0]
        class_uuids = _murfey_id(message["program_id"], _db, number=4)
        class3d_params = db.Class3DParameters(
            pj_id=pj_id,
            murfey_id=class3d_grp_uuid,
            particles_file=class3d_message["particles_file"],
            class3d_dir=class3d_dir,
            batch_size=class3d_message["batch_size"],
        )
        _db.add(class3d_params)
        _db.commit()
        _murfey_class3ds(
            class_uuids, class3d_message["particles_file"], message["program_id"], _db
        )

        feedback_params.hold_class3d = True
        next_job += 2
        feedback_params.next_job = next_job
        zocalo_message = {
            "parameters": {
                "particles_file": class3d_message["particles_file"],
                "class3d_dir": class3d_dir,
                "batch_size": class3d_message["batch_size"],
                "symmetry": relion_options["symmetry"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "do_initial_model": True,
                "picker_id": other_options["picker_ispyb_id"],
                "class_uuids": {i + 1: m for i, m in enumerate(class_uuids)},
                "class3d_grp_uuid": class3d_grp_uuid,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        _db.add(feedback_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        class3d_params = _db.exec(
            select(db.Class3DParameters).where(db.Class3DParameters.pj_id == pj_id)
        ).one()
        zocalo_message = {
            "parameters": {
                "particles_file": class3d_message["particles_file"],
                "class3d_dir": class3d_params.class3d_dir,
                "batch_size": class3d_message["batch_size"],
                "symmetry": relion_options["symmetry"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "do_initial_model": False,
                "initial_model_file": other_options["initial_model"],
                "picker_id": other_options["picker_ispyb_id"],
                "class_uuids": _3d_class_murfey_ids(
                    class3d_params.particles_file, _app_id(pj_id, _db), _db
                ),
                "class3d_grp_uuid": class3d_params.murfey_id,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "initial_model_iterations": default_spa_parameters.nr_iter_ini_model,
                "nr_classes": default_spa_parameters.nr_classes_3d,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
                "class2d_fraction_of_classes_to_remove": default_spa_parameters.fraction_of_classes_to_remove_2d,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-class3d"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
            },
            "recipes": ["em-spa-class3d"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_class3d = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _register_initial_model(message: dict, _db=murfey_db, demo: bool = False):
    """Received initial model from 3d classification service"""
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    # Add the initial model file to the database
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    feedback_params.initial_model = message.get("initial_model")
    _db.add(feedback_params)
    _db.commit()
    _db.close()


@record_failure
def _flush_spa_preprocessing(message: dict):
    session_id = (
        murfey_db.exec(
            select(db.ClientEnvironment).where(
                db.ClientEnvironment.client_id == message["client_id"]
            )
        )
        .one()
        .session_id
    )
    stashed_files = murfey_db.exec(
        select(db.PreprocessStash)
        .where(db.PreprocessStash.session_id == session_id)
        .where(db.PreprocessStash.tag == message["tag"])
    ).all()
    if not stashed_files:
        return None
    machine_config = get_machine_config()
    collected_ids = murfey_db.exec(
        select(
            db.DataCollectionGroup,
            db.DataCollection,
            db.ProcessingJob,
            db.AutoProcProgram,
        )
        .where(db.DataCollectionGroup.session_id == session_id)
        .where(db.DataCollectionGroup.tag == message["tag"])
        .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
        .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
        .where(db.ProcessingJob.recipe == "em-spa-preprocess")
    ).one()
    params = murfey_db.exec(
        select(db.SPARelionParameters, db.SPAFeedbackParameters)
        .where(db.SPARelionParameters.pj_id == collected_ids[2].id)
        .where(db.SPAFeedbackParameters.pj_id == db.SPARelionParameters.pj_id)
    ).one()
    proc_params = params[0]
    feedback_params = params[1]
    if not proc_params:
        logger.warning(
            f"No SPA processing parameters found for client processing job ID {collected_ids[2].id}"
        )
        raise ValueError(
            "No processing parameters were foudn in the database when flushing SPA preprocessing"
        )

    murfey_ids = _murfey_id(
        collected_ids[3].id,
        murfey_db,
        number=2 * len(stashed_files),
        close=False,
    )
    if feedback_params.picker_murfey_id is None:
        feedback_params.picker_murfey_id = murfey_ids[1]
        murfey_db.add(feedback_params)

    for i, f in enumerate(stashed_files):
        mrcp = Path(f.mrc_out)
        ppath = Path(f.file_path)
        if not mrcp.parent.exists():
            mrcp.parent.mkdir(parents=True)
        movie = db.Movie(
            murfey_id=murfey_ids[2 * i],
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
            foil_hole_id=f.foil_hole_id,
        )
        murfey_db.add(movie)
        zocalo_message = {
            "recipes": ["em-spa-preprocess"],
            "parameters": {
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": collected_ids[1].id,
                "kv": proc_params.voltage,
                "autoproc_program_id": collected_ids[3].id,
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params.angpix,
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[2 * i],
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "gain_ref": proc_params.gain_ref,
                "picker_uuid": murfey_ids[2 * i + 1],
                "session_id": session_id,
                "particle_diameter": proc_params.particle_diameter or 0,
                "fm_int_file": f.eer_fractionation_file,
                "do_icebreaker_jobs": default_spa_parameters.do_icebreaker_jobs,
            },
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
            murfey_db.delete(f)
        else:
            logger.error(
                f"Pre-processing was requested for {ppath.name} but no Zocalo transport object was found"
            )
    murfey_db.commit()
    murfey_db.close()
    return None


@record_failure
def _flush_tomography_preprocessing(message: dict):
    machine_config = get_machine_config()
    session_id = (
        murfey_db.exec(
            select(db.ClientEnvironment).where(
                db.ClientEnvironment.client_id == message["client_id"]
            )
        )
        .one()
        .session_id
    )
    stashed_files = murfey_db.exec(
        select(db.PreprocessStash)
        .where(db.PreprocessStash.session_id == session_id)
        .where(db.PreprocessStash.group_tag == message["data_collection_group_tag"])
    ).all()
    if not stashed_files:
        return
    collected_ids = murfey_db.exec(
        select(
            db.DataCollectionGroup,
        )
        .where(db.DataCollectionGroup.session_id == session_id)
        .where(db.DataCollectionGroup.tag == message["data_collection_group_tag"])
    ).first()
    proc_params = murfey_db.exec(
        select(db.TomographyPreprocessingParameters).where(
            db.TomographyPreprocessingParameters.dcg_id == collected_ids.id
        )
    ).one()
    if not proc_params:
        visit_name = message["visit_name"].replace("\r\n", "").replace("\n", "")
        logger.warning(
            f"No tomography processing parameters found for client {sanitise(str(message['client_id']))} on visit {sanitise(visit_name)}"
        )
        return

    for f in stashed_files:
        collected_ids = murfey_db.exec(
            select(
                db.DataCollectionGroup,
                db.DataCollection,
                db.ProcessingJob,
                db.AutoProcProgram,
            )
            .where(db.DataCollectionGroup.session_id == session_id)
            .where(db.DataCollectionGroup.tag == message["data_collection_group_tag"])
            .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
            .where(db.DataCollection.tag == f.tag)
            .where(db.ProcessingJob.dc_id == db.DataCollection.id)
            .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
            .where(db.ProcessingJob.recipe == "em-tomo-preprocess")
        ).one()
        detached_ids = [c.id for c in collected_ids]

        murfey_ids = _murfey_id(detached_ids[3], murfey_db, number=1, close=False)
        p = Path(f.mrc_out)
        if not p.parent.exists():
            p.parent.mkdir(parents=True)
        movie = db.Movie(
            murfey_id=murfey_ids[0],
            path=f.file_path,
            image_number=f.image_number,
            tag=f.tag,
        )
        murfey_db.add(movie)
        zocalo_message = {
            "recipes": ["em-tomo-preprocess"],
            "parameters": {
                "feedback_queue": machine_config.feedback_queue,
                "node_creator_queue": machine_config.node_creator_queue,
                "dcid": detached_ids[1],
                "autoproc_program_id": detached_ids[3],
                "movie": f.file_path,
                "mrc_out": f.mrc_out,
                "pixel_size": proc_params.pixel_size,
                "kv": proc_params.voltage,
                "image_number": f.image_number,
                "microscope": get_microscope(),
                "mc_uuid": murfey_ids[0],
                "ft_bin": proc_params.motion_corr_binning,
                "fm_dose": proc_params.dose_per_frame,
                "gain_ref": (
                    str(machine_config.rsync_basepath / proc_params.gain_ref)
                    if proc_params.gain_ref
                    else proc_params.gain_ref
                ),
                "fm_int_file": proc_params.eer_fractionation_file or "",
            },
        }
        logger.info(
            f"Launching tomography preprocessing with Zocalo message: {zocalo_message}"
        )
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        else:
            feedback_callback(
                {},
                {
                    "register": "motion_corrected",
                    "movie": f.file_path,
                    "mrc_out": f.mrc_out,
                    "movie_id": murfey_ids[0],
                    "program_id": detached_ids[3],
                },
            )
        murfey_db.delete(f)
        murfey_db.commit()


def _flush_grid_square_records(message: dict, _db=murfey_db, demo: bool = False):
    tag = message["tag"]
    session_id = message["session_id"]
    gs_ids = []
    for gs in _db.exec(
        select(db.GridSquare)
        .where(db.GridSquare.session_id == session_id)
        .where(db.GridSquare.tag == tag)
    ).all():
        gs_ids.append(gs.id)
        if demo:
            logger.info(f"Flushing grid square {gs.name}")
    for i in gs_ids:
        _flush_foil_hole_records(i, _db=_db, demo=demo)


def _flush_foil_hole_records(grid_square_id: int, _db=murfey_db, demo: bool = False):
    for fh in _db.exec(
        select(db.FoilHole).where(db.FoilHole.grid_square_id == grid_square_id)
    ).all():
        if demo:
            logger.info(f"Flushing foil hole: {fh.name}")


def _register_refinement(message: dict, _db=murfey_db, demo: bool = False):
    """Received class to refine from 3D classification"""
    machine_config = get_machine_config()
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()
    other_options = dict(feedback_params)

    if feedback_params.hold_refine:
        # If waiting then save the message
        refine_params = _db.exec(
            select(db.RefineParameters).where(db.RefineParameters.pj_id == pj_id)
        ).one()
        # refine_params.refine_dir is not set as it will be the same as before
        refine_params.run = True
        refine_params.class3d_dir = message["class3d_dir"]
        refine_params.class_number = message["best_class"]
        _db.add(refine_params)
        _db.commit()
        _db.close()
    else:
        # Send all other messages on to a container
        try:
            refine_params = _db.exec(
                select(db.RefineParameters).where(db.RefineParameters.pj_id == pj_id)
            ).one()
        except SQLAlchemyError:
            next_job = feedback_params.next_job
            refine_dir = f"{message['refine_dir']}{(feedback_params.next_job + 2):03}"
            refined_grp_uuid = _murfey_id(message["program_id"], _db)[0]
            refined_class_uuid = _murfey_id(message["program_id"], _db)[0]

            refine_params = db.RefineParameters(
                pj_id=pj_id,
                murfey_id=refined_grp_uuid,
                refine_dir=refine_dir,
                class3d_dir=message["class3d_dir"],
                class_number=message["best_class"],
            )
            _db.add(refine_params)
            _db.commit()
            _murfey_refine(refined_class_uuid, refine_dir, message["program_id"], _db)

            next_job += 5
            feedback_params.next_job = next_job

        zocalo_message = {
            "parameters": {
                "refine_job_dir": refine_params.refine_dir,
                "class3d_dir": message["class3d_dir"],
                "class_number": message["best_class"],
                "pixel_size": relion_options["angpix"],
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "node_creator_queue": machine_config.node_creator_queue,
                "nr_iter": default_spa_parameters.nr_iter_3d,
                "picker_id": other_options["picker_ispyb_id"],
                "refined_class_uuid": _refine_murfey_id(
                    refine_params.refine_dir, _app_id(pj_id, _db), _db
                ),
                "refined_grp_uuid": refine_params.murfey_id,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
            },
            "recipes": ["em-spa-refine"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
        feedback_params.hold_refine = True
        _db.add(feedback_params)
        _db.commit()
        _db.close()


def _register_bfactors(message: dict, _db=murfey_db, demo: bool = False):
    """Received refined class to calculate b-factor"""
    machine_config = get_machine_config()
    pj_id_params = _pj_id(message["program_id"], _db, recipe="em-spa-preprocess")
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    relion_params = _db.exec(
        select(db.SPARelionParameters).where(
            db.SPARelionParameters.pj_id == pj_id_params
        )
    ).one()
    relion_options = dict(relion_params)
    feedback_params = _db.exec(
        select(db.SPAFeedbackParameters).where(
            db.SPAFeedbackParameters.pj_id == pj_id_params
        )
    ).one()

    if not feedback_params.hold_refine:
        logger.warning("B-Factors requested but refine hold is off")
        return False

    # Add b-factor for refinement run
    bfactor_run = db.BFactors(
        pj_id=pj_id,
        bfactor_directory=f"{message['project_dir']}/Refine3D/bfactor_{message['number_of_particles']}",
        number_of_particles=message["number_of_particles"],
        resolution=message["resolution"],
    )
    _db.add(bfactor_run)
    _db.commit()

    # All messages should create b-factor jobs as the refine hold is on at this point
    try:
        bfactor_params = _db.exec(
            select(db.BFactorParameters).where(db.BFactorParameters.pj_id == pj_id)
        ).one()
    except SQLAlchemyError:
        bfactor_params = db.BFactorParameters(
            pj_id=pj_id,
            project_dir=message["project_dir"],
            batch_size=message["number_of_particles"],
            refined_grp_uuid=message["refined_grp_uuid"],
            refined_class_uuid=message["refined_class_uuid"],
            class_reference=message["class_reference"],
            class_number=message["class_number"],
            mask_file=message["mask_file"],
        )
        _db.add(bfactor_params)
        _db.commit()

    bfactor_particle_count = default_spa_parameters.bfactor_min_particles
    while bfactor_particle_count < bfactor_params.batch_size:
        bfactor_run_name = (
            f"{bfactor_params.project_dir}/BFactors/bfactor_{bfactor_particle_count}"
        )
        try:
            bfactor_run = _db.exec(
                select(db.BFactors)
                .where(db.BFactors.pj_id == pj_id)
                .where(db.BFactors.bfactor_directory == bfactor_run_name)
            ).one()
            bfactor_run.resolution = 0
        except SQLAlchemyError:
            bfactor_run = db.BFactors(
                pj_id=pj_id,
                bfactor_directory=bfactor_run_name,
                number_of_particles=bfactor_particle_count,
                resolution=0,
            )
        _db.add(bfactor_run)
        _db.commit()

        bfactor_particle_count *= 2

        zocalo_message = {
            "parameters": {
                "bfactor_directory": bfactor_run.bfactor_directory,
                "class_reference": bfactor_params.class_reference,
                "class_number": bfactor_params.class_number,
                "number_of_particles": bfactor_run.number_of_particles,
                "batch_size": bfactor_params.batch_size,
                "pixel_size": message["pixel_size"],
                "mask": bfactor_params.mask_file,
                "particle_diameter": relion_options["particle_diameter"],
                "mask_diameter": relion_options["mask_diameter"] or 0,
                "node_creator_queue": machine_config.node_creator_queue,
                "picker_id": feedback_params.picker_ispyb_id,
                "refined_grp_uuid": bfactor_params.refined_grp_uuid,
                "refined_class_uuid": bfactor_params.refined_class_uuid,
                "session_id": message["session_id"],
                "autoproc_program_id": _app_id(
                    _pj_id(message["program_id"], _db, recipe="em-spa-refine"), _db
                ),
                "feedback_queue": machine_config.feedback_queue,
            },
            "recipes": ["em-spa-bfactor"],
        }
        if _transport_object:
            _transport_object.send(
                "processing_recipe", zocalo_message, new_connection=True
            )
    _db.close()
    return True


def _save_bfactor(message: dict, _db=murfey_db, demo: bool = False):
    """Received b-factor from refinement run"""
    pj_id = _pj_id(message["program_id"], _db, recipe="em-spa-refine")
    bfactor_run = _db.exec(
        select(db.BFactors)
        .where(db.BFactors.pj_id == pj_id)
        .where(db.BFactors.number_of_particles == message["number_of_particles"])
    ).one()
    bfactor_run.resolution = message["resolution"]
    _db.add(bfactor_run)
    _db.commit()

    # Find all the resolutions in the b-factors table
    all_bfactors = _db.exec(select(db.BFactors).where(db.BFactors.pj_id == pj_id)).all()
    particle_counts = [bf.number_of_particles for bf in all_bfactors]
    resolutions = [bf.resolution for bf in all_bfactors]

    if all(resolutions):
        # Calculate b-factor and add to ispyb class table
        bfactor_fitting = np.polyfit(
            np.log(particle_counts), 1 / np.array(resolutions) ** 2, 2
        )
        refined_class_uuid = message["refined_class_uuid"]

        # Request an ispyb insert of the b-factor fitting parameters
        if False and _transport_object:
            _transport_object.send(
                "ispyb_connector",
                {
                    "parameters": {
                        "ispyb_command": "buffer",
                        "buffer_lookup": {
                            "particle_classification_id": refined_class_uuid,
                        },
                        "buffer_command": {
                            "ispyb_command": "insert_particle_classification"
                        },
                        "bfactor_fit_intercept": str(bfactor_fitting[2]),
                        "bfactor_fit_linear": str(bfactor_fitting[1]),
                        "bfactor_fit_quadratic": str(bfactor_fitting[0]),
                    },
                    "content": {"dummy": "dummy"},
                },
                new_connection=True,
            )

        # Clean up the b-factors table and release the hold
        [_db.delete(bf) for bf in all_bfactors]
        _db.commit()
        _release_refine_hold(message)
    _db.close()


def feedback_callback(header: dict, message: dict) -> None:
    try:
        record = None
        if "environment" in message:
            params = message["recipe"][str(message["recipe-pointer"])].get(
                "parameters", {}
            )
            message = message["payload"]
            message.update(params)
        if message["register"] == "motion_corrected":
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.AutoProcProgram.id == message["program_id"])
            ).one()
            session_id = collected_ids[0].session_id

            # Find the autoprocprogram id for the alignment recipe
            alignment_ids = murfey_db.exec(
                select(
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.DataCollection.id == collected_ids[1].id)
                .where(db.ProcessingJob.recipe == "em-tomo-align")
            ).one()

            relevant_tilt_and_series = murfey_db.exec(
                select(db.Tilt, db.TiltSeries)
                .where(db.Tilt.movie_path == message.get("movie"))
                .where(db.Tilt.tilt_series_id == db.TiltSeries.id)
                .where(db.TiltSeries.session_id == session_id)
            ).one()
            relevant_tilt = relevant_tilt_and_series[0]
            relevant_tilt_series = relevant_tilt_and_series[1]
            relevant_tilt.motion_corrected = True
            murfey_db.add(relevant_tilt)
            murfey_db.commit()
            if (
                check_tilt_series_mc(relevant_tilt_series.id)
                and not relevant_tilt_series.processing_requested
            ):
                relevant_tilt_series.processing_requested = True
                murfey_db.add(relevant_tilt_series)

                machine_config = get_machine_config()
                tilts = get_all_tilts(relevant_tilt_series.id)
                ids = get_job_ids(relevant_tilt_series.id, alignment_ids[2].id)
                preproc_params = get_tomo_preproc_params(ids.dcgid)
                stack_file = (
                    Path(message["mrc_out"]).parents[3]
                    / "Tomograms"
                    / "job006"
                    / "tomograms"
                    / f"{relevant_tilt_series.tag}_stack.mrc"
                )
                if not stack_file.parent.exists():
                    stack_file.parent.mkdir(parents=True)
                tilt_offset = _midpoint([float(get_angle(t)) for t in tilts])
                zocalo_message = {
                    "recipes": ["em-tomo-align"],
                    "parameters": {
                        "input_file_list": str([[t, str(get_angle(t))] for t in tilts]),
                        "path_pattern": "",  # blank for now so that it works with the tomo_align service changes
                        "dcid": ids.dcid,
                        "appid": ids.appid,
                        "stack_file": str(stack_file),
                        "pixel_size": preproc_params.pixel_size,
                        "manual_tilt_offset": -tilt_offset,
                        "node_creator_queue": machine_config.node_creator_queue,
                    },
                }
                if _transport_object:
                    logger.info(
                        f"Sending Zocalo message for processing: {zocalo_message}"
                    )
                    _transport_object.send(
                        "processing_recipe", zocalo_message, new_connection=True
                    )
                else:
                    logger.info(
                        f"No transport object found. Zocalo message would be {zocalo_message}"
                    )

            prom.preprocessed_movies.labels(processing_job=collected_ids[2].id).inc()
            murfey_db.commit()
            murfey_db.close()
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "data_collection_group":
            client = murfey_db.exec(
                select(db.ClientEnvironment).where(
                    db.ClientEnvironment.client_id == message["client_id"]
                )
            ).one()
            ispyb_session_id = murfey.server.ispyb.get_session_id(
                microscope=message["microscope"],
                proposal_code=message["proposal_code"],
                proposal_number=message["proposal_number"],
                visit_number=message["visit_number"],
                db=murfey.server.ispyb.Session(),
            )
            if dcg_murfey := murfey_db.exec(
                select(db.DataCollectionGroup)
                .where(db.DataCollectionGroup.session_id == client.session_id)
                .where(db.DataCollectionGroup.tag == message.get("tag"))
            ).all():
                dcgid = dcg_murfey[0].id
            else:
                record = DataCollectionGroup(
                    sessionId=ispyb_session_id,
                    experimentType=message["experiment_type"],
                    experimentTypeId=message["experiment_type_id"],
                )
                dcgid = _register(record, header)
                murfey_dcg = db.DataCollectionGroup(
                    id=dcgid,
                    session_id=client.session_id,
                    tag=message.get("tag"),
                )
                murfey_db.add(murfey_dcg)
                murfey_db.commit()
                murfey_db.close()
            if _transport_object:
                if dcgid is None:
                    time.sleep(2)
                    _transport_object.transport.nack(header, requeue=True)
                    return None
                if global_state.get("data_collection_group_ids") and isinstance(
                    global_state["data_collection_group_ids"], dict
                ):
                    global_state["data_collection_group_ids"] = {
                        **global_state["data_collection_group_ids"],
                        message.get("tag"): dcgid,
                    }
                else:
                    global_state["data_collection_group_ids"] = {
                        message.get("tag"): dcgid
                    }
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "data_collection":
            murfey_session_id = (
                murfey_db.exec(
                    select(db.ClientEnvironment).where(
                        db.ClientEnvironment.client_id == message["client_id"]
                    )
                )
                .one()
                .session_id
            )
            ispyb_session_id = murfey.server.ispyb.get_session_id(
                microscope=message["microscope"],
                proposal_code=message["proposal_code"],
                proposal_number=message["proposal_number"],
                visit_number=message["visit_number"],
                db=murfey.server.ispyb.Session(),
            )
            dcg = murfey_db.exec(
                select(db.DataCollectionGroup)
                .where(db.DataCollectionGroup.session_id == murfey_session_id)
                .where(db.DataCollectionGroup.tag == message["source"])
            ).all()
            if dcg:
                dcgid = dcg[0].id
                # flush_data_collections(message["source"], murfey_db)
            else:
                logger.warning(
                    f"No data collection group ID was found for image directory {message['image_directory']} and source {message['source']}"
                )
                if _transport_object:
                    _transport_object.transport.nack(header, requeue=True)
                return None
            if dc_murfey := murfey_db.exec(
                select(db.DataCollection)
                .where(db.DataCollection.tag == message.get("tag"))
                .where(db.DataCollection.dcg_id == dcgid)
            ).all():
                dcid = dc_murfey[0].id
            else:
                record = DataCollection(
                    SESSIONID=ispyb_session_id,
                    experimenttype=message["experiment_type"],
                    imageDirectory=message["image_directory"],
                    imageSuffix=message["image_suffix"],
                    voltage=message["voltage"],
                    dataCollectionGroupId=dcgid,
                    pixelSizeOnImage=message["pixel_size"],
                    imageSizeX=message["image_size_x"],
                    imageSizeY=message["image_size_y"],
                    slitGapHorizontal=message.get("slit_width"),
                    magnification=message.get("magnification"),
                    exposureTime=message.get("exposure_time"),
                    totalExposedDose=message.get("total_exposed_dose"),
                    c2aperture=message.get("c2aperture"),
                    phasePlate=int(message.get("phase_plate", 0)),
                )
                dcid = _register(
                    record,
                    header,
                    tag=(
                        message.get("tag")
                        if message["experiment_type"] == "tomography"
                        else ""
                    ),
                )
                murfey_dc = db.DataCollection(
                    id=dcid,
                    tag=message.get("tag"),
                    dcg_id=dcgid,
                )
                murfey_db.add(murfey_dc)
                murfey_db.commit()
                murfey_db.close()
            if dcid is None and _transport_object:
                _transport_object.transport.nack(header, requeue=True)
                return None
            if global_state.get("data_collection_ids") and isinstance(
                global_state["data_collection_ids"], dict
            ):
                global_state["data_collection_ids"] = {
                    **global_state["data_collection_ids"],
                    message.get("tag"): dcid,
                }
            else:
                global_state["data_collection_ids"] = {message.get("tag"): dcid}
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "processing_job":
            logger.info("registering processing job")
            assert isinstance(global_state["data_collection_ids"], dict)
            _dcid = global_state["data_collection_ids"].get(message["tag"])
            if _dcid is None:
                logger.warning(f"No data collection ID found for {message['tag']}")
                if _transport_object:
                    _transport_object.transport.nack(header, requeue=True)
                return None
            if pj_murfey := murfey_db.exec(
                select(db.ProcessingJob)
                .where(db.ProcessingJob.recipe == message["recipe"])
                .where(db.ProcessingJob.dc_id == _dcid)
            ).all():
                pid = pj_murfey[0].id
            else:
                record = ProcessingJob(dataCollectionId=_dcid, recipe=message["recipe"])
                run_parameters = message.get("parameters", {})
                assert isinstance(run_parameters, dict)
                if message.get("job_parameters"):
                    job_parameters = [
                        ProcessingJobParameter(parameterKey=k, parameterValue=v)
                        for k, v in message["job_parameters"].items()
                    ]
                    pid = _register(ExtendedRecord(record, job_parameters), header)
                else:
                    pid = _register(record, header)
                murfey_pj = db.ProcessingJob(
                    id=pid, recipe=message["recipe"], dc_id=_dcid
                )
                murfey_db.add(murfey_pj)
                murfey_db.commit()
                murfey_db.close()
            if pid is None and _transport_object:
                _transport_object.transport.nack(header, requeue=True)
                return None
            prom.preprocessed_movies.labels(processing_job=pid)
            if global_state.get("processing_job_ids"):
                global_state["processing_job_ids"] = {
                    **global_state["processing_job_ids"],  # type: ignore
                    message.get("tag"): {
                        **global_state["processing_job_ids"].get(message.get("tag"), {}),  # type: ignore
                        message["recipe"]: pid,
                    },
                }
            else:
                prids = {message["tag"]: {message["recipe"]: pid}}
                global_state["processing_job_ids"] = prids
            if message.get("job_parameters"):
                if _transport_object:
                    _transport_object.transport.ack(header)
                return None
            if app_murfey := murfey_db.exec(
                select(db.AutoProcProgram).where(db.AutoProcProgram.pj_id == pid)
            ).all():
                appid = app_murfey[0].id
            else:
                record = AutoProcProgram(
                    processingJobId=pid, processingStartTime=datetime.now()
                )
                appid = _register(record, header)
                if appid is None and _transport_object:
                    _transport_object.transport.nack(header, requeue=True)
                    return None
                murfey_app = db.AutoProcProgram(id=appid, pj_id=pid)
                murfey_db.add(murfey_app)
                murfey_db.commit()
                murfey_db.close()
            if global_state.get("autoproc_program_ids"):
                assert isinstance(global_state["autoproc_program_ids"], dict)
                global_state["autoproc_program_ids"] = {
                    **global_state["autoproc_program_ids"],
                    message.get("tag"): {
                        **global_state["autoproc_program_ids"].get(message.get("tag"), {}),  # type: ignore
                        message["recipe"]: appid,
                    },
                }
            else:
                global_state["autoproc_program_ids"] = {
                    message["tag"]: {message["recipe"]: appid}
                }
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "flush_tomography_preprocess":
            _flush_tomography_preprocessing(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "flush_spa_preprocess":
            _flush_spa_preprocessing(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "spa_processing_parameters":
            client = murfey_db.exec(
                select(db.ClientEnvironment).where(
                    db.ClientEnvironment.client_id == message["client_id"]
                )
            ).one()
            session_id = client.session_id
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollectionGroup.session_id == session_id)
                .where(db.DataCollectionGroup.tag == message["tag"])
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.ProcessingJob.recipe == "em-spa-preprocess")
            ).one()
            pj_id = collected_ids[2].id
            if not murfey_db.exec(
                select(db.SPARelionParameters).where(
                    db.SPARelionParameters.pj_id == pj_id
                )
            ).all():
                machine_config = get_machine_config()
                params = db.SPARelionParameters(
                    pj_id=collected_ids[2].id,
                    angpix=float(message["pixel_size_on_image"]) * 1e10,
                    dose_per_frame=message["dose_per_frame"],
                    gain_ref=(
                        str(machine_config.rsync_basepath / message["gain_ref"])
                        if message["gain_ref"]
                        else message["gain_ref"]
                    ),
                    voltage=message["voltage"],
                    motion_corr_binning=message["motion_corr_binning"],
                    eer_grouping=message["eer_fractionation"],
                    symmetry=message["symmetry"],
                    particle_diameter=message["particle_diameter"],
                    downscale=message["downscale"],
                    boxsize=message["boxsize"],
                    small_boxsize=message["small_boxsize"],
                    mask_diameter=message["mask_diameter"],
                )
                feedback_params = db.SPAFeedbackParameters(
                    pj_id=collected_ids[2].id,
                    estimate_particle_diameter=not bool(message["particle_diameter"]),
                    hold_class2d=False,
                    hold_class3d=False,
                    class_selection_score=0,
                    star_combination_job=0,
                    initial_model="",
                    next_job=0,
                )
                murfey_db.add(params)
                murfey_db.add(feedback_params)
                murfey_db.commit()
                logger.info(
                    f"SPA processing parameters registered for processing job {collected_ids[2].id}"
                )
                murfey_db.close()
            else:
                logger.info(
                    f"SPA processing parameters already exist for processing job ID {pj_id}"
                )
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "tomography_processing_parameters":
            client = murfey_db.exec(
                select(db.ClientEnvironment).where(
                    db.ClientEnvironment.client_id == message["client_id"]
                )
            ).one()
            session_id = client.session_id
            collected_ids = murfey_db.exec(
                select(
                    db.DataCollectionGroup,
                    db.DataCollection,
                    db.ProcessingJob,
                    db.AutoProcProgram,
                )
                .where(db.DataCollectionGroup.session_id == session_id)
                .where(db.DataCollectionGroup.tag == message["tag"])
                .where(db.DataCollection.dcg_id == db.DataCollectionGroup.id)
                .where(db.DataCollection.tag == message["tilt_series_tag"])
                .where(db.ProcessingJob.dc_id == db.DataCollection.id)
                .where(db.AutoProcProgram.pj_id == db.ProcessingJob.id)
                .where(db.ProcessingJob.recipe == "em-tomo-preprocess")
            ).one()
            if not murfey_db.exec(
                select(func.count(db.TomographyPreprocessingParameters.dcg_id)).where(
                    db.TomographyPreprocessingParameters.dcg_id == collected_ids[0].id
                )
            ).one():
                params = db.TomographyPreprocessingParameters(
                    dcg_id=collected_ids[0].id,
                    pixel_size=float(message["pixel_size_on_image"]) * 10**10,
                    voltage=message["voltage"],
                    dose_per_frame=message["dose_per_frame"],
                    motion_corr_binning=message["motion_corr_binning"],
                    gain_ref=message["gain_ref"],
                    eer_fractionation_file=message["eer_fractionation_file"],
                )
                murfey_db.add(params)
                murfey_db.commit()
                murfey_db.close()
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "picked_particles":
            movie = murfey_db.exec(
                select(db.Movie).where(
                    db.Movie.murfey_id == message["motion_correction_id"]
                )
            ).one()
            movie.preprocessed = True
            murfey_db.add(movie)
            murfey_db.commit()
            feedback_params = murfey_db.exec(
                select(db.SPAFeedbackParameters).where(
                    db.SPAFeedbackParameters.pj_id
                    == _pj_id(message["program_id"], murfey_db)
                )
            ).one()
            if feedback_params.estimate_particle_diameter:
                _register_picked_particles_use_diameter(message)
            else:
                _register_picked_particles_use_boxsize(message)
            prom.preprocessed_movies.labels(
                processing_job=_pj_id(message["program_id"], murfey_db)
            ).inc()
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_incomplete_2d_batch":
            _release_2d_hold(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "incomplete_particles_file":
            _register_incomplete_2d_batch(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "complete_particles_file":
            _register_complete_2d_batch(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "save_class_selection_score":
            _register_class_selection(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_3d_batch":
            _release_3d_hold(message)
            if message.get("do_refinement"):
                _register_refinement(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "run_class3d":
            _register_3d_batch(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "save_initial_model":
            _register_initial_model(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_particle_selection":
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_class_selection":
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "atlas_registered":
            _flush_grid_square_records(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "done_refinement":
            bfactors_registered = _register_bfactors(message)
            if _transport_object:
                if bfactors_registered:
                    _transport_object.transport.ack(header)
                else:
                    _transport_object.transport.nack(header)
            return None
        elif message["register"] == "done_bfactor":
            _save_bfactor(message)
            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        if _transport_object:
            _transport_object.transport.nack(header, requeue=False)
        return None
    except PendingRollbackError:
        murfey_db.rollback()
        murfey_db.close()
        logger.warning("Murfey database required a rollback")
        if _transport_object:
            _transport_object.transport.nack(header, requeue=True)
    except OperationalError:
        logger.warning("Murfey database error encountered", exc_info=True)
        time.sleep(1)
        if _transport_object:
            _transport_object.transport.nack(header, requeue=True)
    except Exception:
        logger.warning(
            "Exception encountered in server RabbitMQ callback", exc_info=True
        )
        if _transport_object:
            _transport_object.transport.nack(header, requeue=False)
    return None


@singledispatch
def _register(record, header: dict, **kwargs):
    raise NotImplementedError(f"Not method to register {record} or type {type(record)}")


@_register.register  # type: ignore
def _(record: Base, header: dict, **kwargs):
    if not _transport_object:
        logger.error(
            f"No transport object found when processing record {record}. Message header: {header}"
        )
        return None
    try:
        if isinstance(record, DataCollection):
            return _transport_object.do_insert_data_collection(record, **kwargs)[
                "return_value"
            ]
        if isinstance(record, DataCollectionGroup):
            return _transport_object.do_insert_data_collection_group(record)[
                "return_value"
            ]
        if isinstance(record, ProcessingJob):
            return _transport_object.do_create_ispyb_job(record)["return_value"]
        if isinstance(record, AutoProcProgram):
            return _transport_object.do_update_processing_status(record)["return_value"]
        # session = Session()
        # session.add(record)
        # session.commit()
        # _transport_object.transport.ack(header, requeue=False)
        return getattr(record, record.__table__.primary_key.columns[0].name)

    except SQLAlchemyError as e:
        logger.error(f"Murfey failed to insert ISPyB record {record}", e, exc_info=True)
        # _transport_object.transport.nack(header)
        return None
    except AttributeError as e:
        logger.error(
            f"Murfey could not find primary key when inserting record {record}",
            e,
            exc_info=True,
        )
        return None


@_register.register  # type: ignore
def _(extended_record: ExtendedRecord, header: dict, **kwargs):
    if not _transport_object:
        raise ValueError(
            "Transport object should not be None if a database record is being updated"
        )
    return _transport_object.do_create_ispyb_job(
        extended_record.record, params=extended_record.record_params
    )["return_value"]


def feedback_listen():
    if _transport_object:
        if not _transport_object.feedback_queue:
            _transport_object.feedback_queue = (
                _transport_object.transport._subscribe_temporary(
                    channel_hint="", callback=None, sub_id=None
                )
            )
        _transport_object._connection_callback = partial(
            _transport_object.transport.subscribe,
            _transport_object.feedback_queue,
            feedback_callback,
            acknowledgement=True,
        )
        _transport_object.transport.subscribe(
            _transport_object.feedback_queue, feedback_callback, acknowledgement=True
        )
