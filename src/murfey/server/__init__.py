from __future__ import annotations

import argparse
import logging
import os
from functools import partial, singledispatch
from threading import Thread
from typing import Any, List, NamedTuple

import numpy as np
import uvicorn
import workflows
import zocalo.configuration
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
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import select

import murfey
import murfey.server.websocket
from murfey.server.config import get_hostname, get_machine_config, get_microscope
from murfey.server.murfey_db import murfey_db

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass
import murfey.util.db as db
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


class ExtendedRecord(NamedTuple):
    record: Base
    record_params: List[Base]


class JobIDs(NamedTuple):
    dcid: int
    pid: int
    appid: int
    client_id: int


def check_tilt_series_mc(tag: str) -> bool:
    results = murfey_db.exec(select(db.Tilt).where(db.Tilt.tilt_series_tag == tag))
    return all(r[0].motion_corrected for r in results) and results[0][1].completed


def get_all_tilts(tag: str) -> List[str]:
    results = murfey_db.exec(select(db.Tilt).where(db.Tilt.tilt_series_tag == tag))
    return [r.movie_path for r in results]


def get_job_ids(tag: str) -> JobIDs:
    results = murfey_db.exec(
        select(db.TiltSeries, db.AutoProcProgram, db.ProcessingJob, db.DataCollection)
        .where(db.TiltSeries.tag == tag)
        .where(db.AutoProcProgram.id == db.TiltSeries.auto_proc_program_id)
        .where(db.ProcessingJob.id == db.AutoProcProgram.pj_id)
        .where(db.ProcessingJob.dc_id == db.DataCollection.id)
    )
    return JobIDs(
        dcid=results[0][-1].id,
        pid=results[0][-2].id,
        appid=results[0][-3].id,
        client_id=results[0][0].client_id,
    )


def get_tomo_proc_params(client_id: int, *args) -> db.TomographyProcessingParameters:
    results = murfey_db.exec(
        select(db.TomographyProcessingParameters).where(
            db.TomographyProcessingParameters.client_id == client_id
        )
    )
    return results[0]


def respond_with_template(filename: str, parameters: dict[str, Any] | None = None):
    template_parameters = {
        "hostname": get_hostname(),
        "microscope": get_microscope(),
        "version": murfey.__version__,
    }
    if parameters:
        template_parameters.update(parameters)
    return templates.TemplateResponse(filename, template_parameters)


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


async def feedback_callback_async(header: dict, message: dict) -> None:
    logger.info(f"feedback_callback_async called with {header}, {message}")
    if message["register"] == "motion_corrected":
        if murfey.server.websocket.manager:
            if global_state.get("motion_corrected_movies") and isinstance(
                global_state["motion_corrected_movies"], dict
            ):
                await global_state.aupdate(
                    "motion_corrected_movies",
                    {
                        message.get("movie"): [
                            message.get("mrc_out"),
                            message.get("movie_id"),
                        ],
                    },
                )
            else:
                await global_state.aupdate(
                    "motion_corrected_movies",
                    {
                        message.get("movie"): [
                            message.get("mrc_out"),
                            message.get("movie_id"),
                        ]
                    },
                )


def _register_picked_particles(message: dict):
    """Received picked particles from the autopick service"""
    # Add this message to the table of seen messages
    params_to_forward = message.get("extraction_parameters")
    if isinstance(params_to_forward, dict):
        ctf_params = db.CtfParameters(
            micrographs_file=params_to_forward["micrographs_file"],
            coord_list_file=params_to_forward["coords_list_file"],
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

        picking_db_len = murfey_db.exec(select(func.count(db.ParticleSizes))).one()
        if picking_db_len > 10000:
            # If there are enough particles to get a diameter
            feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()
            relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
            if not feedback_params.particle_diameter:
                # If the diameter has not been calculated then find it
                picking_db = murfey_db.exec(
                    select(db.ParticleSizes.particle_size)
                ).all()
                particle_diameter = np.quantile(list(picking_db), 0.75)
                feedback_params.particle_diameter = particle_diameter
                murfey_db.add(feedback_params)
                murfey_db.commit()
                murfey_db.close()

                ctf_db = murfey_db.exec(select(db.CtfParameters)).all()
                for saved_message in ctf_db:
                    # Send on all saved messages to extraction
                    zocalo_message = {
                        "parameters": {
                            "micrographs_file": saved_message.micrographs_file,
                            "coord_list_file": saved_message.coords_list_file,
                            "output_file": saved_message.extract_file,
                            "pix_size": relion_params.pixel_size_on_image,
                            "ctf_image": saved_message.ctf_image,
                            "ctf_max_resolution": saved_message.ctf_max_resolution,
                            "ctf_figure_of_merit": saved_message.ctf_figure_of_merit,
                            "defocus_u": saved_message.defocus_u,
                            "defocus_v": saved_message.defocus_v,
                            "defocus_angle": saved_message.defocus_angle,
                            "particle_diameter": particle_diameter,
                            "downscale": relion_params.downscale,
                            "relion_options": dict(relion_params),
                        },
                        "recipes": ["em-spa-extract"],
                    }
                    if _transport_object:
                        _transport_object.send("processing_recipe", zocalo_message)
            else:
                # If the diameter is known then just send the new message
                particle_diameter = feedback_params.particle_diameter
                zocalo_message = {
                    "parameters": {
                        "micrographs_file": params_to_forward["micrographs_file"],
                        "coord_list_file": params_to_forward["coords_list_file"],
                        "output_file": params_to_forward["extract_file"],
                        "pix_size": relion_params.pixel_size_on_image,
                        "ctf_image": params_to_forward["ctf_image"],
                        "ctf_max_resolution": params_to_forward["ctf_max_resolution"],
                        "ctf_figure_of_merit": params_to_forward["ctf_figure_of_merit"],
                        "defocus_u": params_to_forward["defocus_u"],
                        "defocus_v": params_to_forward["defocus_v"],
                        "defocus_angle": params_to_forward["defocus_angle"],
                        "particle_diameter": particle_diameter,
                        "downscale": relion_params.downscale,
                        "relion_options": dict(relion_params),
                    },
                    "recipes": ["em-spa-extract"],
                }
                if _transport_object:
                    _transport_object.send("processing_recipe", zocalo_message)

        else:
            # If not enough particles then save the new sizes
            particle_list = message.get("particle_sizes_list")
            if isinstance(particle_list, list):
                for particle in particle_list:
                    new_particle = db.ParticleSizes(particle_size=particle)
                    murfey_db.add(new_particle)
                    murfey_db.commit()
                    murfey_db.close()


def _register_incomplete_2d_batch(message: dict):
    """Received first batch from particle selection service"""
    relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
    feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()
    class2d_message = message.get("class2d_message")
    if isinstance(class2d_message, dict):
        zocalo_message = {
            "parameters": {
                "particles_file": class2d_message["particles_file"],
                "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                "batch_is_complete": False,
                "batch_size": class2d_message["batch_size"],
                "particle_diameter": feedback_params.particle_diameter,
                "combine_star_job_number": -1,
                "relion_options": dict(relion_params),
            },
            "recipes": ["relion-class2d"],
        }
        if _transport_object:
            _transport_object.send("processing_recipe", zocalo_message)


def _register_complete_2d_batch(message: dict):
    """Received full batch from particle selection service"""
    class2d_message = message.get("class2d_message")
    if isinstance(class2d_message, dict):
        relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
        feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()
        if feedback_params.hold_class2d:
            # If waiting then save the message
            class2d_params = db.CtfParameters(
                particles_file=class2d_message["particles_file"],
                class2d_dir=class2d_message["class2d_dir"],
                batch_size=class2d_message["batch_size"],
            )
            murfey_db.add(class2d_params)
            murfey_db.commit()
            murfey_db.close()
        elif not feedback_params.class_selection_score:
            # For the first batch, start a container and set the database to wait
            feedback_params.star_combination_job = feedback_params.next_job + 2
            zocalo_message = {
                "parameters": {
                    "particles_file": class2d_message["particles_file"],
                    "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                    "batch_is_complete": True,
                    "batch_size": class2d_message["batch_size"],
                    "particle_diameter": feedback_params.particle_diameter,
                    "combine_star_job_number": feedback_params.star_combination_job,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class2d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.hold_class2d = True
            feedback_params.next_job += 3
            murfey_db.add(feedback_params)
            murfey_db.commit()
            murfey_db.close()
        else:
            # Send all other messages on to a container
            zocalo_message = {
                "parameters": {
                    "particles_file": class2d_message["particles_file"],
                    "class2d_dir": f"{class2d_message['class2d_dir']}{feedback_params.next_job:03}",
                    "batch_is_complete": True,
                    "batch_size": class2d_message["batch_size"],
                    "particle_diameter": feedback_params.particle_diameter,
                    "combine_star_job_number": feedback_params.star_combination_job,
                    "autoselect_min_score": feedback_params.class_selection_score,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class2d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.next_job += 2
            murfey_db.add(feedback_params)
            murfey_db.commit()
            murfey_db.close()


def _register_class_selection(message: dict):
    """Received selection score from class selection service"""
    relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
    class2d_db = murfey_db.exec(select(db.Class2DParameters)).all()
    # Add the class selection score to the database
    feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()
    feedback_params.class_selection_score = message.get("class_selection_score")
    if isinstance(feedback_params.class_selection_score, dict):
        feedback_params.hold_class2d = False
        for saved_message in class2d_db:
            # Send all held Class2D messages on with the selection score added
            zocalo_message = {
                "parameters": {
                    "particles_file": saved_message.particles_file,
                    "class2d_dir": f"{saved_message.class2d_dir}{feedback_params.next_job:03}",
                    "batch_is_complete": True,
                    "batch_size": saved_message.batch_size,
                    "particle_diameter": feedback_params.particle_diameter,
                    "combine_star_job_number": feedback_params.star_combination_job,
                    "autoselect_min_score": feedback_params.class_selection_score,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class2d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.next_job += 2
        murfey_db.add(feedback_params)
        murfey_db.commit()
        murfey_db.close()


def _register_3d_batch(message: dict):
    """Received 3d batch from class selection service"""
    class3d_message = message.get("class3d_message")
    if isinstance(class3d_message, dict):
        relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
        feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()

        if feedback_params.hold_class3d:
            # If waiting then save the message
            class3d_params = db.CtfParameters(
                particles_file=class3d_message["particles_file"],
                class2d_dir=class3d_message["class3d_dir"],
                batch_size=class3d_message["batch_size"],
            )
            murfey_db.add(class3d_params)
            murfey_db.commit()
            murfey_db.close()
        elif not feedback_params.initial_model:
            # For the first batch, start a container and set the database to wait
            feedback_params.star_combination_job = feedback_params.next_job + 2
            zocalo_message = {
                "parameters": {
                    "particles_file": class3d_message["particles_file"],
                    "class3d_dir": f"{class3d_message['class3d_dir']}{feedback_params.next_job:03}",
                    "batch_size": class3d_message["batch_size"],
                    "particle_diameter": feedback_params.particle_diameter,
                    "do_initial_model": True,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class3d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.hold_class3d = True
            feedback_params.next_job += 2
            murfey_db.add(feedback_params)
            murfey_db.commit()
            murfey_db.close()
        else:
            # Send all other messages on to a container
            zocalo_message = {
                "parameters": {
                    "particles_file": class3d_message["particles_file"],
                    "class3d_dir": f"{class3d_message['class3d_dir']}{feedback_params.next_job:03}",
                    "batch_size": class3d_message["batch_size"],
                    "particle_diameter": feedback_params.particle_diameter,
                    "initial_model_file": feedback_params.initial_model,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class3d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.next_job += 1
            murfey_db.add(feedback_params)
            murfey_db.commit()
            murfey_db.close()


def _register_initial_model(message: dict):
    """Received initial model from 3d classification service"""
    relion_params = murfey_db.exec(select(db.SPARelionParameters)).one()
    class3d_db = murfey_db.exec(select(db.Class3DParameters)).all()
    # Add the initial model file to the database
    feedback_params = murfey_db.exec(select(db.SPAFeedbackParameters)).one()
    feedback_params.class_selection_score = message.get("initial_model")
    if isinstance(feedback_params.class_selection_score, dict):
        feedback_params.hold_class3d = False
        for saved_message in class3d_db:
            # Send all held Class3D messages with the initial model added
            zocalo_message = {
                "parameters": {
                    "particles_file": saved_message.particles_file,
                    "class3d_dir": f"{saved_message.class3d_dir}{feedback_params.next_job:03}",
                    "batch_size": saved_message.batch_size,
                    "particle_diameter": feedback_params.particle_diameter,
                    "initial_model_file": feedback_params.initial_model,
                    "relion_options": dict(relion_params),
                },
                "recipes": ["relion-class3d"],
            }
            if _transport_object:
                _transport_object.send("processing_recipe", zocalo_message)
            feedback_params.next_job += 1
            murfey_db.add(feedback_params)
            murfey_db.commit()
            murfey_db.close()


def feedback_callback(header: dict, message: dict) -> None:
    try:
        record = None
        if "environment" in message:
            message = message["payload"]
        if message["register"] == "motion_corrected":
            relevant_tilt = murfey_db.exec(
                select(db.Tilt).where(db.Tilt.movie_path == message.get("movie"))
            ).one()
            relevant_tilt.motion_corrected = True
            murfey_db.add(relevant_tilt)
            murfey_db.commit()
            murfey_db.close()
            if check_tilt_series_mc(relevant_tilt.tilt_series_tag):
                tilts = get_all_tilts(relevant_tilt.tilt_series_tag)
                ids = get_job_ids(relevant_tilt.tilt_series_tag)
                params = get_tomo_proc_params(ids.client_id)
                tilt_series_proc_params = {
                    "name": relevant_tilt.tilt_series_tag,
                    "file_tilt_list": str(tilts),
                    "dcid": ids.dcid,
                    "processing_job": ids.pid,
                    "autoproc_program_id": ids.appid,
                    "motion_corrected_path": message.get("mrc_out"),
                    "movie_id": message.get("movie_id"),
                    "pixel_size": params.pixel_size,
                    "manual_tilt_offset": params.manual_tilt_offset,
                }
                logger.info(
                    f"Sending processing request with parameters: {tilt_series_proc_params}"
                )

            global_state.update(
                "motion_corrected_movies",
                {
                    message.get("movie"): [
                        message.get("mrc_out"),
                        message.get("movie_id"),
                    ]
                },
                perform_state_update=False,
            )

            if _transport_object:
                _transport_object.transport.ack(header)
            return None
        elif message["register"] == "data_collection_group":
            record = DataCollectionGroup(
                sessionId=message["session_id"],
                experimentType=message["experiment_type"],
                experimentTypeId=message["experiment_type_id"],
            )
            dcgid = _register(record, header)
            if _transport_object:
                if dcgid is None:
                    _transport_object.transport.nack(header)
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
            murfey_dcg = db.DataCollectionGroup(
                id=dcgid,
                client=message["client_id"],
                tag=message.get("tag"),
            )
            murfey_db.add(murfey_dcg)
            murfey_db.commit()
            murfey_db.close()
            return None
        elif message["register"] == "data_collection":
            dcgid = global_state.get("data_collection_group_ids", {}).get(  # type: ignore
                message["source"]
            )
            if dcgid is None:
                raise ValueError(
                    f"No data collection group ID was found for image directory {message['image_directory']}"
                )
            record = DataCollection(
                SESSIONID=message["session_id"],
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
                tag=message.get("tag")
                if message["experiment_type"] == "tomography"
                else "",
            )
            murfey_dc = db.DataCollection(
                id=dcid,
                client=message["client_id"],
                tag=message.get("tag"),
                dcg_id=dcgid,
            )
            murfey_db.add(murfey_dc)
            murfey_db.commit()
            murfey_db.close()
            if dcid is None and _transport_object:
                _transport_object.transport.nack(header)
                return None
            logger.debug(f"registered: {message.get('tag')}")
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
            assert isinstance(global_state["data_collection_ids"], dict)
            _dcid = global_state["data_collection_ids"][message["tag"]]
            record = ProcessingJob(dataCollectionId=_dcid, recipe=message["recipe"])
            if message.get("job_parameters"):
                job_parameters = [
                    ProcessingJobParameter(parameterKey=k, parameterValue=v)
                    for k, v in message["job_parameters"].items()
                ]
                pid = _register(ExtendedRecord(record, job_parameters), header)
            else:
                pid = _register(record, header)
            murfey_pj = db.ProcessingJob(id=pid, recipe=message["recipe"], dc_id=_dcid)
            murfey_db.add(murfey_pj)
            murfey_db.commit()
            murfey_db.close()
            if pid is None and _transport_object:
                _transport_object.transport.nack(header)
                return None
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
            record = AutoProcProgram(processingJobId=pid)
            appid = _register(record, header)
            if appid is None and _transport_object:
                _transport_object.transport.nack(header)
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
                        **global_state["processing_job_ids"].get(message.get("tag"), {}),  # type: ignore
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
        elif message["register"] == "picked_particles":
            _register_picked_particles(message)
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
        if _transport_object:
            _transport_object.transport.nack(header, requeue=False)
        return None
    except Exception:
        logger.warning(
            "Exception encountered in server RabbitMQ callback", exc_info=True
        )


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
