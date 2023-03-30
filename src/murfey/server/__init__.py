from __future__ import annotations

import argparse
import logging
import os
import socket
from functools import lru_cache, partial, singledispatch
from pathlib import Path
from threading import Thread
from typing import Any, List, NamedTuple

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
from sqlalchemy.exc import SQLAlchemyError

import murfey
import murfey.server.websocket
from murfey.server.config import MachineConfig, from_file

try:
    from murfey.server.ispyb import TransportManager  # Session
except AttributeError:
    pass
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

    murfey_machine_configuration = os.environ["MURFEY_MACHINE_CONFIGURATION"]
    machine_config: MachineConfig = MachineConfig(
        acquisition_software=[],
        calibrations={},
        data_directories={},
        rsync_basepath=Path("dls/tmp"),
    )
    if murfey_machine_configuration:
        microscope = get_microscope()
        machine_config = from_file(Path(murfey_machine_configuration), microscope)
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


@lru_cache()
def get_microscope():
    try:
        hostname = get_hostname()
        microscope_from_hostname = hostname.split(".")[0]
    except OSError:
        microscope_from_hostname = "Unknown"
    microscope_name = os.getenv("BEAMLINE", microscope_from_hostname)
    return microscope_name


@lru_cache()
def get_hostname():
    return socket.gethostname()


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


def feedback_callback(header: dict, message: dict) -> None:
    record = None
    if "environment" in message:
        message = message["payload"]
    if message["register"] == "motion_corrected":
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
                global_state["data_collection_group_ids"] = {message.get("tag"): dcgid}
            _transport_object.transport.ack(header)
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
        )
        dcid = _register(record, header, tag=message.get("tag"))
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
            pid = _register(ExtendedRecord(record, job_parameters))
        else:
            pid = _register(record, header)
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
        record = AutoProcProgram(processingJobId=pid)
        appid = _register(record, header)
        if appid is None and _transport_object:
            _transport_object.transport.nack(header)
            return None
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
    return _transport_object.do_create_ispyb_job(
        extended_record.record, params=extended_record.record_params
    )["return_value"]


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
