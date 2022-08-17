from __future__ import annotations

import argparse
import logging
import os
import socket
from functools import lru_cache, singledispatch
from threading import Thread
from typing import Any

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
)
from rich.logging import RichHandler
from sqlalchemy.exc import SQLAlchemyError

import murfey
import murfey.server.ispyb
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
_transport_object = None


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
    # setup logging
    zc = zocalo.configuration.from_file()
    zc.activate()

    # Install a log filter to all existing handlers.
    # At this stage this will exclude console loggers, but will cover
    # any Graylog logging set up by the environment activation
    LogFilter.install()

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
    zc.add_command_line_options(parser)
    workflows.transport.add_command_line_options(parser, transport_argument=True)

    args = parser.parse_args()

    # Set up Zocalo connection
    _set_up_transport(args.transport)

    # Set up logging now that the desired verbosity is known
    _set_up_logging(quiet=args.quiet, verbosity=args.verbose)

    rabbit_thread = Thread(target=feedback_listen, daemon=True)
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
    _transport_object = murfey.server.ispyb.TransportManager(transport_type)


def feedback_callback(header: dict, message: dict) -> None:
    record = None
    if message["register"] == "motion_corrected":
        if global_state.get("motion_corrected") and isinstance(
            global_state["motion_corrected"], list
        ):
            global_state["motion_corrected"].append(message["movie"])
        else:
            global_state["motion_corrected"] = [message["movie"]]
        return None
    elif message["register"] == "data_collection_group":
        record = DataCollectionGroup(
            sessionId=message["session_id"],
            experimentType=message["experiment_type"],
        )
        dcgid = _register(record, header)
        if _transport_object:
            if dcgid is None:
                _transport_object.transport.nack(header)
                return None
            global_state["data_collection_group_id"] = dcgid
            _transport_object.transport.ack(header)
        return None
    elif message["register"] == "data_collection":
        record = DataCollection(
            SESSIONID=message["session_id"],
            experimenttype=message["experiment_type"],
            imageDirectory=message["image_directory"],
            imageSuffix=message["image_suffix"],
            voltage=message["voltage"],
            dataCollectionGroupId=global_state.get("data_collection_group_id"),
        )
        dcid = _register(record, header)
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
        pid = _register(record, header)
        if pid is None and _transport_object:
            _transport_object.transport.nack(header)
            return None
        if global_state.get("processing_job_ids"):
            assert isinstance(global_state["processing_job_ids"], dict)
            global_state["processing_job_ids"] = {
                **global_state["processing_job_ids"],
                message.get("tag"): pid,
            }
        else:
            global_state["processing_job_ids"] = {message["tag"]: pid}
        record = AutoProcProgram(processingJobId=pid)
        appid = _register(record, header)
        if appid is None and _transport_object:
            _transport_object.transport.nack(header)
            return None
        if global_state.get("autoproc_program_ids"):
            assert isinstance(global_state["autoproc_program_ids"], dict)
            global_state["autoproc_program_ids"] = {
                **global_state["autoproc_program_ids"],
                message.get("tag"): appid,
            }
        else:
            global_state["autoproc_program_ids"] = {message["tag"]: appid}
        if _transport_object:
            _transport_object.transport.ack(header)
        return None
    if _transport_object:
        _transport_object.transport.nack(header, requeue=False)
    return None


@singledispatch
def _register(record, header: dict):
    raise NotImplementedError(f"Not method to register {record} or type {type(record)}")


@_register.register
def _(record: Base, header: dict):
    if not _transport_object:
        logger.error(
            f"No transport object found when processing record {record}. Message header: {header}"
        )
        return None
    try:
        murfey.server.ispyb.DB.add(record)
        murfey.server.ispyb.DB.commit()
        return getattr(record, record.__table__.primary_key.columns[0].name)
    except SQLAlchemyError as e:
        logger.error(f"Murfey failed to insert ISPyB record {record}", e, exc_info=True)
        return None
    except AttributeError as e:
        logger.error(
            f"Murfey could not find primary key when inserting record {record}",
            e,
            exc_info=True,
        )
        return None


def feedback_listen():
    if _transport_object:
        _transport_object.transport.subscribe(
            "murfey_feedback", feedback_callback, acknowledgement=True
        )
