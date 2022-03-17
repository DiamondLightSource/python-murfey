from __future__ import annotations

import argparse
import functools
import logging
import os
import socket
from typing import Any

import uvicorn
import zocalo.configuration
from fastapi.templating import Jinja2Templates
from rich.logging import RichHandler

import murfey

try:
    from importlib.resources import files
except ImportError:
    # Fallback for Python 3.8
    from importlib_resources import files  # type: ignore

logger = logging.getLogger("murfey.server")

template_files = files("murfey") / "templates"
templates = Jinja2Templates(directory=template_files)

_running_server: uvicorn.Server | None = None


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
    zc.activate_environment("live")

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
    args = parser.parse_args()

    # Set up logging now that the desired verbosity is known
    _set_up_logging(quiet=args.quiet, verbosity=args.verbose)

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


@functools.lru_cache()
def get_microscope():
    try:
        hostname = get_hostname()
        microscope_from_hostname = hostname.split(".")[0]
    except OSError:
        microscope_from_hostname = "Unknown"
    microscope_name = os.getenv("BEAMLINE", microscope_from_hostname)
    return microscope_name


@functools.lru_cache()
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
