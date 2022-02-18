from __future__ import annotations

import argparse
import functools
import logging
import os
import pathlib
import socket
from typing import Any

import uvicorn
import zocalo.configuration
from fastapi.templating import Jinja2Templates

import murfey

try:
    from importlib.resources import files
except ImportError:
    # Fallback for Python 3.8
    from importlib_resources import files  # type: ignore

ZOCALO_CONFIG = "/dls_sw/apps/zocalo/live/configuration.yaml"

logger = logging.getLogger("murfey.server")

template_files = files("murfey") / "templates"
templates = Jinja2Templates(directory=template_files)


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
    zc = zocalo.configuration.from_file(ZOCALO_CONFIG)
    zc.activate_environment("live")
    logger.setLevel(logging.DEBUG)
    LogFilter.install()

    parser = argparse.ArgumentParser(description="Start the Murfey server")
    parser.add_argument(
        "--env_file",
        help="Path to environment file",
        default=pathlib.Path(__file__).parent / "example_environment_file",
    )
    args = parser.parse_args()
    logger.info("Starting Murfey")
    print("Starting Murfey server")
    uvicorn.run(
        "murfey.server.main:app",
        host="127.0.0.1",
        port=8000,
        env_file=args.env_file,
        log_level="warning",
    )  # set to warning to reduce log clogging
    logger.info("Server shutting down")


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
