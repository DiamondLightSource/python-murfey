from __future__ import annotations

import argparse
import logging
import os
from threading import Thread
from typing import Literal

import graypy
import uvicorn
from rich.logging import RichHandler
from workflows import Error as WorkflowsError
from workflows.transport.pika_transport import PikaTransport

import murfey
import murfey.server
from murfey.server.feedback import feedback_listen
from murfey.server.ispyb import TransportManager
from murfey.util import LogFilter
from murfey.util.config import get_microscope, get_security_config

logger = logging.getLogger("murfey.server.run")


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


def _set_up_transport(transport_type: Literal["PikaTransport"]):
    # Update the existing TransportManager object in 'murfey.server'
    murfey.server._transport_object = TransportManager(transport_type)


def run():
    """
    Main function that starts up the Murfey server
    """

    # Set up argument parser
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
    parser.add_argument(
        "--root-path",
        default="",
        type=str,
        help="Uvicorn root path for use in conjunction with a proxy",
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
    # Parse and separate known and unknown args
    args, unknown = parser.parse_known_args()

    # Load the security configuration
    security_config = get_security_config()

    # Set up GrayLog handler if provided in the configuration
    if security_config.graylog_host:
        handler = graypy.GELFUDPHandler(
            security_config.graylog_host, security_config.graylog_port, level_names=True
        )
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
    # Install a log filter to all existing handlers.
    LogFilter.install()

    if args.demo:
        # Run in demo mode with no connections set up
        os.environ["MURFEY_DEMO"] = "1"
    else:
        # Load RabbitMQ configuration and set up the connection
        try:
            PikaTransport().load_configuration_file(
                security_config.rabbitmq_credentials
            )
            _set_up_transport("PikaTransport")
            logger.info("Set up message transport manager")
        except WorkflowsError:
            logger.error(
                "Error encountered setting up RabbitMQ connection",
                exc_info=True,
            )

    # Set up logging now that the desired verbosity is known
    _set_up_logging(quiet=args.quiet, verbosity=args.verbose)

    if not args.temporary and murfey.server._transport_object:
        murfey.server._transport_object.feedback_queue = security_config.feedback_queue
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
    config = uvicorn.Config(
        "murfey.server.main:app",
        host=args.host,
        port=args.port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
        workers=args.workers,
        root_path=args.root_path,
    )

    murfey.server._running_server = uvicorn.Server(config=config)
    murfey.server._running_server.run()
    logger.info("Server shutting down")


def shutdown():
    if murfey.server._running_server:
        murfey.server._running_server.should_exit = True
        murfey.server._running_server.force_exit = True
