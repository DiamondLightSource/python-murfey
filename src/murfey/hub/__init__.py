import argparse
import logging

import uvicorn
from rich.logging import RichHandler

import murfey
from murfey.util import LogFilter

logger = logging.getLogger("murfey.hub")


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey Hub server")
    parser.add_argument(
        "--host",
        help="Listen for incoming connections on a specific interface (IP address or hostname; default: all)",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        help="Listen for incoming TCP connections on this port (default: 8002)",
        type=int,
        default=8002,
    )
    args = parser.parse_args()

    LogFilter.install()

    rich_handler = RichHandler(enable_link_path=False)
    logging.getLogger("murfey").setLevel(logging.INFO)
    logging.getLogger("murfey").addHandler(rich_handler)
    logging.getLogger("fastapi").addHandler(rich_handler)
    logging.getLogger("uvicorn").addHandler(rich_handler)

    logger.info(
        f"Starting Murfey Hub server version {murfey.__version__}, listening on {args.host}:{args.port}"
    )
    global _running_server
    config = uvicorn.Config(
        "murfey.hub.main:app",
        host=args.host,
        port=args.port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
    )

    logger.info("Starting hub server")
    _running_server = uvicorn.Server(config=config)
    _running_server.run()
    logger.info("Hub server shutting down")
