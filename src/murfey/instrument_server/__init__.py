import argparse
import logging

import uvicorn

import murfey

logger = logging.getLogger("murfey.instrument_server")


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey server")
    parser.add_argument(
        "--host",
        help="Listen for incoming connections on a specific interface (IP address or hostname; default: all)",
        default="0.0.0.0",
    )
    parser.add_argument(
        "--port",
        help="Listen for incoming TCP connections on this port (default: 8001)",
        type=int,
        default=8001,
    )
    args = parser.parse_args()

    logger.info(
        f"Starting Murfey server version {murfey.__version__}, listening on {args.host}:{args.port}"
    )
    global _running_server
    config = uvicorn.Config(
        "murfey.instrument_server.main:app",
        host=args.host,
        port=args.port,
        log_config=None,
        ws_ping_interval=300,
        ws_ping_timeout=300,
    )

    _running_server = uvicorn.Server(config=config)
    _running_server.run()
    logger.info("Server shutting down")
