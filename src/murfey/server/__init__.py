from __future__ import annotations

import argparse
import logging
import pathlib

import uvicorn
import zocalo.configuration

ZOCALO_CONFIG = "/dls_sw/apps/zocalo/live/configuration.yaml"

logger = logging.getLogger("murfey.server")


def run():
    # setup logging
    logger.setLevel(logging.INFO)
    zc = zocalo.configuration.from_file(ZOCALO_CONFIG)
    zc.activate_environment("live")

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
    logger.info("Server startup complete.")
