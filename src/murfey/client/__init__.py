from __future__ import annotations

import argparse
import configparser
import pathlib

import murfey.client.update
from murfey.client.main import websocket_app


def run():
    config = read_config()
    known_server = config["Murfey"].get("server")

    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument(
        "--server", type=str, help="Murfey server to connect to", default=known_server
    )
    args = parser.parse_args()
    parser.add_argument("--visit", help="Name of visit", required=True)
    # visit_name = parser.parse_args().visit

    websocket_app()

    if not args.server:
        exit("Murfey server not set. Please run with --server")

    if args.server != known_server:
        # New server specified. Verify that it is real
        print(f"Attempting to connect to new server {args.server}")
        try:
            murfey.client.update.check(args.server, install=False)
        except Exception as e:
            exit(f"Could not reach Murfey server at {args.server!r} - {e}")

        # If server is reachable then update the configuration
        config["Murfey"]["server"] = args.server
        write_config(config)

    if args.server:
        # Now run an actual update check
        try:
            murfey.client.update.check(args.server)
        except Exception as e:
            print(f"Murfey update check failed with {e}")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        with open(pathlib.Path.home() / ".murfey", "r") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        pass
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


def write_config(config: configparser.ConfigParser):
    with open(pathlib.Path.home() / ".murfey", "w") as configfile:
        config.write(configfile)
