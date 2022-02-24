from __future__ import annotations

import argparse
import configparser
import pathlib
import threading

import murfey.client.update
from murfey.client.main import setup_rsync, websocket_app


def run():
    config = read_config()
    known_server = config["Murfey"].get("server")

    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument(
        "--server", type=str, help="Murfey server to connect to", default=known_server
    )

    parser.add_argument("--visit", help="Name of visit", required=True)
    parser.add_argument("--source", help="Directory to transfer files from")
    parser.add_argument("--destination", help="Directory to transfer files to")
    args = parser.parse_args()
    visit_name = args.visit

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

    ws = threading.Thread(target=websocket_app)
    ws.start()
    if args.source and args.destination:
        source_directory = pathlib.Path(args.source)
        destination_directory = pathlib.Path(args.destination)
        setup_rsync(visit_name, source_directory, destination_directory)


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
