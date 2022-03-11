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
    parser.add_argument(
        "--update",
        nargs="?",
        default=None,
        const=True,
        help="Update Murfey to the newest or to a specific version",
    )
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
        if args.update:
            # User requested installation of a specific or a newer version
            if args.update is True:
                try:
                    murfey.client.update.check(args.server, force=True)
                    print("\nYou are already running the newest version of Murfey")
                    exit()
                except Exception as e:
                    exit(f"Murfey update check failed with {e}")
            if murfey.client.update.install_murfey(args.server, args.update):
                print(f"\nMurfey has been updated to version {args.update}")
                exit()
            else:
                exit("Error occurred while updating Murfey")

        # Otherwise run a routine update check to ensure client and server are compatible
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
