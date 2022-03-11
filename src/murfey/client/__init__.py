from __future__ import annotations

import argparse
import configparser
import pathlib
import platform
import shutil
import threading
import webbrowser
from typing import Literal

import murfey.client.update
from murfey.client.transfer import setup_rsync
from murfey.client.websockets import websocket_app


def _enable_webbrowser_in_cygwin():
    """Helper function to make webbrowser.open() work in CygWin"""
    if "cygwin" in platform.system().lower() and shutil.which("cygstart"):
        webbrowser.register("cygstart", None, webbrowser.GenericBrowser("cygstart"))


def _check_for_updates(server: str, install_version: None | Literal[True] | str):
    if install_version is True:
        # User requested installation of the newest version
        try:
            murfey.client.update.check(server, force=True)
            print("\nYou are already running the newest version of Murfey")
            exit()
        except Exception as e:
            exit(f"Murfey update check failed with {e}")

    if install_version:
        # User requested installation of a specific version
        if murfey.client.update.install_murfey(server, install_version):
            print(f"\nMurfey has been updated to version {install_version}")
            exit()
        else:
            exit("Error occurred while updating Murfey")

    # Otherwise run a routine update check to ensure client and server are compatible
    try:
        murfey.client.update.check(server)
    except Exception as e:
        print(f"Murfey update check failed with {e}")


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

    # If user requested installation of a specific or a newer version then
    # make that happen, otherwise ensure client and server are compatible and
    # update if necessary.
    _check_for_updates(server=args.server, install_version=args.update)

    _enable_webbrowser_in_cygwin()

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
