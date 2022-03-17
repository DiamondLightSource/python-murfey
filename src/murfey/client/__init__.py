from __future__ import annotations

import argparse
import configparser
import logging
import pathlib
import platform
import shutil
import threading
import time
import webbrowser
from typing import Literal

from rich.logging import RichHandler

import murfey.client.update
import murfey.client.websocket
from murfey.client.customlogging import CustomHandler
from murfey.client.transfer import just_watch_files, setup_rsync
from murfey.utils.file_monitor import Monitor

log = logging.getLogger("murfey.client")


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
        "--server",
        metavar="HOST:PORT",
        type=str,
        help=f"Murfey server to connect to ({known_server})",
        default=known_server,
    )
    parser.add_argument("--visit", help="Name of visit")
    parser.add_argument("--source", help="Directory to transfer files from")
    parser.add_argument("--destination", help="Directory to transfer files to")
    parser.add_argument(
        "--update",
        metavar="VERSION",
        nargs="?",
        default=None,
        const=True,
        help="Update Murfey to the newest or to a specific version",
    )

    args = parser.parse_args()

    if not args.server:
        exit("Murfey server not set. Please run with --server host:port")
    if not args.server.startswith(("http://", "https://")):
        if "://" in args.server:
            exit("Unknown server protocol. Only http:// and https:// are allowed")
        args.server = f"http://{args.server}"

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

    log.setLevel(logging.DEBUG)
    rich_handler = RichHandler(enable_link_path=False)
    ws = murfey.client.websocket.WSApp(server=args.server)
    logging.getLogger().addHandler(rich_handler)
    handler = CustomHandler(ws.send)
    logging.getLogger().addHandler(handler)
    logging.getLogger("websocket").setLevel(logging.WARNING)

    log.info("Starting Websocket connection")

    if args.visit and args.source and args.destination:
        log.info("Starting Monitor/RSync processes")
        source_directory = pathlib.Path(args.source)
        destination_directory = pathlib.Path(args.destination)
        setup_rsync(args.visit, source_directory, destination_directory)

    # Leave threads running
    try:
        while True:
            time.sleep(3)
            ws.send("ohai")
            log.debug(f"Client is running {ws}")
    except KeyboardInterrupt:
        pass

    log.info("Encountered CTRL+C")

    if args.destination and not args.source:
        destination_directory = pathlib.Path(args.destination)
        monitor = Monitor(destination_directory)
        monitor.process(in_thread=True)
        watch = threading.Thread(target=just_watch_files, args=(args.visit, monitor))
        watch.start()
        time.sleep(300)
        print(f"Stopping watching {destination_directory}")
        monitor.stop()
        watch.join()


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
