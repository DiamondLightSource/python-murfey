from __future__ import annotations

import argparse
import configparser
import json
import logging
import platform
import shutil
import sys
import time
import webbrowser
from pathlib import Path
from typing import Literal
from urllib.parse import ParseResult, urlparse

import requests
from rich.logging import RichHandler

import murfey.client.rsync
import murfey.client.update
import murfey.client.watchdir
import murfey.client.websocket
from murfey.client.customlogging import CustomHandler
from murfey.util.models import Visit

log = logging.getLogger("murfey.client")


def _enable_webbrowser_in_cygwin():
    """Helper function to make webbrowser.open() work in CygWin"""
    if "cygwin" in platform.system().lower() and shutil.which("cygstart"):
        webbrowser.register("cygstart", None, webbrowser.GenericBrowser("cygstart"))


def _check_for_updates(
    server: ParseResult, install_version: None | Literal[True] | str
):
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


def _get_visit_list(api_base: ParseResult):
    get_visits_url = api_base._replace(path="/visits_raw")
    server_reply = requests.get(get_visits_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    return [Visit.parse_obj(v) for v in server_reply.json()]


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
    parser.add_argument(
        "--source", help="Directory to transfer files from", type=Path, default="."
    )
    parser.add_argument(
        "--destination",
        help="Directory to transfer files to (syntax: 'data/2022/cm31093-2/tmp/murfey')",
    )
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

    murfey_url = urlparse(args.server, allow_fragments=False)
    if args.server != known_server:
        # New server specified. Verify that it is real
        print(f"Attempting to connect to new server {args.server}")
        try:
            murfey.client.update.check(murfey_url, install=False)
        except Exception as e:
            exit(f"Could not reach Murfey server at {args.server!r} - {e}")

        # If server is reachable then update the configuration
        config["Murfey"]["server"] = args.server
        write_config(config)

    # If user requested installation of a specific or a newer version then
    # make that happen, otherwise ensure client and server are compatible and
    # update if necessary.
    _check_for_updates(server=murfey_url, install_version=args.update)

    from pprint import pprint

    print("Ongoing visits:")
    ongoing_visits = _get_visit_list(murfey_url)
    pprint(ongoing_visits)

    _enable_webbrowser_in_cygwin()

    log.setLevel(logging.DEBUG)
    rich_handler = RichHandler(enable_link_path=False)
    ws = murfey.client.websocket.WSApp(server=args.server)
    logging.getLogger().addHandler(rich_handler)
    handler = CustomHandler(ws.send)
    logging.getLogger().addHandler(handler)
    logging.getLogger("murfey").setLevel(logging.DEBUG)
    logging.getLogger("websocket").setLevel(logging.WARNING)

    log.info("Starting Websocket connection")

    start_dc = input(
        "Press 'D' to start a new Data Collection or press any other key to continue:"
    )
    if start_dc == "D":
        image_directory = str(args.destination)
        image_suffix = input("Enter the image suffix: ")
        visit = str(args.visit)
        dc_params = {
            "type": "start_dc",
            "image_directory": image_directory,
            "image_suffix": image_suffix,
            "visit": visit,
        }
        ws.send(json.dumps(dc_params))

    source_watcher = murfey.client.watchdir.DirWatcher(args.source, settling_time=60)

    if args.destination:
        rsync_process = murfey.client.rsync.RSyncer(
            args.source, basepath_remote=Path(args.destination), server_url=murfey_url
        )

        def rsync_result(update: murfey.client.rsync.RSyncerUpdate):
            if update.outcome is murfey.client.rsync.TransferResult.SUCCESS:
                log.info(
                    f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
                )
            else:
                log.warning(f"Failed to transfer file {str(update.file_path)!r}")
                rsync_process.enqueue(update.file_path)

        rsync_process.subscribe(rsync_result)
        rsync_process.start()
        source_watcher.subscribe(rsync_process.enqueue)
    else:
        log.error("No destination set, no files will be transferred")

    log.info(
        f"Murfey {murfey.__version__} on Python {'.'.join(map(str, sys.version_info[0:3]))} entering main loop"
    )
    try:
        while True:
            source_watcher.scan()
            time.sleep(15)
            # ws.send("ohai")
            log.debug(f"Client is running {ws}")
    except KeyboardInterrupt:
        log.info("Encountered CTRL+C")

    if args.destination:
        rsync_process.stop()
    ws.close()
    log.info("Client stopped")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        with open(Path.home() / ".murfey") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        pass
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


def write_config(config: configparser.ConfigParser):
    with open(Path.home() / ".murfey", "w") as configfile:
        config.write(configfile)
