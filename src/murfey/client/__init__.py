from __future__ import annotations

import argparse
import configparser

# import json
import logging
import os
import platform
import shutil
import sys
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import List, Literal
from urllib.parse import ParseResult, urlparse

import requests

# from multiprocessing import Process, Queue
from rich.prompt import Confirm

import murfey.client.rsync
import murfey.client.update
import murfey.client.watchdir
import murfey.client.websocket
from murfey.client.customlogging import CustomHandler, DirectableRichHandler
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.tui.app import MurfeyTUI
from murfey.client.tui.status_bar import StatusBar
from murfey.util import _get_visit_list

# from asyncio import Queue


# from rich.prompt import Prompt


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


def run():
    config = read_config()
    try:
        server_routing = config["ServerRouter"]
    except KeyError:
        server_routing = {}
    server_routing_prefix_found = False
    if server_routing:
        for path_prefix, server in server_routing.items():
            if str(Path.cwd()).startswith(path_prefix):
                known_server = server
                server_routing_prefix_found = True
                break
            else:
                known_server = None
    else:
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
    parser.add_argument(
        "--demo",
        action="store_true",
    )
    parser.add_argument(
        "--appearance-time",
        type=float,
        default=-1,
        help="Only consider top level directories that have appeared more recently than this many hours ago",
    )
    parser.add_argument(
        "--fake-dc",
        action="store_true",
        default=False,
        help="Do not perform data collection related calls to API (avoids database inserts)",
    )
    parser.add_argument(
        "--time-based-transfer",
        action="store_true",
        help="Transfer new files",
    )
    parser.add_argument(
        "--no-transfer",
        action="store_true",
        help="Avoid actually transferring files",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Turn on debugging logs",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        default=False,
        help="Perform rsync transfers locally rather than remotely",
    )
    parser.add_argument(
        "--ignore-mdoc-metadata",
        action="store_true",
        default=False,
        help="Do not attempt to read metadata from all mdoc files",
    )
    parser.add_argument(
        "--remove-files",
        action="store_true",
        default=False,
        help="Remove source files immediately after their transfer",
    )
    parser.add_argument(
        "--relax",
        action="store_true",
        default=False,
        help="Relax the condition that the source directory needs to be recognised from the configuration",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="",
        help="Name of Murfey session to be created",
    )
    parser.add_argument(
        "--skip-existing-processing",
        action="store_true",
        default=False,
        help="Do not trigger processing for any data directories currently on disk (you may have started processing for them in a previous murfey run)",
    )

    args = parser.parse_args()

    if not args.server:
        exit("Murfey server not set. Please run with --server host:port")
    if not args.server.startswith(("http://", "https://")):
        if "://" in args.server:
            exit("Unknown server protocol. Only http:// and https:// are allowed")
        args.server = f"http://{args.server}"

    if args.remove_files:
        remove_prompt = Confirm.ask(
            f"Are you sure you want to remove files from {args.source or Path('.').resolve()}?"
        )
        if not remove_prompt:
            exit("Exiting")

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

    if args.no_transfer:
        log.info("No files will be transferred as --no-transfer flag was specified")

    from pprint import pprint

    ongoing_visits = []
    if args.visit:
        ongoing_visits = [args.visit]
    elif server_routing_prefix_found:
        for part in Path.cwd().parts:
            if "-" in part:
                ongoing_visits = [part]
                break
    if not ongoing_visits:
        print("Ongoing visits:")
        ongoing_visits = _get_visit_list(murfey_url)
        pprint(ongoing_visits)
        ongoing_visits = [v.name for v in ongoing_visits]

    _enable_webbrowser_in_cygwin()

    log.setLevel(logging.DEBUG)
    log_queue = Queue()
    input_queue = Queue()

    # rich_handler = DirectableRichHandler(log_queue, enable_link_path=False)
    rich_handler = DirectableRichHandler(enable_link_path=False)
    rich_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)

    client_id = requests.get(f"{murfey_url.geturl()}/new_client_id/").json()
    ws = murfey.client.websocket.WSApp(
        server=args.server,
        id=client_id["new_id"],
    )

    logging.getLogger().addHandler(rich_handler)
    handler = CustomHandler(ws.send)
    logging.getLogger().addHandler(handler)
    logging.getLogger("murfey").setLevel(logging.INFO)
    logging.getLogger("websocket").setLevel(logging.WARNING)

    log.info("Starting Websocket connection")

    status_bar = StatusBar()

    machine_data = requests.get(f"{murfey_url.geturl()}/machine/").json()
    gain_ref: Path | None = None

    instance_environment = MurfeyInstanceEnvironment(
        url=murfey_url,
        client_id=ws.id,
        software_versions=machine_data.get("software_versions", {}),
        # sources=[Path(args.source)],
        # watchers=source_watchers,
        default_destination=args.destination
        or f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}",
        demo=args.demo,
        processing_only_mode=server_routing_prefix_found,
    )

    ws.environment = instance_environment

    rich_handler.redirect = True
    app = MurfeyTUI(
        environment=instance_environment,
        visits=ongoing_visits,
        queues={"input": input_queue, "logs": log_queue},
        status_bar=status_bar,
        dummy_dc=args.fake_dc,
        do_transfer=not args.no_transfer,
        gain_ref=gain_ref,
        redirected_logger=rich_handler,
        force_mdoc_metadata=not args.ignore_mdoc_metadata,
        strict=not args.relax,
        processing_enabled=machine_data.get("processing_enabled", True),
        skip_existing_processing=args.skip_existing_processing,
    )
    app.run()
    rich_handler.redirect = False


def main_loop(
    source_watchers: List[murfey.client.watchdir.DirWatcher],
    appearance_time: float,
    transfer_all: bool,
):
    log.info(
        f"Murfey {murfey.__version__} on Python {'.'.join(map(str, sys.version_info[0:3]))} entering main loop"
    )
    if appearance_time > 0:
        modification_time: float | None = time.time() - appearance_time * 3600
    else:
        modification_time = None
    while True:
        for sw in source_watchers:
            sw.scan(modification_time=modification_time, transfer_all=transfer_all)
        time.sleep(15)


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        mcch = os.environ.get("MURFEY_CLIENT_CONFIG_HOME")
        murfey_client_config_home = Path(mcch) if mcch else Path.home()
        with open(murfey_client_config_home / ".murfey") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        pass
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


def write_config(config: configparser.ConfigParser):
    mcch = os.environ.get("MURFEY_CLIENT_CONFIG_HOME")
    murfey_client_config_home = Path(mcch) if mcch else Path.home()
    with open(murfey_client_config_home / ".murfey", "w") as configfile:
        config.write(configfile)
