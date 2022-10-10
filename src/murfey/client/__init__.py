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

# from multiprocessing import Process, Queue
from threading import Thread
from typing import Literal
from urllib.parse import ParseResult, urlparse

import procrunner
import requests

import murfey.client.rsync
import murfey.client.update
import murfey.client.watchdir
import murfey.client.websocket
from murfey.client.analyser import Analyser
from murfey.client.customlogging import CustomHandler, DirectableRichHandler
from murfey.client.gain_ref import determine_gain_ref
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer
from murfey.client.tui.app import MurfeyTUI
from murfey.client.tui.status_bar import StatusBar
from murfey.util.models import Visit

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


def _get_visit_list(api_base: ParseResult, demo: bool = False):
    get_visits_url = api_base._replace(path="/visits_raw")
    server_reply = requests.get(get_visits_url.geturl())
    if server_reply.status_code != 200:
        raise ValueError(f"Server unreachable ({server_reply.status_code})")
    return [Visit.parse_obj(v) for v in server_reply.json()]


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
        "--appearance_time",
        type=float,
        default=-1,
        help="Only consider top level directories that have appeared more recently than this many hours ago",
    )
    parser.add_argument(
        "--real_dc",
        action="store_true",
        default=False,
        help="Actually perform data collection related calls to API (will do inserts in ISPyB)",
    )
    parser.add_argument(
        "--transfer_all",
        action="store_true",
        help="Transfer all files in current directory regardless of age",
    )
    parser.add_argument(
        "--no_transfer",
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

    if args.no_transfer:
        log.info("No files will be transferred as --no_transfer flag was specified")

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
        ongoing_visits = _get_visit_list(murfey_url, demo=args.demo)
        pprint(ongoing_visits)
        ongoing_visits = [v.name for v in ongoing_visits]

    _enable_webbrowser_in_cygwin()

    log.setLevel(logging.DEBUG)
    log_queue = Queue()
    input_queue = Queue()

    rich_handler = DirectableRichHandler(log_queue, enable_link_path=False)
    rich_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    ws = murfey.client.websocket.WSApp(server=args.server)
    logging.getLogger().addHandler(rich_handler)
    handler = CustomHandler(ws.send)
    logging.getLogger().addHandler(handler)
    logging.getLogger("murfey").setLevel(logging.INFO)
    logging.getLogger("websocket").setLevel(logging.WARNING)

    log.info("Starting Websocket connection")

    # start_dc = Prompt.ask("Would you like to register a new data collection?", choices=["y", "n"])

    # if start_dc == "y":
    #     image_directory = str(args.destination)
    #     image_suffix = Prompt.ask("Enter the image suffix", choices=[".tiff", ".tif", ".mrc", ".eer"])
    #     visit = str(args.visit)
    #     dc_params = {
    #         "type": "start_dc",
    #         "image_directory": image_directory,
    #         "image_suffix": image_suffix,
    #         "visit": visit,
    #     }
    #     ws.send(json.dumps(dc_params))

    status_bar = StatusBar()
    source_watcher = murfey.client.watchdir.DirWatcher(
        args.source, settling_time=1, status_bar=status_bar
    )

    machine_data = requests.get(f"{murfey_url.geturl()}/machine/").json()
    gain_ref: Path | None = None
    if machine_data.get("gain_reference_directory"):
        try:
            gain_ref = determine_gain_ref(machine_data["gain_reference_directory"])
        except RuntimeError:
            pass

    main_loop_thread = Thread(
        target=main_loop,
        args=[source_watcher, args.appearance_time, args.transfer_all],
        kwargs={"gain_ref": gain_ref},
        daemon=True,
    )
    main_loop_thread.start()

    instance_environment = MurfeyInstanceEnvironment(
        url=murfey_url,
        source=Path(args.source),
        watcher=source_watcher,
        default_destination=args.destination
        or f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}",
        demo=args.demo,
        processing_only_mode=server_routing_prefix_found,
    )

    ws.environment = instance_environment

    rsync_process = RSyncer(
        instance_environment.source,
        basepath_remote=Path(args.destination or f"data/{datetime.now().year}"),
        server_url=murfey_url,
        local=args.local or instance_environment.demo,
        do_transfer=not args.no_transfer,
    )
    source_watcher.subscribe(rsync_process.enqueue)

    analyser = Analyser(
        instance_environment.source,
        environment=instance_environment if args.real_dc else None,
    )
    # source_watcher.subscribe(analyser.enqueue)
    rsync_process.subscribe(analyser.enqueue)

    rich_handler.redirect = True
    MurfeyTUI.run(
        log_verbosity=2,
        environment=instance_environment,
        visits=ongoing_visits,
        queues={"input": input_queue, "logs": log_queue},
        status_bar=status_bar,
        dummy_dc=not args.real_dc,
        do_transfer=not args.no_transfer,
        rsync_process=rsync_process,
        analyser=analyser,
    )
    rich_handler.redirect = False

    try:
        main_loop_thread.join()
    except KeyboardInterrupt:
        log.info("Encountered CTRL+C")
        # if args.destination:
        #     rsync_process.stop()
        ws.close()
        log.info("Client stopped")


def main_loop(
    source_watcher: murfey.client.watchdir.DirWatcher,
    appearance_time: float,
    transfer_all: bool,
    gain_ref: Path | None = None,
):
    log.info(
        f"Murfey {murfey.__version__} on Python {'.'.join(map(str, sys.version_info[0:3]))} entering main loop"
    )
    if gain_ref:
        gain_rsync = procrunner.run(["rsync", str(gain_ref)])
        if gain_rsync.returncode:
            log.warning(
                f"Gain reference file {gain_ref} was not successfully transferred"
            )
    if appearance_time > 0:
        modification_time: float | None = time.time() - appearance_time * 3600
    else:
        modification_time = None
    while True:
        source_watcher.scan(
            modification_time=modification_time, transfer_all=transfer_all
        )
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
