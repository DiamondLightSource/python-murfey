from __future__ import annotations

import argparse
import configparser
import logging
import platform
import shutil
import time
import webbrowser
from pathlib import Path
from typing import Literal
from urllib.parse import ParseResult, urlparse

from rich.logging import RichHandler

import murfey.client.rsync
import murfey.client.update
import murfey.client.watchdir
import murfey.client.websocket
from murfey.client.customlogging import CustomHandler

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

    def rsync_result(update: murfey.client.rsync.RSyncerUpdate):
        if update.outcome is murfey.client.rsync.TransferResult.SUCCESS:
            log.info(
                f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
            )
        else:
            log.warning(f"Failed to transfer file {str(update.file_path)!r}")

    rsync_process = murfey.client.rsync.RSyncer(
        args.source,
        basepath_remote=Path(args.destination or "data/2022/cm31093-2/tmp/murfey"),
        server_url=murfey_url,
    )
    rsync_process.subscribe(rsync_result)
    rsync_process.start()

    source_watcher = murfey.client.watchdir.DirWatcher(args.source, settling_time=5)
    source_watcher.subscribe(rsync_process.enqueue)

    # with open("/dls/tmp/wra62962/directories/z2MvX0sf/filelist", "r") as fh:
    #    filelist = fh.read().split("\n")
    # for f in filelist:
    #     if f:
    #         rsync_process.queue.put(Path(f).absolute())

    # Leave threads running
    try:
        while True:
            source_watcher.scan()
            time.sleep(3)
            ws.send("ohai")
            log.debug(f"Client is running {ws}")
    except KeyboardInterrupt:
        log.info("Encountered CTRL+C")

    rsync_process.stop()
    ws.close()


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    try:
        with open(Path.home() / ".murfey", "r") as configfile:
            config.read_file(configfile)
    except FileNotFoundError:
        pass
    if "Murfey" not in config:
        config["Murfey"] = {}
    return config


def write_config(config: configparser.ConfigParser):
    with open(Path.home() / ".murfey", "w") as configfile:
        config.write(configfile)
