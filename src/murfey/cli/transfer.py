from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from urllib.parse import ParseResult, urlparse

import requests
from rich.console import Console
from rich.prompt import Confirm

from murfey.client import read_config
from murfey.util.api import url_path_for
from murfey.util.config import MachineConfig


def run():
    config = read_config()
    known_server = config["Murfey"].get("server", "")
    instrument_name = config["Murfey"].get("instrument_name", "")

    parser = argparse.ArgumentParser(description="Transfer using a remote rsync daemon")

    parser.add_argument("--source", type=str, default="")
    parser.add_argument("--destination", "-d", type=str)
    parser.add_argument("--destination-prefix", type=str, default="data")
    parser.add_argument("--delete", action="store_true")
    parser.add_argument(
        "--server",
        metavar="HOST:PORT",
        type=str,
        help=f"Murfey server to connect to ({known_server})",
        default=known_server,
    )

    args = parser.parse_args()

    console = Console()
    murfey_url: ParseResult = urlparse(args.server, allow_fragments=False)

    machine_data = MachineConfig(
        requests.get(
            f"{murfey_url.geturl()}{url_path_for('session_control.router', 'machine_info_by_instrument', instrument_name=instrument_name)}"
        ).json()
    )
    if Path(args.source or ".").resolve() in machine_data.data_directories:
        console.print("[red]Source directory is the base directory, exiting")
        return

    cmd = [
        "rsync",
        "-iiv",
        "--times",
        "--progress",
        "-o",  # preserve ownership
        "-p",  # preserve permissions
    ]
    if args.delete:
        cmd.append("--remove-source-files")
        if Path(args.source or ".").is_file():
            num_files = 1
        else:
            num_files = len(
                [f for f in Path(args.source or ".").glob("**/*") if f.is_file()]
            )
        delete_prompt = Confirm.ask(
            f"Do you want to remove {num_files} from {args.source or Path('.').resolve()}?"
        )
        if not delete_prompt:
            return
    console.print(
        f"Copying {args.source} -> {murfey_url.hostname}::{args.destination_prefix}/{args.destination}"
    )
    if Path(args.source or ".").is_file():
        cmd.extend(
            [
                args.source or ".",
                f"{murfey_url.hostname}::{args.destination_prefix}/{args.destination}",
            ]
        )
    else:
        cmd.append("-r")
        cmd.extend(list(Path(args.source or ".").glob("*")))
        cmd.append(f"{murfey_url.hostname}::{args.destination}")

    result = subprocess.run(cmd)
    if result.returncode:
        console.print(f"[red]rsync failed returning code {result.returncode}")
