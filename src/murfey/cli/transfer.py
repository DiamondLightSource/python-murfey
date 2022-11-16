from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

import procrunner
from rich.console import Console
from rich.prompt import Confirm

from murfey.client import read_config


def run():
    config = read_config()
    known_server = config["Murfey"].get("server")

    parser = argparse.ArgumentParser(description="Transfer using a remote rsync daemon")

    parser.add_argument("--source", type=str, default="")
    parser.add_argument("--destination", type=str)
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
    murfey_url = urlparse(args.server, allow_fragments=False)

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
    if Path(args.source or ".").is_file():
        cmd.extend([args.source or ".", f"{murfey_url.hostname}::{args.destination}"])
    else:
        cmd.append("-r")
        cmd.extend(list(Path(args.source or ".").glob("*")))
        cmd.append(f"{murfey_url.hostname}::{args.destination}")

    result = procrunner.run(cmd)
    if result.returncode:
        console.print(f"[red]rsync failed returning code {result.returncode}")
