from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

import procrunner
from rich.console import Console
from rich.prompt import Prompt

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
        num_files = len(f for f in Path(args.destination).glob("**/*") if f.is_file())
        delete_prompt = Prompt.ask(
            f"Do you want to remove {num_files} from {args.source or Path('.').resolve()}?"
        )
        if not delete_prompt:
            return
    cmd.extend([args.source or ".", f"{murfey_url.hostname}::{args.destination}"])

    result = procrunner.run(cmd)
    if result.returncode:
        console.print(f"[red]rsync failed returning code {result.returncode}")
