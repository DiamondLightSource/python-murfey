import argparse
import os

from murfey.util.db import clear, setup


def run():
    parser = argparse.ArgumentParser(
        description="Generate the necessary tables for the Murfey database"
    )

    parser.add_argument(
        "--no-clear",
        dest="clear",
        default=True,
        action="store_false",
        help="Do not clear current database tables before creating specified tables",
    )
    parser.add_argument(
        "-m",
        "--microscope",
        dest="microscope",
        type=str,
        default="",
        help="Microscope as specified in the Murfey machine configuration",
    )

    args = parser.parse_args()
    if args.microscope:
        os.environ["BEAMLINE"] = args.microscope

    from murfey.server.murfey_db import url

    if args.clear:
        clear(url())
    setup(url())
