import argparse

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

    args = parser.parse_args()

    from murfey.server.murfey_db import url

    if args.clear:
        clear(url())
    setup(url())
