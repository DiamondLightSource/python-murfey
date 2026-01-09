import argparse

from murfey.util.db import clear, setup
from murfey.util.processing_db import (
    clear as processing_db_clear,
    setup as processing_db_setup,
)


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
        "--include-processing",
        dest="processing",
        default=True,
        action="store_true",
        help="Include processing results tables (MotionCorr, CTF, etc)",
    )

    args = parser.parse_args()

    from murfey.server.murfey_db import url

    if args.clear and args.processing:
        processing_db_clear(url())
    elif args.clear:
        clear(url())

    if args.processing:
        processing_db_setup(url())
    else:
        setup(url())
