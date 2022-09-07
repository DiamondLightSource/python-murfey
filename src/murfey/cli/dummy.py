from __future__ import annotations

import argparse
from pathlib import Path

from murfey.util.dummy_setup import generate_detector_data, initialise


def run():
    parser = argparse.ArgumentParser(description="Start the Murfey client")
    parser.add_argument(
        "dummy_dir",
        type=str,
        help="Directory to launch dummy data production",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        dest="timeout",
        type=int,
        help="Number of seconds after which dummy data collection will finish",
        default=60,
    )
    args = parser.parse_args()

    base_path = initialise(Path(args.dummy_dir))
    generate_detector_data(base_path, timeout=args.timeout)
