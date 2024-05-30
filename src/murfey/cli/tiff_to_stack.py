import argparse
from pathlib import Path

from murfey.util.clem import convert_tiff_to_stack


def run():
    parser = argparse.ArgumentParser(
        description="Convert individual TIFF files into image stacks"
    )

    parser.add_argument(
        nargs=1,
        dest="tiff_list",
        help="List of TIFF files belonging to a particular image series",
    )
    parser.add_argument(
        "--root-dir",
        default="images",
        type=str,
        help="Top subdirectory that TIFF files are stored in. Used to determine destination of the created image stacks",
    )

    args = parser.parse_args()

    convert_tiff_to_stack(
        Path(args.tiff_list),
        root_folder=args.root_dir,
        number_of_processes=args.num_procs,
    )
