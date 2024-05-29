import argparse
from pathlib import Path

from murfey.util.clem import convert_tiff_to_stack


def run():
    parser = argparse.ArgumentParser(
        description="Convert individual TIFF files into image stacks"
    )

    parser.add_argument(
        nargs=1,
        dest="tiff_path",
        help="Path to directory containing TIFF files for conversion",
    )
    parser.add_argument(
        "--root-dir",
        default="images",
        type=str,
        help="Top subdirectory that TIFF files are stored in. Used to determine destination of the created image stacks",
    )
    parser.add_argument(
        "-n", "--num-procs", default=1, type=int, help="Number of processes"
    )

    args = parser.parse_args()

    convert_tiff_to_stack(
        Path(args.tiff_path),
        root_folder=args.root_dir,
        number_of_processes=args.num_procs,
    )
