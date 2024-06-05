import argparse
from pathlib import Path

from murfey.util.lif import convert_lif_to_tiff


def run():
    parser = argparse.ArgumentParser(description="Convert LIF to TIFF")

    parser.add_argument(
        nargs=1, dest="lif_path", help="Path to LIF file for conversion"
    )
    parser.add_argument(
        "--root-dir",
        default="images",
        type=str,
        help="Top subdirectory that LIF files are stored in. Used to determine destination of TIFFs",
    )
    parser.add_argument(
        "-n", "--num-procs", default=1, type=int, help="Number of processes"
    )

    args = parser.parse_args()

    convert_lif_to_tiff(
        Path(args.lif_path),
        root_folder=args.root_dir,
        number_of_processes=args.num_procs,
    )
