import argparse
import os

from cryptography.fernet import Fernet

from murfey.server.config import get_machine_config


def run():
    parser = argparse.ArgumentParser(description="Decrypt Murfey database password")

    parser.add_argument(nargs="?", dest="password", help="Password to decrypt")
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

    machine_config = get_machine_config()
    f = Fernet(machine_config.crypto_key.encode("ascii"))
    print(f.decrypt(args.password.encode("ascii")).decode())
