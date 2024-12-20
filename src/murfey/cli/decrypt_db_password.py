import argparse

from cryptography.fernet import Fernet

from murfey.util.config import get_global_config


def run():
    parser = argparse.ArgumentParser(description="Decrypt Murfey database password")

    parser.add_argument(nargs="?", dest="password", help="Password to decrypt")

    args = parser.parse_args()

    global_config = get_global_config()
    f = Fernet(global_config.crypto_key.encode("ascii"))
    print(f.decrypt(args.password.encode("ascii")).decode())
