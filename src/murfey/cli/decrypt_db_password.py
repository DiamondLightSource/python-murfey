import argparse

from cryptography.fernet import Fernet

from murfey.util.config import get_security_config


def run():
    parser = argparse.ArgumentParser(description="Decrypt Murfey database password")

    parser.add_argument(nargs="?", dest="password", help="Password to decrypt")

    args = parser.parse_args()

    security_config = get_security_config()
    f = Fernet(security_config.crypto_key.encode("ascii"))
    print(f.decrypt(args.password.encode("ascii")).decode())
