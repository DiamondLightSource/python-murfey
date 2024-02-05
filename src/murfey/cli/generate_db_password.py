import secrets
import string

from cryptography.fernet import Fernet

from murfey.server.config import get_machine_config


def run():
    machine_config = get_machine_config()
    f = Fernet(machine_config.crypto_key.encode("ascii"))
    alphabet = string.ascii_letters + string.digits
    password = "".join(secrets.choice(alphabet) for i in range(32))
    print(f.encrypt(password.encode("ascii")).decode())
