import secrets
import string

from cryptography.fernet import Fernet

from murfey.util.config import get_security_config


def run():
    security_config = get_security_config()
    f = Fernet(security_config.crypto_key.encode("ascii"))
    alphabet = string.ascii_letters + string.digits
    password = "".join(secrets.choice(alphabet) for i in range(32))
    print(f.encrypt(password.encode("ascii")).decode())
