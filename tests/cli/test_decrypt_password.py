import os
from unittest.mock import patch

import yaml
from cryptography.fernet import Fernet

from murfey.cli.decrypt_db_password import run
from murfey.util.config import get_security_config


def test_decrypt_password(capsys, tmp_path):
    security_config = get_security_config()
    crypto_key = Fernet.generate_key()
    security_config.crypto_key = crypto_key.decode("ascii")
    with open(tmp_path / "config.yaml", "w") as cfg:
        yaml.dump(security_config.model_dump(), cfg)
    os.environ["MURFEY_SECURITY_CONFIGURATION"] = str(tmp_path / "config.yaml")
    password = "abcd"
    f = Fernet(crypto_key)
    encrypted_password = f.encrypt(password.encode("ascii")).decode()
    with patch("argparse._sys.argv", ["murfey.decrypt_password", encrypted_password]):
        run()
    captured = capsys.readouterr()
    assert captured.out.replace("\n", "").replace("\r\n", "") == password
