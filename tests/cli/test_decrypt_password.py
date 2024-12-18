import os
from unittest.mock import patch

import yaml
from cryptography.fernet import Fernet

from murfey.cli.decrypt_db_password import run
from murfey.util.config import get_global_config


def test_decrypt_password(capsys, tmp_path):
    global_config = get_global_config()
    crypto_key = Fernet.generate_key()
    global_config.crypto_key = crypto_key.decode("ascii")
    with open(tmp_path / "config.yaml", "w") as cfg:
        yaml.dump(global_config.dict(), cfg)
    os.environ["MURFEY_global_configURATION"] = str(tmp_path / "config.yaml")
    password = "abcd"
    f = Fernet(crypto_key)
    encrypted_password = f.encrypt(password.encode("ascii")).decode()
    with patch("argparse._sys.argv", ["murfey.decrypt_password", encrypted_password]):
        run()
    captured = capsys.readouterr()
    assert captured.out.replace("\n", "").replace("\r\n", "") == password
