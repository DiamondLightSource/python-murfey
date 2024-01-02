import os
from unittest.mock import patch

import yaml
from cryptography.fernet import Fernet

from murfey.cli.decrypt_db_password import run
from murfey.server.config import get_machine_config


def test_decrypt_password(capsys, tmp_path):
    machine_config = get_machine_config()
    crypto_key = Fernet.generate_key()
    machine_config.crypto_key = crypto_key.decode()
    with open(tmp_path / "config.yaml", "w") as cfg:
        yaml.dump({"m01": machine_config.dict()}, cfg)
    os.environ["MURFEY_MACHINE_CONFIGURATION"] = str(tmp_path / "config.yaml")
    os.environ["BEAMLINE"] = "m01"
    password = "abcd"
    f = Fernet(crypto_key)
    encrypted_password = f.encrypt(password.encode("ascii")).decode()
    with patch("argparse._sys.argv", ["murfey.decrypt_password", encrypted_password]):
        run()
    captured = capsys.readouterr()
    assert captured.out.replace("\n", "").replace("\r\n", "") == password
