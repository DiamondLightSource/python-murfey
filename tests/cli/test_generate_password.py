import os

import yaml
from cryptography.fernet import Fernet

from murfey.cli.generate_db_password import run
from murfey.server.config import get_machine_config


def test_generate_password(capsys, tmp_path):
    machine_config = get_machine_config()
    crypto_key = Fernet.generate_key()
    machine_config.crypto_key = crypto_key.decode()
    with open(tmp_path / "config.yaml", "w") as cfg:
        yaml.dump({"m01": machine_config.dict()}, cfg)
    os.environ["MURFEY_MACHINE_CONFIGURATION"] = str(tmp_path / "config.yaml")
    os.environ["BEAMLINE"] = "m01"
    run()
    captured = capsys.readouterr()
    f = Fernet(crypto_key)
    assert len(f.decrypt(captured.out).decode()) == 32
