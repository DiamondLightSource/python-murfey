import os

import yaml
from cryptography.fernet import Fernet

from murfey.cli.generate_db_password import run
from murfey.util.config import get_global_config


def test_generate_password(capsys, tmp_path):
    global_config = get_global_config()
    crypto_key = Fernet.generate_key()
    global_config.crypto_key = crypto_key.decode("ascii")
    with open(tmp_path / "config.yaml", "w") as cfg:
        yaml.dump(global_config.dict(), cfg)
    os.environ["MURFEY_GLOBAL_CONFIGURATION"] = str(tmp_path / "config.yaml")
    run()
    captured = capsys.readouterr()
    f = Fernet(crypto_key)
    assert len(f.decrypt(captured.out).decode()) == 32
