import json
import os

import pytest
from sqlmodel import Session

from murfey.util.db import Session as MurfeySession
from murfey.util.db import clear, setup
from tests import engine, url

mock_security_config_name = "security_config.yaml"


@pytest.fixture
def start_postgres():
    clear(url)
    setup(url)

    murfey_session = MurfeySession(id=2, name="cm12345-6")
    with Session(engine) as murfey_db:
        murfey_db.add(murfey_session)
        murfey_db.commit()


@pytest.fixture()
def mock_security_configuration(tmp_path):
    config_file = tmp_path / mock_security_config_name
    security_config = {
        "auth_key": "auth_key",
        "auth_algorithm": "auth_algorithm",
        "feedback_queue": "murfey_feedback",
        "rabbitmq_credentials": "/path/to/rabbitmq.yaml",
        "murfey_db_credentials": "/path/to/murfey_db_credentials",
        "crypto_key": "crypto_key",
    }
    with open(config_file, "w") as f:
        json.dump(security_config, f)
    os.environ["MURFEY_SECURITY_CONFIGURATION"] = str(config_file)
