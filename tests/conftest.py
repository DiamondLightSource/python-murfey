import json
from configparser import ConfigParser
from pathlib import Path

import pytest
from sqlmodel import Session

from murfey.util.db import Session as MurfeySession
from murfey.util.db import clear, setup
from tests import murfey_db_engine, murfey_db_url


@pytest.fixture
def start_postgres():
    clear(murfey_db_url)
    setup(murfey_db_url)

    murfey_session = MurfeySession(id=2, name="cm12345-6")
    with Session(murfey_db_engine) as murfey_db:
        murfey_db.add(murfey_session)
        murfey_db.commit()


@pytest.fixture()
def mock_client_configuration() -> ConfigParser:
    """
    Returns the client-side configuration file as a pre-loaded ConfigParser object.
    """
    config = ConfigParser()
    config["Murfey"] = {
        "instrument_name": "murfey",
        "server": "http://0.0.0.0:8000",
        "token": "pneumonoultramicroscopicsilicovolcanoconiosis",
    }
    return config


@pytest.fixture()
def mock_ispyb_credentials(tmp_path: Path):
    creds_file = tmp_path / "ispyb_creds.cfg"
    ispyb_config = ConfigParser()
    # Use values from the GitHub workflow ISPyB config file
    ispyb_config["ispyb_sqlalchemy"] = {
        "username": "ispyb_api_sqlalchemy",
        "password": "password_5678",
        "host": "localhost",
        "port": "3306",
        "database": "ispybtest",
    }
    with open(creds_file, "w") as file:
        ispyb_config.write(file)
    return creds_file


@pytest.fixture()
def mock_security_configuration(
    tmp_path: Path,
    mock_ispyb_credentials: Path,
):
    config_file = tmp_path / "security_config.yaml"
    security_config = {
        "murfey_db_credentials": "/path/to/murfey_db_credentials",
        "crypto_key": "crypto_key",
        "auth_key": "auth_key",
        "auth_algorithm": "auth_algorithm",
        "rabbitmq_credentials": "/path/to/rabbitmq.yaml",
        "feedback_queue": "murfey_feedback",
        "ispyb_credentials": str(mock_ispyb_credentials),
    }
    with open(config_file, "w") as f:
        json.dump(security_config, f)
    return config_file
