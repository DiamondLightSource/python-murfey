import json
from configparser import ConfigParser
from pathlib import Path
from typing import Generator

import ispyb
import pytest
from ispyb.sqlalchemy import BLSession, Person, Proposal, url
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlalchemy.orm import scoped_session, sessionmaker

from murfey.util.db import Session as MurfeySession
from murfey.util.db import clear, setup
from tests import murfey_db_engine, murfey_db_url


@pytest.fixture(scope="session")
def session_tmp_path(tmp_path_factory) -> Path:
    """
    Creates a temporary path that persists for the entire test session
    """
    return tmp_path_factory.mktemp("session_tmp")


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def mock_ispyb_credentials(session_tmp_path: Path) -> Path:
    creds_file = session_tmp_path / "ispyb_creds.cfg"
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


@pytest.fixture(scope="session")
def mock_security_configuration(
    session_tmp_path: Path,
    mock_ispyb_credentials: Path,
) -> Path:
    config_file = session_tmp_path / "security_config.yaml"
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


"""
=======================================================================================
Fixtures for setting up mock ISPyB database
=======================================================================================
These were adapted from the tests found at:
https://github.com/DiamondLightSource/ispyb-api/blob/main/tests/conftest.py
"""


class ExampleVisit:
    """
    This is a class to store information that will common to all database entries for
    a particular Murfey session, to enable ease of replication when creating database
    fixtures.
    """

    # Visit-related (ISPyB & Murfey)
    instrument_name = "i01"
    proposal_code = "cm"
    proposal_number = 12345
    visit_number = 6

    # Person (ISPyB)
    given_name = "Eliza"
    family_name = "Murfey"
    login = "murfey123"


@pytest.fixture(scope="session")
def ispyb_db_connection(mock_ispyb_credentials):
    with ispyb.open(mock_ispyb_credentials) as connection:
        yield connection


@pytest.fixture(scope="session")
def ispyb_engine(mock_ispyb_credentials):
    ispyb_engine = create_engine(
        url=url(mock_ispyb_credentials), connect_args={"use_pure": True}
    )
    yield ispyb_engine
    ispyb_engine.dispose()


@pytest.fixture(scope="session")
def ispyb_session_factory(ispyb_engine):
    return scoped_session(sessionmaker(bind=ispyb_engine))


@pytest.fixture
def ispyb_db(ispyb_session_factory) -> Generator[SQLAlchemySession, None, None]:
    # Get a new session from the session factory
    ispyb_db: SQLAlchemySession = ispyb_session_factory()

    # Populate the ISPyB table with some default values
    person_db_entry = Person(
        givenName=ExampleVisit.given_name,
        familyName=ExampleVisit.family_name,
        login=ExampleVisit.login,
    )
    ispyb_db.add(person_db_entry)
    ispyb_db.commit()

    proposal_db_entry = Proposal(
        personId=person_db_entry.personId,
        proposalCode=ExampleVisit.proposal_code,
        proposalNumber=str(ExampleVisit.proposal_number),
    )
    ispyb_db.add(proposal_db_entry)
    ispyb_db.commit()

    bl_session_db_entry = BLSession(
        proposalId=proposal_db_entry.proposalId,
        beamLineName=ExampleVisit.instrument_name,
        visit_number=ExampleVisit.visit_number,
    )
    ispyb_db.add(bl_session_db_entry)
    ispyb_db.commit()

    # Yield the Session and pass processing over to other function
    yield ispyb_db

    # Tidying up
    ispyb_db.rollback()
    ispyb_db.close()


"""
=======================================================================================
Fixtures for setting up mock Murfey database
=======================================================================================
"""


@pytest.fixture
def start_postgres():
    clear(murfey_db_url)
    setup(murfey_db_url)

    murfey_session = MurfeySession(id=2, name="cm12345-6")
    with SQLAlchemySession(murfey_db_engine) as murfey_db:
        murfey_db.add(murfey_session)
        murfey_db.commit()
