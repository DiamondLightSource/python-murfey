from __future__ import annotations

import json
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Generator, Type, TypeVar

import ispyb
import pytest
from ispyb.sqlalchemy import BLSession, ExperimentType, Person, Proposal, url
from sqlalchemy import Engine, RootTransaction, and_, create_engine, event, select
from sqlalchemy.exc import InterfaceError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session as SQLAlchemySession
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session as SQLModelSession
from sqlmodel import SQLModel

from murfey.util.db import Session as MurfeySession


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
Database-related helper functions and classes
=======================================================================================
"""


class ExampleVisit:
    """
    This class stores information that will be common to all database entries for a
    particular Murfey session, to enable ease of replication when creating database
    fixtures.
    """

    # Visit-related (ISPyB & Murfey)
    instrument_name = "i01"
    proposal_code = "cm"
    proposal_number = 12345
    visit_number = 6

    # Murfey-specific
    murfey_session_id = 1

    # Person (ISPyB)
    given_name = "Eliza"
    family_name = "Murfey"
    login = "murfey123"


class ISPyBTableValues:
    """
    Visit-independent default values for ISPyB tables
    """

    # ExperimentType (ISPyB)
    experiment_types = {
        "Tomography": 36,
        "Single Particle": 37,
        "Atlas": 44,
    }


SQLAlchemyTable = TypeVar("SQLAlchemyTable", bound=DeclarativeMeta)


def get_or_create_db_entry(
    session: SQLAlchemySession | SQLModelSession,
    table: Type[SQLAlchemyTable],
    lookup_kwargs: dict[str, Any] = {},
    insert_kwargs: dict[str, Any] = {},
) -> SQLAlchemyTable:
    """
    Helper function to facilitate looking up or creating SQLAlchemy table entries.
    Returns the entry if a match based on the lookup criteria is found, otherwise
    creates and returns a new entry.
    """

    # if lookup kwargs are provided, check if entry exists
    if lookup_kwargs:
        conditions = [
            getattr(table, key) == value for key, value in lookup_kwargs.items()
        ]
        # Use 'exec()' for SQLModel sessions
        if isinstance(session, SQLModelSession):
            entry = session.exec(select(table).where(and_(*conditions))).first()
        # Use 'execute()' for SQLAlchemy sessions
        elif isinstance(session, SQLAlchemySession):
            entry = (
                session.execute(select(table).where(and_(*conditions)))
                .scalars()
                .first()
            )
        else:
            raise TypeError("Unexpected Session type")
        if entry:
            return entry

    # If not present, create and return new entry
    # Use new kwargs if provided; otherwise, use lookup kwargs
    insert_kwargs = insert_kwargs or lookup_kwargs
    entry = table(**insert_kwargs)
    session.add(entry)
    session.commit()
    return entry


def restart_savepoint(
    session: SQLAlchemySession | SQLModelSession, transaction: RootTransaction
):
    """
    Re-establish a SAVEPOINT after a nested transaction is committed or rolled back.
    This helps to maintain isolation across different test cases.
    """
    if transaction.nested and not transaction._parent.nested:
        session.begin_nested()


"""
=======================================================================================
Fixtures for setting up test ISPyB database
=======================================================================================
These were adapted from the tests found at:
https://github.com/DiamondLightSource/ispyb-api/blob/main/tests/conftest.py
"""


@pytest.fixture(scope="session")
def ispyb_db_connection(mock_ispyb_credentials):
    with ispyb.open(mock_ispyb_credentials) as connection:
        yield connection


@pytest.fixture(scope="session")
def ispyb_engine(mock_ispyb_credentials):
    engine = create_engine(
        url=url(mock_ispyb_credentials), connect_args={"use_pure": True}
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def ispyb_db_session_factory(ispyb_engine):
    return sessionmaker(bind=ispyb_engine, expire_on_commit=False)


@pytest.fixture(scope="session")
def seed_ispyb_db(ispyb_db_session_factory):
    try:
        # Populate the ISPyB table with some initial values
        # Return existing table entry if already present
        ispyb_db_session: SQLAlchemySession = ispyb_db_session_factory()
        person_db_entry = get_or_create_db_entry(
            session=ispyb_db_session,
            table=Person,
            lookup_kwargs={
                "givenName": ExampleVisit.given_name,
                "familyName": ExampleVisit.family_name,
                "login": ExampleVisit.login,
            },
        )
        proposal_db_entry = get_or_create_db_entry(
            session=ispyb_db_session,
            table=Proposal,
            lookup_kwargs={
                "personId": person_db_entry.personId,
                "proposalCode": ExampleVisit.proposal_code,
                "proposalNumber": str(ExampleVisit.proposal_number),
            },
        )
        _ = get_or_create_db_entry(
            session=ispyb_db_session,
            table=BLSession,
            lookup_kwargs={
                "proposalId": proposal_db_entry.proposalId,
                "beamLineName": ExampleVisit.instrument_name,
                "visit_number": ExampleVisit.visit_number,
            },
        )
        _ = [
            get_or_create_db_entry(
                session=ispyb_db_session,
                table=ExperimentType,
                lookup_kwargs={
                    "experimentTypeId": id,
                    "name": name,
                    "proposalType": "em",
                    "active": 1,
                },
            )
            for name, id in ISPyBTableValues.experiment_types.items()
        ]
        ispyb_db_session.close()
    except InterfaceError:
        # If this fails in the GitHub test environment, raise it as a genuine error
        if os.getenv("GITHUB_ACTIONS") == "true":
            raise InterfaceError
        pytest.skip("ISPyB database has not been set up; skipping test")


@pytest.fixture
def ispyb_db_session(
    ispyb_db_session_factory,
    ispyb_engine: Engine,
    seed_ispyb_db,
) -> Generator[SQLAlchemySession, None, None]:
    """
    Returns a test-safe session that wraps each test in a rollback-safe save point.
    """
    connection = ispyb_engine.connect()
    transaction = connection.begin()  # Outer transaction

    session: SQLAlchemySession = ispyb_db_session_factory(bind=connection)
    session.begin_nested()  # Save point for test

    # Trigger the restart_savepoint function after the end of the transaction
    event.listen(session, "after_transaction_end", restart_savepoint)

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


"""
=======================================================================================
Fixtures for setting up test Murfey database
=======================================================================================
"""


@pytest.fixture(scope="session")
def murfey_db_url():
    try:
        return (
            f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
            f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
        )
    # Skip Murfey database-related tests if the environment for it hasn't been set up
    except KeyError:
        # If this fails in the GitHub test environment, raise it as a genuine error
        if os.getenv("GITHUB_ACTIONS") == "true":
            raise KeyError
        pytest.skip("Murfey PostgreSQL database has not been set up; skipping test")
        return ""


@pytest.fixture(scope="session")
def murfey_db_engine(murfey_db_url):
    engine = create_engine(murfey_db_url)
    SQLModel.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def murfey_db_session_factory(murfey_db_engine):
    return sessionmaker(
        bind=murfey_db_engine, expire_on_commit=False, class_=SQLModelSession
    )


@pytest.fixture(scope="session")
def seed_murfey_db(murfey_db_session_factory):
    # Populate Murfey database with initial values
    session: SQLModelSession = murfey_db_session_factory()
    _ = get_or_create_db_entry(
        session=session,
        table=MurfeySession,
        lookup_kwargs={
            "id": ExampleVisit.murfey_session_id,
            "name": f"{ExampleVisit.proposal_code}{ExampleVisit.proposal_number}-{ExampleVisit.visit_number}",
        },
    )
    session.close()


@pytest.fixture
def murfey_db_session(
    murfey_db_session_factory,
    murfey_db_engine: Engine,
    seed_murfey_db,
) -> Generator[SQLModelSession, None, None]:
    """
    Returns a test-safe session that wraps each test in a rollback-safe save point
    """
    connection = murfey_db_engine.connect()
    transaction = connection.begin()

    session: SQLModelSession = murfey_db_session_factory(bind=connection)
    session.begin_nested()  # Save point for test

    # Trigger the restart_savepoint function after the end of the transaction
    event.listen(session, "after_transaction_end", restart_savepoint)

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
