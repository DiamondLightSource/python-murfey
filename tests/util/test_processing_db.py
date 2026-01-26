import os

import pytest
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, select

from murfey.util.processing_db import (
    CTF,
    BFactorFit,
    CryoemInitialModel,
    DataCollectionGroup,
    MotionCorrection,
    ParticleClassification,
    ParticleClassificationGroup,
    ParticlePicker,
    ProcessedTomogram,
    RelativeIceThickness,
    TiltImageAlignment,
    Tomogram,
)


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


def test_processing_tables_exist(murfey_db_url):
    from sqlmodel import SQLModel

    engine = create_engine(murfey_db_url)
    SQLModel.metadata.create_all(engine)
    connection = engine.connect()

    with sessionmaker(
        bind=connection, expire_on_commit=False, class_=Session
    ) as murfey_db_session:
        assert murfey_db_session.exec(select(DataCollectionGroup)).all() == []
        assert murfey_db_session.exec(select(MotionCorrection)).all() == []
        assert murfey_db_session.exec(select(CTF)).all() == []
        assert murfey_db_session.exec(select(ParticlePicker)).all() == []
        assert murfey_db_session.exec(select(Tomogram)).all() == []
        assert murfey_db_session.exec(select(ProcessedTomogram)).all() == []
        assert murfey_db_session.exec(select(RelativeIceThickness)).all() == []
        assert murfey_db_session.exec(select(TiltImageAlignment)).all() == []
        assert murfey_db_session.exec(select(ParticleClassificationGroup)).all() == []
        assert murfey_db_session.exec(select(ParticleClassification)).all() == []
        assert murfey_db_session.exec(select(BFactorFit)).all() == []
        assert murfey_db_session.exec(select(CryoemInitialModel)).all() == []

        from murfey.util.db import ClientEnvironment

        assert murfey_db_session.exec(select(ClientEnvironment)).all() == []

    connection.close()
    metadata = MetaData()
    metadata.create_all(engine)
    metadata.reflect(engine)
    metadata.drop_all(engine)
