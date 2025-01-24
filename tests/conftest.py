import pytest

from murfey.util.db import Session, clear, setup
from tests import engine, url


@pytest.fixture
def start_postgres():
    clear(url)
    setup(url)

    murfey_session = Session(id=2, name="cm12345-6")
    with Session(engine) as murfey_db:
        murfey_db.add(murfey_session)
        murfey_db.commit()
