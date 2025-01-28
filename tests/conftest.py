import pytest
from sqlmodel import Session

from murfey.util.db import Session as MurfeySession
from murfey.util.db import clear, setup
from tests import engine, url


@pytest.fixture
def start_postgres():
    clear(url)
    setup(url)

    murfey_session = MurfeySession(id=2, name="cm12345-6")
    with Session(engine) as murfey_db:
        murfey_db.add(murfey_session)
        murfey_db.commit()
