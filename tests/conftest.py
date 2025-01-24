import pytest

from murfey.util.db import clear, setup
from tests import url


@pytest.fixture
def start_postgres():
    clear(url)
    setup(url)
