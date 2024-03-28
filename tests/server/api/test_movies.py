from unittest.mock import Mock, create_autospec

from fastapi.testclient import TestClient
from sqlmodel import Session

from murfey.server.main import app
from murfey.server.murfey_db import murfey_db_session


def movies_return():
    return [("Supervisor_1", 2)]


expression = Mock()
expression.all = movies_return

mock_session = create_autospec(Session, instance=True)
mock_session.exec.return_value = expression


def override_murfey_db():
    try:
        db = mock_session
        yield db
    finally:
        db.close()


app.dependency_overrides[murfey_db_session] = override_murfey_db

client = TestClient(app)


def test_movie_count():
    response = client.get("/num_movies")
    assert response.status_code == 200
    assert len(mock_session.method_calls) == 2
    assert response.json() == {"Supervisor_1": 2}
